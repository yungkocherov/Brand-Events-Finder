import asyncio
import json
import logging
import re
import time
from functools import partial

import httpx
from ddgs import DDGS
from mistralai.client import Mistral

from app.models import BrandEvent, BrandEventsResponse

logger = logging.getLogger(__name__)

CATEGORY_LABELS = {
    "market_exit": "Уход / приход на рынок",
    "rebrand": "Ребрендинг / смена названия",
    "new_product": "Запуск нового продукта",
    "supply": "Перебои с поставками / дефицит",
    "ad_campaign": "Рекламная кампания",
    "scandal": "Скандал / суд",
    "sanctions": "Санкции / ограничения",
    "price_change": "Изменение цен",
    "management": "Смена руководства",
    "merger": "Слияние / поглощение",
    "other": "Другое",
}

SYSTEM_PROMPT = """\
Ты — бизнес-аналитик. Тебе даны результаты поиска по бренду «{brand}»{industry_note}.
Отфильтруй и оставь ТОЛЬКО те результаты, которые описывают реальные значимые \
события, непосредственно связанные с брендом «{brand}»{industry_note}.

Отсей:
- Статьи, не связанные с брендом «{brand}»
- Статьи про другие компании/продукты с похожим названием, но из ДРУГОЙ отрасли
- Общие новости рынка/отрасли без упоминания бренда
- Дубликаты одного и того же события
- Незначительные события

Для каждого оставшегося события укажи:
- event_name: краткое название события
- event_date: ТОЛЬКО дата в формате YYYY-MM-DD (например "2024-03-15"). БЕЗ скобок, БЕЗ слов "дата публикации". Если есть [дата публикации: X] в источнике — извлеки оттуда X. Если даты нет — оставь "". НЕ ВЫДУМЫВАЙ даты!
- description: 1-2 предложения
- impact_category: СТРОГО одно из: market_exit (уход/приход на рынок), rebrand (ребрендинг), new_product (новый продукт), supply (перебои поставок/дефицит), ad_campaign (реклама), scandal (скандал/суд), sanctions (санкции), price_change (изменение цен), management (смена руководства), merger (слияние), other (другое)
- impact_score: ЦЕЛОЕ число от 1 до 5, насколько событие повлияло на бизнес-метрики бренда (выручку, продажи, узнаваемость). 1=минимальное влияние, 5=критическое (уход с рынка, крупный скандал, ребрендинг)
- sentiment: СТРОГО одно из: positive, negative, neutral
- source_url: URL из результатов поиска
- source_title: домен источника

ВАЖНО: ответ СТРОГО в формате JSON-массива, без markdown, без ```json```:
[
  {{
    "event_name": "...",
    "event_date": "YYYY-MM-DD",
    "description": "...",
    "impact_category": "market_exit|rebrand|new_product|supply|ad_campaign|scandal|sanctions|price_change|management|merger|other",
    "impact_score": 1-5,
    "sentiment": "positive|negative|neutral",
    "source_url": "https://...",
    "source_title": "..."
  }}
]

Если ни один результат не подходит — верни пустой массив []."""


SEARCH_QUERY_TEMPLATES = [
    '"{brand}" {industry} новости события',
    '"{brand}" {industry} скандал суд кризис санкции',
    '"{brand}" {industry} запуск ребрендинг сделка',
    '"{brand}" {industry} уход с рынка приход выход закрытие',
    '"{brand}" {industry} цены подорожание дефицит перебои поставки',
]


def _search_ddg(brand: str, industry: str = "") -> list[dict]:
    """Search DuckDuckGo with broad queries about the brand."""
    all_results = []
    seen_urls = set()
    industry_part = industry if industry else ""

    queries = [t.format(brand=brand, industry=industry_part).strip() for t in SEARCH_QUERY_TEMPLATES]

    with DDGS() as ddgs:
        for query in queries:
            try:
                results = list(ddgs.text(query, max_results=15, region="ru-ru"))
            except Exception as e:
                logger.error(f"DDG search failed: {e}")
                results = []

            for r in results:
                url = r.get("href", "")
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)
                all_results.append({
                    "title": r.get("title", "").strip(),
                    "href": url,
                    "body": r.get("body", "").strip(),
                })

            logger.info(f"DDG: {len(results)} results for '{query}'")
            time.sleep(0.5)

    return all_results


def _analyze_with_mistral(
    api_key: str, brand: str, search_results: list[dict],
    industry: str = "", model: str = "open-mistral-nemo",
) -> str:
    """Use Mistral to filter and structure search results."""
    client = Mistral(api_key=api_key)

    formatted = []
    for i, r in enumerate(search_results, 1):
        date = r.get("fetched_date") or _date_from_url(r["href"])
        date_hint = f" [дата публикации: {date}]" if date else ""
        formatted.append(
            f"{i}. {r['title']}{date_hint}\n"
            f"   URL: {r['href']}\n"
            f"   {r['body']}"
        )
    search_text = "\n\n".join(formatted)

    industry_note = f" (отрасль: {industry})" if industry else ""
    prompt = SYSTEM_PROMPT.format(brand=brand, industry_note=industry_note)

    response = client.chat.complete(
        model=model,
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": search_text},
        ],
        temperature=0,
        max_tokens=8000,
    )

    text = response.choices[0].message.content or ""
    logger.info(f"Mistral response length for '{brand}': {len(text)}")
    return text


def _parse_events(text: str, brand: str) -> list[BrandEvent]:
    text = text.strip()

    if "```" in text:
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)

    start = text.find("[")
    end = text.rfind("]")
    if start == -1 or end == -1:
        return []

    try:
        data = json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        return []

    events = []
    for item in data:
        try:
            # Extract clean YYYY-MM-DD from date field (Mistral sometimes wraps it)
            raw_date = str(item.get("event_date", ""))
            date_match = re.search(r"(\d{4})-(\d{2})-(\d{2})", raw_date)
            clean_date = date_match.group(0) if date_match else ""
            events.append(BrandEvent(
                brand=brand,
                event_name=item.get("event_name", ""),
                event_date=clean_date,
                description=item.get("description", ""),
                impact_category=item.get("impact_category", "other"),
                impact_score=int(item.get("impact_score", 3)) if str(item.get("impact_score", "")).isdigit() else 3,
                sentiment=item.get("sentiment", "neutral"),
                source_url=item.get("source_url", ""),
                source_title=item.get("source_title", ""),
            ))
        except Exception:
            continue

    return events


async def search_brand_events(
    brand: str,
    api_key: str = "",
    industry: str = "",
    model: str = "open-mistral-nemo",
) -> BrandEventsResponse:
    loop = asyncio.get_event_loop()
    logger.info(f"Searching '{brand}', industry='{industry}'")

    # Step 1: DDG search
    search_results = await loop.run_in_executor(
        None, partial(_search_ddg, brand, industry)
    )

    logger.info(f"Brand '{brand}': {len(search_results)} raw results")
    if not search_results:
        return BrandEventsResponse(brand=brand, events=[])

    # Step 2: enrich with article publication dates (parallel page fetch)
    mistral_input = search_results[:30]
    await _enrich_with_dates(mistral_input)
    logger.info(f"Brand '{brand}': enriched dates for {sum(1 for r in mistral_input if r.get('fetched_date'))}/{len(mistral_input)} articles")

    # Step 3: filter with Mistral
    if api_key:
        events = []
        for attempt in range(3):
            try:
                ai_response = await loop.run_in_executor(
                    None, partial(_analyze_with_mistral, api_key, brand, mistral_input, industry, model)
                )
                events = _parse_events(ai_response, brand)
                if events:
                    break
                logger.warning(f"Mistral returned 0 events for '{brand}', attempt {attempt + 1}/3")
            except Exception as e:
                logger.error(f"Mistral attempt {attempt + 1}/3 failed for '{brand}': {e}")
            if attempt < 2:
                await asyncio.sleep(2)
        if not events:
            logger.warning(f"All Mistral attempts failed for '{brand}', using raw results")
            events = _raw_to_events(brand, search_results)
    else:
        events = _raw_to_events(brand, search_results)

    events.sort(key=lambda e: (-e.impact_score, e.event_date))
    logger.info(f"Brand '{brand}': {len(events)} events after filtering")
    return BrandEventsResponse(brand=brand, events=events)


def _raw_to_events(brand: str, results: list[dict]) -> list[BrandEvent]:
    """Fallback: convert raw search results to events without AI."""
    events = []
    for r in results:
        date_str = _extract_date(f"{r['title']} {r['body']}")
        events.append(BrandEvent(
            brand=brand,
            event_name=r["title"],
            event_date=date_str,
            description=r["body"],
            impact_category=r["category"],
            sentiment="neutral",
            source_url=r["href"],
            source_title=_domain(r["href"]),
        ))
    return events


MONTHS_RU = {
    "январ": "01", "феврал": "02", "март": "03", "апрел": "04",
    "мая": "05", "мае": "05", "май": "05", "июн": "06",
    "июл": "07", "август": "08", "сентябр": "09",
    "октябр": "10", "ноябр": "11", "декабр": "12",
}


def _extract_date(text: str) -> str:
    m = re.search(r"(\d{1,2})[./](\d{1,2})[./](20\d{2})", text)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    m = re.search(r"(20\d{2})-(\d{2})-(\d{2})", text)
    if m:
        return m.group(0)
    for month_prefix, month_num in MONTHS_RU.items():
        pattern = rf"(\d{{1,2}})\s+{month_prefix}\S*\s+(20\d{{2}})"
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return f"{m.group(2)}-{month_num}-{m.group(1).zfill(2)}"
    for month_prefix, month_num in MONTHS_RU.items():
        pattern = rf"{month_prefix}\S*\s+(20\d{{2}})"
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return f"{m.group(1)}-{month_num}-01"
    m = re.search(r"(20\d{2})", text)
    if m:
        return f"{m.group(1)}-01-01"
    return ""


def _domain(url: str) -> str:
    m = re.search(r"https?://(?:www\.)?([^/]+)", url)
    return m.group(1) if m else ""


async def _fetch_article_date(client: httpx.AsyncClient, url: str) -> str:
    """Fetch article HTML and extract publication date.

    Priority: URL date > <head> meta/JSON-LD > first <time> tag in article.
    Avoids matching sidebar/comment dates by limiting HTML scan area.
    """
    # 1. URL date is most reliable when present
    url_date = _date_from_url(url)
    if url_date:
        return url_date

    try:
        resp = await client.get(url, timeout=3, follow_redirects=True,
                                headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return ""
        html = resp.text
    except Exception:
        return ""

    # Extract <head> for meta tags only
    head_end = html.lower().find("</head>")
    head = html[:head_end] if head_end > 0 else html[:15000]

    # 2. JSON-LD datePublished (in <head> or early body)
    early = html[:20000]
    m = re.search(r'"datePublished"\s*:\s*"(\d{4}-\d{2}-\d{2})', early)
    if m:
        return m.group(1)

    # 3. meta tags in <head>
    m = re.search(
        r'<meta[^>]+(?:property|name)=["\'](?:article:published_time|pubdate|publishdate|date|dc\.date)["\'][^>]+content=["\']([^"\']+)',
        head, re.IGNORECASE,
    )
    if not m:
        m = re.search(
            r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name)=["\'](?:article:published_time|pubdate|publishdate|date|dc\.date)["\']',
            head, re.IGNORECASE,
        )
    if m:
        d = re.search(r"(\d{4})-(\d{2})-(\d{2})", m.group(1))
        if d:
            return d.group(0)

    # 4. First <time datetime="..."> in early body (article header area)
    m = re.search(r'<time[^>]+datetime=["\'](\d{4}-\d{2}-\d{2})', early)
    if m:
        return m.group(1)

    return ""


async def _enrich_with_dates(results: list[dict]) -> None:
    """Fetch all article pages in parallel and add 'fetched_date' field."""
    async with httpx.AsyncClient() as client:
        tasks = [_fetch_article_date(client, r["href"]) for r in results]
        dates = await asyncio.gather(*tasks, return_exceptions=True)
        for r, d in zip(results, dates):
            r["fetched_date"] = d if isinstance(d, str) else ""


def _date_from_url(url: str) -> str:
    """Extract YYYY-MM-DD from URL patterns."""
    # /YYYY/MM/DD/
    m = re.search(r"/(20\d{2})/(\d{1,2})/(\d{1,2})(?:/|\b)", url)
    if m:
        return f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"
    # /DD/MM/YYYY/ (e.g. РБК)
    m = re.search(r"/(\d{1,2})/(\d{1,2})/(20\d{2})(?:/|\b)", url)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    # YYYY-MM-DD
    m = re.search(r"(20\d{2})-(\d{2})-(\d{2})", url)
    if m:
        return m.group(0)
    # /YYYY/MM/
    m = re.search(r"/(20\d{2})/(\d{1,2})(?:/|\b)", url)
    if m:
        return f"{m.group(1)}-{m.group(2).zfill(2)}-01"
    return ""
