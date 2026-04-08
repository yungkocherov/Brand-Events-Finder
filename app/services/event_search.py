import asyncio
import logging
import re
import time
from functools import partial

from ddgs import DDGS

from app.models import BrandEvent, BrandEventsResponse

logger = logging.getLogger(__name__)

EVENT_TYPES = {
    "market_exit": {
        "label": "Уход / приход на рынок",
        "query": "{brand} уход с рынка закрытие выход {year}",
    },
    "rebrand": {
        "label": "Ребрендинг / смена названия",
        "query": "{brand} ребрендинг смена названия логотип {year}",
    },
    "sanctions": {
        "label": "Санкции / ограничения",
        "query": "{brand} санкции ограничения запрет блокировка {year}",
    },
    "scandal": {
        "label": "Крупный скандал / суд",
        "query": "{brand} скандал суд штраф иск {year}",
    },
    "new_product": {
        "label": "Запуск нового продукта",
        "query": "{brand} запуск новый продукт сервис релиз {year}",
    },
    "management": {
        "label": "Смена собственника / руководства",
        "query": "{brand} смена CEO директор руководство назначение {year}",
    },
    "ad_campaign": {
        "label": "Крупная рекламная кампания",
        "query": "{brand} рекламная кампания спонсорство амбассадор {year}",
    },
    "supply": {
        "label": "Перебои с поставками / дефицит",
        "query": "{brand} дефицит перебои поставки нехватка {year}",
    },
    "price_change": {
        "label": "Изменение цен",
        "query": "{brand} повышение цен подорожание скидки {year}",
    },
    "merger": {
        "label": "Слияние / поглощение",
        "query": "{brand} слияние поглощение покупка сделка {year}",
    },
}

MONTHS_RU = {
    "январ": "01", "феврал": "02", "март": "03", "апрел": "04",
    "мая": "05", "мае": "05", "май": "05", "июн": "06",
    "июл": "07", "август": "08", "сентябр": "09",
    "октябр": "10", "ноябр": "11", "декабр": "12",
}


def _extract_date(text: str, fallback_year: int) -> str:
    """Try to extract a date from text snippet."""
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

    return f"{fallback_year}-01-01"


def _domain(url: str) -> str:
    m = re.search(r"https?://(?:www\.)?([^/]+)", url)
    return m.group(1) if m else ""


def _search_ddg(
    brand: str, event_types: list[str], year_from: int, year_to: int
) -> list[BrandEvent]:
    """Search DuckDuckGo for selected event types."""
    events = []
    seen_urls = set()

    queries = []
    for year in range(year_from, year_to + 1):
        for et in event_types:
            cfg = EVENT_TYPES.get(et)
            if not cfg:
                continue
            queries.append((cfg["query"].format(brand=brand, year=year), et, year))

    with DDGS() as ddgs:
        for query, category, year in queries:
            try:
                results = list(ddgs.text(query, max_results=5, region="ru-ru"))
            except Exception as e:
                logger.error(f"DDG search failed: {e}")
                results = []

            for r in results:
                url = r.get("href", "")
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                title = r.get("title", "").strip()
                body = r.get("body", "").strip()
                date_str = _extract_date(f"{title} {body}", year)

                events.append(BrandEvent(
                    brand=brand,
                    event_name=title,
                    event_date=date_str,
                    description=body,
                    impact_category=category,
                    sentiment="neutral",
                    source_url=url,
                    source_title=_domain(url),
                ))

            time.sleep(0.3)

    return events


async def search_brand_events(
    brand: str,
    event_types: list[str] | None = None,
    year_from: int = 2022,
    year_to: int = 2025,
) -> BrandEventsResponse:
    if not event_types:
        event_types = list(EVENT_TYPES.keys())

    loop = asyncio.get_event_loop()
    logger.info(f"Searching '{brand}' ({year_from}-{year_to}), types: {event_types}")

    events = await loop.run_in_executor(
        None, partial(_search_ddg, brand, event_types, year_from, year_to)
    )

    events.sort(key=lambda e: e.event_date)
    logger.info(f"Brand '{brand}': {len(events)} events found")
    return BrandEventsResponse(brand=brand, events=events)
