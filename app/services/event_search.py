import asyncio
import logging
import re
import time
from functools import partial

from ddgs import DDGS

from app.models import BrandEvent, BrandEventsResponse

logger = logging.getLogger(__name__)

SEARCH_QUERIES = [
    ("{brand} скандал суд штраф {year}", "legal"),
    ("{brand} ребрендинг запуск продукт {year}", "awareness"),
    ("{brand} выручка санкции кризис {year}", "revenue"),
    ("{brand} партнёрство слияние сделка {year}", "operations"),
]

MONTHS_RU = {
    "январ": "01", "феврал": "02", "март": "03", "апрел": "04",
    "мая": "05", "мае": "05", "май": "05", "июн": "06",
    "июл": "07", "август": "08", "сентябр": "09",
    "октябр": "10", "ноябр": "11", "декабр": "12",
}


def _extract_date(text: str, fallback_year: int) -> str:
    """Try to extract a date from text snippet."""
    # DD.MM.YYYY or DD/MM/YYYY
    m = re.search(r"(\d{1,2})[./](\d{1,2})[./](20\d{2})", text)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"

    # YYYY-MM-DD
    m = re.search(r"(20\d{2})-(\d{2})-(\d{2})", text)
    if m:
        return m.group(0)

    # "DD месяца YYYY" pattern
    for month_prefix, month_num in MONTHS_RU.items():
        pattern = rf"(\d{{1,2}})\s+{month_prefix}\S*\s+(20\d{{2}})"
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return f"{m.group(2)}-{month_num}-{m.group(1).zfill(2)}"

    # "месяц YYYY"
    for month_prefix, month_num in MONTHS_RU.items():
        pattern = rf"{month_prefix}\S*\s+(20\d{{2}})"
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return f"{m.group(1)}-{month_num}-01"

    # Just a year
    m = re.search(r"(20\d{2})", text)
    if m:
        return f"{m.group(1)}-01-01"

    return f"{fallback_year}-01-01"


def _search_ddg(brand: str, year_from: int, year_to: int) -> list[BrandEvent]:
    """Search DuckDuckGo and convert results directly to events."""
    events = []
    seen_urls = set()

    with DDGS() as ddgs:
        for year in range(year_from, year_to + 1):
            for query_template, category in SEARCH_QUERIES:
                query = query_template.format(brand=brand, year=year)
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

                time.sleep(1)

    return events


def _domain(url: str) -> str:
    """Extract domain name from URL."""
    m = re.search(r"https?://(?:www\.)?([^/]+)", url)
    return m.group(1) if m else ""


async def search_brand_events(
    brand: str, year_from: int = 2022, year_to: int = 2025
) -> BrandEventsResponse:
    loop = asyncio.get_event_loop()
    logger.info(f"Searching '{brand}' ({year_from}-{year_to})")

    events = await loop.run_in_executor(
        None, partial(_search_ddg, brand, year_from, year_to)
    )

    # Sort by date
    events.sort(key=lambda e: e.event_date)

    logger.info(f"Brand '{brand}': {len(events)} events found")
    return BrandEventsResponse(brand=brand, events=events)
