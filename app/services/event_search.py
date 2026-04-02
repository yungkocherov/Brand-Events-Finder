import json
import os

import anthropic

from app.models import BrandEvent, BrandEventsResponse

_client: anthropic.AsyncAnthropic | None = None


def _get_client() -> anthropic.AsyncAnthropic:
    global _client
    if _client is None:
        _client = anthropic.AsyncAnthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    return _client


SYSTEM_PROMPT = """\
Ты — аналитик, который ищет значимые события для брендов и компаний.
Твоя задача — найти реальные события, которые могли повлиять на бизнес-метрики компании:
выручку, узнаваемость бренда, репутацию, операционную деятельность и т.д.

Примеры событий:
- Ребрендинг, смена названия
- Крупные рекламные кампании или спонсорства
- Судебные разбирательства, штрафы
- Утечки данных, скандалы
- Перебои с поставками, закрытие производств
- Выход на новые рынки, запуск новых продуктов
- Смена руководства
- Слияния и поглощения
- Санкции, ограничения
- Крупные партнёрства

Для каждого найденного события укажи:
1. Название события (кратко)
2. Дату события (в формате YYYY-MM-DD, если точная дата неизвестна — хотя бы месяц и год)
3. Описание (2-3 предложения)
4. Категорию влияния: revenue, awareness, reputation, operations, legal
5. Ссылку на источник (URL)
6. Название источника

Ищи события за последние 3 года. Верни от 3 до 10 наиболее значимых событий.

ВАЖНО: Ответ СТРОГО в формате JSON-массива, без markdown-разметки, без ```json```, просто чистый JSON:
[
  {
    "event_name": "...",
    "event_date": "YYYY-MM-DD",
    "description": "...",
    "impact_category": "...",
    "source_url": "https://...",
    "source_title": "..."
  }
]
"""


async def search_brand_events(brand: str) -> BrandEventsResponse:
    client = _get_client()

    response = await client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        tools=[
            {
                "type": "web_search_20250305",
                "name": "web_search",
                "max_uses": 10,
            }
        ],
        messages=[
            {
                "role": "user",
                "content": (
                    f"Найди значимые события для бренда/компании «{brand}», "
                    f"которые могли повлиять на бизнес-метрики. "
                    f"Используй поиск в интернете для получения актуальной информации."
                ),
            }
        ],
    )

    # Extract text from response
    text = ""
    for block in response.content:
        if block.type == "text":
            text += block.text

    # Parse JSON from response
    events = _parse_events(text, brand)
    return BrandEventsResponse(brand=brand, events=events)


def _parse_events(text: str, brand: str) -> list[BrandEvent]:
    """Parse events JSON from Claude's response."""
    # Try to find JSON array in text
    text = text.strip()

    # Remove markdown code fences if present
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)

    # Find JSON array boundaries
    start = text.find("[")
    end = text.rfind("]")
    if start == -1 or end == -1:
        return []

    json_str = text[start : end + 1]

    try:
        data = json.loads(json_str)
    except json.JSONDecodeError:
        return []

    events = []
    for item in data:
        try:
            events.append(
                BrandEvent(
                    brand=brand,
                    event_name=item.get("event_name", ""),
                    event_date=item.get("event_date", ""),
                    description=item.get("description", ""),
                    impact_category=item.get("impact_category", "other"),
                    source_url=item.get("source_url", ""),
                    source_title=item.get("source_title", ""),
                )
            )
        except Exception:
            continue

    return events
