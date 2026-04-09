"""Test DDG and Mistral stability across multiple runs."""
import json
import os
import time

from dotenv import load_dotenv
load_dotenv()

from app.services.event_search import _search_ddg, _analyze_with_mistral, _parse_events

BRAND = "Добрый"
INDUSTRY = "напитки"
EVENT_TYPES = ["market_exit", "rebrand", "new_product", "supply", "ad_campaign", "scandal"]
API_KEY = os.environ.get("MISTRAL_API_KEY", "")
RUNS = 3


def main():
    ddg_runs = []
    mistral_runs = []

    # --- DDG stability ---
    print("=" * 60)
    print("  DDG STABILITY TEST")
    print("=" * 60)

    for i in range(RUNS):
        t0 = time.time()
        results = _search_ddg(BRAND, EVENT_TYPES, INDUSTRY)
        elapsed = time.time() - t0
        urls = sorted(set(r["href"] for r in results))
        ddg_runs.append({"urls": urls, "count": len(results), "time": round(elapsed, 1)})
        print(f"  Run {i+1}: {len(results)} results, {len(urls)} unique URLs, {elapsed:.1f}s")
        if i < RUNS - 1:
            time.sleep(2)

    # Compare DDG runs
    print(f"\n  DDG overlap analysis:")
    for i in range(RUNS):
        for j in range(i + 1, RUNS):
            set_i = set(ddg_runs[i]["urls"])
            set_j = set(ddg_runs[j]["urls"])
            overlap = len(set_i & set_j)
            total = len(set_i | set_j)
            pct = round(overlap / total * 100) if total else 0
            print(f"    Run {i+1} vs Run {j+1}: {overlap}/{total} URLs overlap ({pct}%)")

    # --- Mistral stability (same input) ---
    if not API_KEY:
        print("\n  Skipping Mistral test (no API key)")
        return

    print(f"\n{'=' * 60}")
    print("  MISTRAL STABILITY TEST (same DDG input)")
    print("=" * 60)

    # Use first DDG run as fixed input
    fixed_results = _search_ddg(BRAND, EVENT_TYPES, INDUSTRY)
    print(f"  Fixed input: {len(fixed_results)} search results\n")

    for i in range(RUNS):
        t0 = time.time()
        response = _analyze_with_mistral(API_KEY, BRAND, fixed_results, INDUSTRY)
        events = _parse_events(response, BRAND)
        elapsed = time.time() - t0

        event_names = sorted(e.event_name for e in events)
        mistral_runs.append({"events": event_names, "count": len(events), "time": round(elapsed, 1)})
        print(f"  Run {i+1}: {len(events)} events, {elapsed:.1f}s")
        for e in events:
            print(f"    - {e.event_name} ({e.event_date})")
        print()

    # Compare Mistral runs
    print(f"  Mistral overlap analysis:")
    for i in range(RUNS):
        for j in range(i + 1, RUNS):
            set_i = set(mistral_runs[i]["events"])
            set_j = set(mistral_runs[j]["events"])
            overlap = len(set_i & set_j)
            total = len(set_i | set_j)
            pct = round(overlap / total * 100) if total else 0
            print(f"    Run {i+1} vs Run {j+1}: {overlap}/{total} events overlap ({pct}%)")

    # Save
    report = {"ddg_runs": ddg_runs, "mistral_runs": mistral_runs}
    with open("stability_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\nSaved to stability_report.json")


if __name__ == "__main__":
    main()
