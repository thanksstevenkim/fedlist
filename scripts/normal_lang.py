#!/usr/bin/env python3
import json
from pathlib import Path
from fetch_stats import STATS_OK_PATH, normalize_language_code

def main():
    path = STATS_OK_PATH
    data = json.loads(path.read_text(encoding="utf-8"))
    changed = 0

    for rec in data:
        langs = rec.get("languages_detected")
        if not isinstance(langs, list):
            continue
        seen = set()
        new_list = []
        for v in langs:
            code = normalize_language_code(v)
            if not code or code in seen:
                continue
            seen.add(code)
            new_list.append(code)
        if new_list != langs:
            rec["languages_detected"] = new_list
            changed += 1

    tmp = path.with_suffix(".ok.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)
    print(f"updated {changed} records")

if __name__ == "__main__":
    main()
