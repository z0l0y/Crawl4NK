import argparse
import datetime
import json
import os
import re
import time
from typing import Dict, List, Optional
from urllib.parse import quote

import requests


DIFFICULTY_LEVEL_MAP = {
    1: "容易",
    2: "中等",
    3: "困难",
}

DIFFICULTY_TEXT_MAP = {
    "简单": "容易",
    "容易": "容易",
    "easy": "容易",
    "中等": "中等",
    "medium": "中等",
    "困难": "困难",
    "hard": "困难",
}

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
URL_RE = re.compile(r"https?://\S+")


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", "", (text or "").strip().lower())


def normalize_difficulty(value) -> str:
    if isinstance(value, int):
        return DIFFICULTY_LEVEL_MAP.get(value, "")

    text = str(value or "").strip().lower()
    if not text:
        return ""

    return DIFFICULTY_TEXT_MAP.get(text, DIFFICULTY_TEXT_MAP.get(str(value).strip(), ""))


def parse_int(value) -> Optional[int]:
    try:
        parsed = int(value)
        if parsed <= 0:
            return None
        return parsed
    except Exception:
        return None


def build_fallback_url(title: str) -> str:
    keyword = str(title or "").strip()
    if not keyword:
        return ""
    return f"https://leetcode.cn/problemset/all/?search={quote(keyword)}"


def read_text_with_fallback(path: str) -> str:
    encodings = ["utf-8", "utf-8-sig", "gb18030", "gbk"]
    last_error = None
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc) as f:
                return f.read()
        except Exception as e:
            last_error = e
    raise RuntimeError(f"无法读取文件 {path}: {last_error}")


def extract_frontend_id_and_title(line: str):
    m = re.match(r"^(.+?)\.\s*(.+)$", line)
    if not m:
        return "", ""

    front = (m.group(1) or "").strip()
    title = (m.group(2) or "").strip()
    if not title:
        return "", ""
    return front, title


def parse_raw_alg_md(path: str) -> List[Dict]:
    if not path or not os.path.exists(path):
        return []

    text = read_text_with_fallback(path)
    lines = [line.strip() for line in text.splitlines()]

    entries = []
    i = 0
    n = len(lines)

    while i < n:
        line = lines[i].strip()
        if not line:
            i += 1
            continue

        frontend_id, title = extract_frontend_id_and_title(line)
        if not title:
            i += 1
            continue

        i += 1
        while i < n and not lines[i].strip():
            i += 1

        difficulty = ""
        url = ""
        last_date = ""
        frequency = 0

        if i < n:
            diff_line = lines[i].strip()
            i += 1

            diff_match = re.search(r"(简单|容易|中等|困难|easy|medium|hard)", diff_line, flags=re.IGNORECASE)
            if diff_match:
                difficulty = normalize_difficulty(diff_match.group(1))

            url_match = URL_RE.search(diff_line)
            if url_match:
                url = url_match.group(0)

        while i < n and not lines[i].strip():
            i += 1

        if i < n and DATE_RE.match(lines[i]):
            last_date = lines[i]
            i += 1

        while i < n and not lines[i].strip():
            i += 1

        if i < n and re.fullmatch(r"\d+", lines[i]):
            frequency = int(lines[i])
            i += 1

        problem_id = parse_int(frontend_id)
        if not url:
            url = ""

        entries.append(
            {
                "id": problem_id,
                "frontend_id": frontend_id,
                "title": title,
                "difficulty": difficulty,
                "last_date": last_date,
                "frequency": frequency,
                "url": url,
                "source": "raw_alg_md",
            }
        )

    return entries


def fetch_codetop_questions(
    base_url: str = "https://codetop.cc",
    timeout: int = 20,
    max_pages: int = 200,
    max_retries: int = 6,
    page_delay: float = 0.2,
) -> List[Dict]:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Referer": base_url + "/home",
        }
    )

    entries = []
    page = 1
    total = None

    while page <= max_pages:
        url = f"{base_url}/api/questions/?page={page}"
        resp = None
        for attempt in range(max_retries):
            current = session.get(url, timeout=timeout)
            if current.status_code == 429:
                wait_secs = min(2 ** attempt, 30)
                print(f"[sync] page {page} got 429, retry in {wait_secs}s")
                time.sleep(wait_secs)
                continue

            if current.status_code >= 500:
                wait_secs = min(2 ** attempt, 10)
                print(f"[sync] page {page} got {current.status_code}, retry in {wait_secs}s")
                time.sleep(wait_secs)
                continue

            resp = current
            break

        if resp is None:
            raise RuntimeError(f"codetop page {page} failed after {max_retries} retries")

        resp.raise_for_status()
        payload = resp.json()

        if not isinstance(payload, dict):
            break

        if total is None:
            total = parse_int(payload.get("count")) or 0

        rows = payload.get("list") or []
        if not rows:
            break

        for row in rows:
            if not isinstance(row, dict):
                continue

            leet = row.get("leetcode") or {}
            if not isinstance(leet, dict):
                continue

            title = str(leet.get("title") or "").strip()
            if not title:
                continue

            frontend_id = str(leet.get("frontend_question_id") or "").strip()
            question_id = parse_int(frontend_id)
            if question_id is None:
                question_id = parse_int(leet.get("question_id"))

            level = normalize_difficulty(leet.get("level"))
            slug = str(leet.get("slug_title") or "").strip()
            url = f"https://leetcode.cn/problems/{slug}" if slug else ""

            time_value = str(row.get("time") or "")
            last_date = time_value[:10] if DATE_RE.match(time_value[:10]) else ""
            frequency = parse_int(row.get("value")) or 0

            entries.append(
                {
                    "id": question_id,
                    "frontend_id": frontend_id,
                    "title": title,
                    "difficulty": level,
                    "last_date": last_date,
                    "frequency": frequency,
                    "url": url,
                    "source": "codetop",
                }
            )

        if total and len(entries) >= total:
            break

        page += 1
        if page_delay > 0:
            time.sleep(page_delay)

    return entries


def merge_hot_entries(entries: List[Dict]) -> List[Dict]:
    merged: Dict[str, Dict] = {}
    title_index: Dict[str, str] = {}

    def merge_two(first: Dict, second: Dict) -> Dict:
        first_freq = parse_int(first.get("frequency")) or 0
        second_freq = parse_int(second.get("frequency")) or 0
        if first_freq >= second_freq:
            winner, loser = first, second
        else:
            winner, loser = second, first

        for field in ("id", "frontend_id", "difficulty", "last_date", "url"):
            if winner.get(field) in (None, "") and loser.get(field) not in (None, ""):
                winner[field] = loser.get(field)

        old_source = str(winner.get("source") or "")
        new_source = str(loser.get("source") or "")
        if old_source and new_source and new_source not in old_source:
            winner["source"] = old_source + "," + new_source
        elif not old_source and new_source:
            winner["source"] = new_source

        return winner

    for item in entries:
        if not isinstance(item, dict):
            continue

        title = str(item.get("title") or "").strip()
        if not title:
            continue

        problem_id = parse_int(item.get("id"))
        frontend_id = str(item.get("frontend_id") or "").strip()
        difficulty = normalize_difficulty(item.get("difficulty"))
        last_date = str(item.get("last_date") or "").strip()
        frequency = parse_int(item.get("frequency")) or 0
        url = str(item.get("url") or "").strip()
        source = str(item.get("source") or "").strip()

        normalized = {
            "id": problem_id,
            "frontend_id": frontend_id,
            "title": title,
            "difficulty": difficulty,
            "last_date": last_date,
            "frequency": frequency,
            "url": url,
            "source": source,
        }

        norm_title = normalize_text(title)
        title_key = f"title:{norm_title}"
        id_key = f"id:{problem_id}" if problem_id is not None else None
        mapped_title_key = title_index.get(norm_title)

        selected_key = None
        if id_key and id_key in merged:
            selected_key = id_key
        elif mapped_title_key and mapped_title_key in merged:
            selected_key = mapped_title_key
        elif title_key in merged:
            selected_key = title_key
        elif id_key:
            selected_key = id_key
        else:
            selected_key = title_key

        if id_key and selected_key != id_key and selected_key in merged and selected_key.startswith("title:"):
            merged[id_key] = merged[selected_key]
            del merged[selected_key]
            selected_key = id_key

        old = merged.get(selected_key)
        if not old:
            merged[selected_key] = normalized
        else:
            merged[selected_key] = merge_two(old, normalized)

        if id_key and selected_key == id_key and title_key in merged and title_key != id_key:
            merged[id_key] = merge_two(merged[id_key], merged[title_key])
            del merged[title_key]

        final_title = str(merged[selected_key].get("title") or "").strip()
        final_norm_title = normalize_text(final_title)
        if final_norm_title:
            title_index[final_norm_title] = selected_key

    hot_list = list(merged.values())
    for item in hot_list:
        if not str(item.get("url") or "").strip():
            item["url"] = build_fallback_url(item.get("title") or "")

    hot_list.sort(key=lambda x: (-(parse_int(x.get("frequency")) or 0), str(x.get("title") or "")))
    return hot_list


def build_slim_hot_entries(hot_entries: List[Dict]) -> List[Dict]:
    slim_entries: List[Dict] = []

    for item in hot_entries:
        if not isinstance(item, dict):
            continue

        title = str(item.get("title") or "").strip()
        problem_id = parse_int(item.get("id"))
        if problem_id is not None and not (1 <= problem_id <= 9999):
            problem_id = None

        if not title and problem_id is None:
            continue

        difficulty = normalize_difficulty(item.get("difficulty"))
        frequency = parse_int(item.get("frequency")) or 1
        if frequency <= 0:
            frequency = 1

        url = str(item.get("url") or item.get("link") or "").strip()
        if not url and title:
            url = build_fallback_url(title)

        slim: Dict = {}
        if problem_id is not None:
            slim["id"] = problem_id
        if title:
            slim["title"] = title
        if difficulty:
            slim["difficulty"] = difficulty
        slim["frequency"] = frequency
        if url:
            slim["url"] = url

        slim_entries.append(slim)

    slim_entries.sort(key=lambda x: (-(parse_int(x.get("frequency")) or 0), str(x.get("title") or "")))
    return slim_entries


def update_alg_json(alg_json_path: str, hot_entries: List[Dict]):
    slim_hot_entries = build_slim_hot_entries(hot_entries)
    alg: Dict = {
        "version": datetime.datetime.now().strftime("alg-sync-%Y%m%d"),
    }

    problem_ids = set()
    problem_titles = []
    seen_titles = set()
    for item in slim_hot_entries:
        pid = parse_int(item.get("id"))
        if pid is not None and 1 <= pid <= 9999:
            problem_ids.add(pid)

        title = str(item.get("title") or "").strip()
        norm_title = normalize_text(title)
        if title and norm_title not in seen_titles:
            seen_titles.add(norm_title)
            problem_titles.append(title)

    alg["problem_ids"] = sorted(problem_ids)
    alg["problem_titles"] = problem_titles
    alg["interview_hot_problems"] = slim_hot_entries

    with open(alg_json_path, "w", encoding="utf-8") as f:
        json.dump(alg, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser(description="Sync alg.json hot problems from remote API and optional raw markdown list.")
    parser.add_argument("--alg-json", default="alg.json", help="alg.json path to update")
    parser.add_argument("--raw-md", default=os.path.join("..", "RAW", "alg.md"), help="optional raw markdown source")
    parser.add_argument("--skip-codetop", action="store_true", help="skip fetching codetop API")
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout seconds")
    parser.add_argument("--max-pages", type=int, default=200, help="max codetop pages to scan")
    parser.add_argument("--retries", type=int, default=6, help="max retries per page for codetop API")
    parser.add_argument("--page-delay", type=float, default=0.2, help="delay seconds between page requests")
    args = parser.parse_args()

    all_entries: List[Dict] = []

    if not args.skip_codetop:
        codetop_entries = fetch_codetop_questions(
            timeout=args.timeout,
            max_pages=args.max_pages,
            max_retries=args.retries,
            page_delay=args.page_delay,
        )
        print(f"[sync] codetop entries: {len(codetop_entries)}")
        all_entries.extend(codetop_entries)

    raw_entries = parse_raw_alg_md(args.raw_md)
    if raw_entries:
        print(f"[sync] raw md entries: {len(raw_entries)}")
        all_entries.extend(raw_entries)

    if not all_entries:
        raise RuntimeError("No entries found from codetop or raw markdown.")

    merged = merge_hot_entries(all_entries)
    print(f"[sync] merged entries: {len(merged)}")

    update_alg_json(args.alg_json, merged)
    print(f"[sync] updated: {args.alg_json}")


if __name__ == "__main__":
    main()
