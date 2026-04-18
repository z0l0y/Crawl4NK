import json
import logging
import random
import re
import time
import urllib.parse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.exceptions import Timeout
from urllib3.util.retry import Retry

from matcher import TextMatcher

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class NowcoderCrawler:
    def __init__(self, config):
        self.config = config
        self.keywords = config.get("keywords", ["backend interview"])
        self.max_pages = max(int(config.get("max_pages", 1) or 1), 1)
        self.max_items = max(int(config.get("max_items_per_keyword", 10) or 0), 0)
        self.max_pages_hard_limit = max(int(config.get("max_pages_hard_limit", 50) or 50), self.max_pages)
        self.fill_valid_quota = bool(config.get("fill_valid_quota", False))
        self.filter_rules = config.get("filter_rules", {})
        self.ac_backend = str(config.get("ac_backend", "auto") or "auto").strip().lower()
        self.ac_native_min_patterns = int(config.get("ac_native_min_patterns", 64) or 64)
        self.crawl_debug_log = bool(config.get("crawl_debug_log", config.get("debug_log", False)))
        self.show_progress_bar = bool(config.get("show_progress_bar", True))
        self.request_connect_timeout = float(config.get("request_connect_timeout", 5) or 5)
        self.request_read_timeout = float(config.get("request_read_timeout", 15) or 15)
        self.request_retry_count = int(config.get("request_retry_count", 2) or 2)
        self.request_retry_backoff = float(config.get("request_retry_backoff", 0.5) or 0.5)
        self.filtered_posts_log = bool(config.get("filtered_posts_log", False))
        self.filtered_posts_log_limit = int(config.get("filtered_posts_log_limit", 50) or 50)
        self.score_filtered_count = 0
        self.score_filtered_examples = []
        self._progress_active = False

        if self.crawl_debug_log:
            logging.getLogger().setLevel(logging.INFO)
        else:
            logging.getLogger().setLevel(logging.WARNING)

        self.matcher = TextMatcher(
            self.filter_rules.get("must_contain", []),
            backend=self.ac_backend,
            native_min_patterns=self.ac_native_min_patterns,
            allow_overrides=self.filter_rules.get("allow_overrides", []),
            normalization=self.filter_rules.get("normalization", {}),
            skip_char_match=self.filter_rules.get("skip_char_match", {}),
            force_combine=self.filter_rules.get("force_combine", {}),
            pattern_cache=self.filter_rules.get("pattern_cache", {}),
            char_id_compression=self.filter_rules.get("char_id_compression", {}),
            score_filter=self.filter_rules.get("score_filter", {}),
        )
        self.score_filter_enabled = bool(self.matcher.score_enabled)
        self.score_filter_threshold = int(self.matcher.score_threshold)
        self.score_parallel_batch_size = max(int(getattr(self.matcher, "score_parallel_batch_size", 8) or 8), 1)
        self.must_not_patterns = self._prepare_must_not_patterns(self.filter_rules.get("must_not_contain", []))

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Content-Type": "application/json",
            "Origin": "https://www.nowcoder.com",
            "Referer": "https://www.nowcoder.com/",
            "Cookie": config.get("cookie", ""),
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

        retry = Retry(
            total=self.request_retry_count,
            connect=self.request_retry_count,
            read=self.request_retry_count,
            status=self.request_retry_count,
            backoff_factor=self.request_retry_backoff,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET", "POST"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self.search_timeout = (self.request_connect_timeout, self.request_read_timeout)
        self.detail_api_timeout = (self.request_connect_timeout, self.request_read_timeout)
        self.detail_page_timeout = (self.request_connect_timeout, self.request_read_timeout + 5)
        self.comment_timeout = (self.request_connect_timeout, self.request_read_timeout)

    def _ensure_progress_newline(self):
        if self._progress_active:
            print()
            self._progress_active = False

    def _log_warning(self, message: str):
        self._ensure_progress_newline()
        logging.warning(message)

    def _log_error(self, message: str):
        self._ensure_progress_newline()
        logging.error(message)

    def _log_network_exception(self, stage: str, identifier: str, exc: Exception):
        if isinstance(exc, (Timeout, TimeoutError)):
            msg = f"{stage} timeout, skipped: {identifier}"
            if self.crawl_debug_log:
                msg += f" ({exc})"
            self._log_warning(msg)
            return

        msg = f"{stage} request failed: {identifier}"
        if self.crawl_debug_log:
            msg += f" ({exc})"
        self._log_error(msg)

    def _render_progress(self, current: int, total: int, prefix: str):
        if total <= 0:
            return
        width = 30
        ratio = min(max(current / total, 0), 1)
        filled = int(width * ratio)
        bar = "#" * filled + "-" * (width - filled)
        print(f"\r{prefix}: [{bar}] {current}/{total}", end="", flush=True)
        if current >= total:
            print()
            self._progress_active = False
        else:
            self._progress_active = True

    def _render_keyword_status(self, keyword: str, candidates: int, archived: int):
        print(
            f"\r[{keyword}] candidates processed: {candidates}, archived: {archived}",
            end="",
            flush=True,
        )
        self._progress_active = True

    def _is_valid_title(self, title):
        if self.matcher.is_allow_override_hit(title):
            return True

        normalized_title = self.matcher.normalize_text(title)
        for word in self.must_not_patterns:
            if word in normalized_title:
                return False
        return True

    def _prepare_must_not_patterns(self, patterns):
        normalized_patterns = []
        for pattern in patterns or []:
            normalized = self.matcher.normalize_text(pattern)
            if normalized:
                normalized_patterns.append(normalized)
        return normalized_patterns

    def _effective_page_limit(self) -> int:
        if self.max_items <= 0:
            return self.max_pages

        adaptive = max(self.max_pages, self.max_items * 3, self.max_pages * 4)
        return min(adaptive, self.max_pages_hard_limit)

    def _is_valid_content(self, title, content_text=""):
        if self.filter_rules.get("must_contain"):
            return self.matcher.match(title, content_text)
        return True

    def _record_score_filtered_post(self, post: dict, score_report: dict):
        self.score_filtered_count += 1

        limit = max(self.filtered_posts_log_limit, 0)
        if limit > 0 and len(self.score_filtered_examples) >= limit:
            return

        breakdown = score_report.get("breakdown", {})
        self.score_filtered_examples.append(
            {
                "title": post.get("title", ""),
                "keyword": post.get("keyword", ""),
                "url": post.get("url", ""),
                "score": score_report.get("score", 0),
                "threshold": score_report.get("threshold", self.score_filter_threshold),
                "content_chars": breakdown.get("content_chars", 0),
                "alg_score": breakdown.get("alg_score", 0),
                "alg_hot_score": breakdown.get("alg_hot_score", 0),
                "alg_problem_hits": breakdown.get("alg_problem_hits", []),
                "alg_topic_hits": breakdown.get("alg_topic_hits", []),
                "alg_problem_id_hits": breakdown.get("alg_problem_id_hits", []),
                "alg_hot_title_hits": breakdown.get("alg_hot_title_hits", []),
                "alg_hot_id_hits": breakdown.get("alg_hot_id_hits", []),
                "alg_hot_matches": breakdown.get("alg_hot_matches", []),
                "tail_tags": breakdown.get("tail_tags", []),
            }
        )

    def iter_search_posts(self, keyword):
        url = "https://gw-c.nowcoder.com/api/sparta/pc/search"
        seen_posts = set()
        page_limit = self._effective_page_limit()
        empty_page_streak = 0

        for page in range(1, page_limit + 1):
            payload = {
                "type": "all",
                "query": keyword,
                "page": page,
                "tag": [],
                "order": "create",
            }
            try:
                if self.crawl_debug_log:
                    logging.info(f"Searching keyword '{keyword}' page {page}...")
                resp = self.session.post(url, json=payload, timeout=self.search_timeout)
                if resp.status_code == 200:
                    data = resp.json()
                    records = data.get("data", {}).get("records", [])
                    if not records:
                        empty_page_streak += 1
                        if page <= self.max_pages or empty_page_streak >= 2:
                            break
                        continue

                    matched_in_page = 0
                    empty_page_streak = 0

                    for record in records:
                        item_data = record.get("data", {})
                        moment_data = item_data.get("momentData", {})
                        content_data = item_data.get("contentData", {})

                        post_id = moment_data.get("id") or content_data.get("id")
                        post_uuid = moment_data.get("uuid") or content_data.get("uuid")
                        detail_api_type = "moment-data" if moment_data.get("uuid") else "content-data"
                        title = moment_data.get("title") or content_data.get("title") or "untitled"
                        raw_content = content_data.get("content") or moment_data.get("content") or ""

                        if post_id and post_uuid:
                            if not self._is_valid_title(title):
                                if self.crawl_debug_log:
                                    logging.info(f"[search_posts] title blocked by blacklist/validation: {title}")
                                continue

                            if not self._is_valid_content(title, raw_content):
                                if self.crawl_debug_log:
                                    logging.info(f"[search_posts] content not matched, skip: {title}")
                                continue

                            if self.crawl_debug_log:
                                logging.info(f"[search_posts] matched candidate: {title}")
                            unique_key = (str(post_id), str(post_uuid))
                            if unique_key in seen_posts:
                                continue
                            seen_posts.add(unique_key)
                            matched_in_page += 1

                            yield {
                                "id": post_id,
                                "uuid": post_uuid,
                                "detail_api_type": detail_api_type,
                                "title": title,
                                "keyword": keyword,
                                "search_content": raw_content,
                            }

                    if matched_in_page == 0 and page > self.max_pages:
                        empty_page_streak += 1
                        if empty_page_streak >= 2:
                            break
                elif page > self.max_pages:
                    empty_page_streak += 1
                    if empty_page_streak >= 2:
                        break

                time.sleep(random.uniform(1.5, 3.5))
            except Exception as e:
                self._log_network_exception("search", f"{keyword} page {page}", e)

    def search_posts(self, keyword):
        posts = []
        for item in self.iter_search_posts(keyword):
            posts.append(item)
            if self.max_items > 0 and (not self.fill_valid_quota) and len(posts) >= self.max_items:
                break
        return posts

    def _build_detail_candidates(self, post_id, post_uuid, detail_api_type=None):
        candidates = []
        if detail_api_type == "content-data":
            if post_id is not None:
                candidates.append(("content-data", str(post_id)))
            if post_uuid:
                candidates.append(("moment-data", str(post_uuid)))
        elif detail_api_type == "moment-data":
            if post_uuid:
                candidates.append(("moment-data", str(post_uuid)))
            if post_id is not None:
                candidates.append(("content-data", str(post_id)))
        else:
            if post_id is not None:
                candidates.append(("content-data", str(post_id)))
            if post_uuid:
                candidates.append(("moment-data", str(post_uuid)))

        return candidates

    def _fetch_content_from_detail_api(self, candidate_details, api_headers):
        visited = set()
        for api_type, detail_id in candidate_details:
            key = (api_type, detail_id)
            if key in visited:
                continue
            visited.add(key)

            try:
                api_url = f"https://gw-c.nowcoder.com/api/sparta/detail/{api_type}/detail/{detail_id}"
                api_resp = self.session.get(api_url, headers=api_headers, timeout=self.detail_api_timeout)
                if api_resp.status_code != 200:
                    continue

                res_json = api_resp.json()
                data = res_json.get("data")
                if isinstance(data, dict) and data.get("content"):
                    return data.get("content", "")
            except Exception as e:
                if self.crawl_debug_log:
                    logging.debug(f"API detail fallback to next endpoint: {e}")

        return ""

    def _extract_content_from_initial_state(self, page_text: str, post_uuid) -> str:
        match = re.search(
            r"window\.__INITIAL_STATE__\s*=\s*(.*?)(?:;\(function|;\s*</script>|</script>)",
            page_text,
            flags=re.DOTALL,
        )
        if not match:
            return ""

        try:
            raw_data = match.group(1).strip(" \t\r\n;")
            if raw_data.startswith("%7B") or raw_data.startswith("%22"):
                raw_data = urllib.parse.unquote(raw_data)

            data = json.loads(raw_data)
            if isinstance(data, str):
                data = json.loads(data)

            for value in data.get("prefetchData", {}).values():
                if not isinstance(value, dict) or "ssrCommonData" not in value:
                    continue
                ssr_data = value.get("ssrCommonData", {})
                for data_key in ("contentData", "momentData"):
                    item_dict = ssr_data.get(data_key)
                    if isinstance(item_dict, dict) and item_dict.get("content"):
                        return item_dict.get("content", "")
        except Exception as e:
            if self.crawl_debug_log:
                self._log_warning(f"Failed to parse embedded JSON (UUID: {post_uuid}): {e}")

        return ""

    @staticmethod
    def _extract_content_from_html(soup: BeautifulSoup) -> str:
        content_div = (
            soup.find("div", class_="feed-content-text")
            or soup.find("div", class_="nc-slate-editor-content")
            or soup.find("div", class_="nc-post-content")
            or soup.find("div", class_="post-content")
            or soup.find("div", class_="feed-content")
            or soup.find("div", class_="content-box")
        )

        if not content_div:
            return ""

        for tag in content_div.find_all(["br", "p", "div"]):
            tag.insert_after("\n")
        return content_div.get_text(separator=" ", strip=True)

    @staticmethod
    def _clean_detail_content(content: str) -> str:
        return str(content or "").replace("\u200b", "")

    def _archive_limit_reached(self, archived_for_keyword: int) -> bool:
        return self.max_items > 0 and (not self.fill_valid_quota) and archived_for_keyword >= self.max_items

    def _flush_scoring_buffer(self, scoring_buffer: list, all_data: list, archived_for_keyword: int) -> tuple[int, bool]:
        if not scoring_buffer:
            return archived_for_keyword, False

        inputs = [
            {
                "title": item.get("title", ""),
                "content": item.get("content", ""),
                "tags": item.get("tags", []),
            }
            for item in scoring_buffer
        ]
        score_reports = self.matcher.evaluate_posts_quality_parallel(inputs)

        reached_limit = False
        for item, score_report in zip(scoring_buffer, score_reports):
            if score_report.get("enabled"):
                item["quality_score"] = score_report.get("score", 0)
                item["quality_score_breakdown"] = score_report.get("breakdown", {})
                item["algorithm_matches"] = score_report.get("breakdown", {}).get("alg_hot_matches", [])

                if not score_report.get("passed", True):
                    self._record_score_filtered_post(item, score_report)
                    if self.crawl_debug_log:
                        logging.info(
                            f"[-] score below threshold ({score_report.get('score', 0)}/"
                            f"{score_report.get('threshold', self.score_filter_threshold)}), filtered: {item['title']}"
                        )
                    continue

            item["comments"] = self.get_comments(item["id"])
            all_data.append(item)
            archived_for_keyword += 1
            if self.crawl_debug_log:
                logging.info(f"archived: {item['title']}")

            if self._archive_limit_reached(archived_for_keyword):
                reached_limit = True
                break

        scoring_buffer.clear()
        return archived_for_keyword, reached_limit

    def get_post_detail(self, post_id, post_uuid, detail_api_type=None):
        canonical_url = f"https://www.nowcoder.com/feed/main/detail/{post_uuid}"
        if detail_api_type == "content-data" and post_id is not None:
            canonical_url = f"https://www.nowcoder.com/discuss/{post_id}"

        url = canonical_url
        try:
            api_headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
            }
            candidate_details = self._build_detail_candidates(post_id, post_uuid, detail_api_type)
            content = self._fetch_content_from_detail_api(candidate_details, api_headers)

            if content:
                return {"content": self._clean_detail_content(content), "tags": [], "url": canonical_url}

            url = canonical_url
            resp = self.session.get(url, timeout=self.detail_page_timeout)
            if resp.status_code == 200 and not content:
                content = self._extract_content_from_initial_state(resp.text, post_uuid)

                soup = BeautifulSoup(resp.text, "html.parser")
                if not content:
                    content = self._extract_content_from_html(soup)

                if content:
                    content = self._clean_detail_content(content)

                tags = [tag.get_text(strip=True) for tag in soup.find_all("a", class_="discuss-tag-item") if soup]
                return {"content": content, "tags": tags, "url": url}
            elif content:
                return {"content": self._clean_detail_content(content), "tags": [], "url": canonical_url}

            time.sleep(random.uniform(1.0, 2.0))
        except Exception as e:
            self._log_network_exception("detail", f"UUID: {post_uuid}", e)

        return {"content": "", "tags": [], "url": url}

    def get_comments(self, post_id):
        comments_list = []
        url = "https://gw-c.nowcoder.com/api/sparta/reply/v2/reply/list"
        payload = {
            "entityId": post_id,
            "entityType": 8,
            "page": 1,
            "pageSize": 50,
            "order": 1,
        }
        try:
            resp = self.session.post(url, json=payload, timeout=self.comment_timeout)
            if resp.status_code == 200:
                data = resp.json().get("data", {}).get("records", [])
                for c in data:
                    comment_text = c.get("content", "")
                    comments_list.append(comment_text)
                    sub_replies = c.get("replyList", [])
                    for sub in sub_replies:
                        comments_list.append(sub.get("content", ""))
            time.sleep(random.uniform(1.0, 2.5))
        except Exception as e:
            self._log_network_exception("comment", f"ID: {post_id}", e)
        return comments_list

    def crawl(self):
        all_data = []
        try:
            total_keywords = len(self.keywords)
            for kw_index, kw in enumerate(self.keywords, start=1):
                archived_for_keyword = 0
                handled_candidates = 0
                scoring_buffer = []

                reached_quota = False
                for p in self.iter_search_posts(kw):
                    if self._archive_limit_reached(archived_for_keyword):
                        break

                    handled_candidates += 1
                    detail = self.get_post_detail(p.get("id"), p.get("uuid"), p.get("detail_api_type"))

                    final_content = detail.get("content")
                    if not final_content:
                        final_content = p.get("search_content", "")

                    if final_content:
                        p.update(detail)
                        p["content"] = final_content
                        scoring_buffer.append(p)

                        if len(scoring_buffer) >= self.score_parallel_batch_size:
                            archived_for_keyword, reached_quota = self._flush_scoring_buffer(
                                scoring_buffer,
                                all_data,
                                archived_for_keyword,
                            )
                            if reached_quota:
                                if self.crawl_debug_log:
                                    logging.info(f"keyword '{kw}' reached archive target {self.max_items}, stop early")
                                break
                    else:
                        if self.crawl_debug_log:
                            logging.warning(f"[-] drop post with empty content: {p['title']} [{p['id']}]")

                    if self.show_progress_bar and not self.crawl_debug_log:
                        self._render_keyword_status(kw, handled_candidates, archived_for_keyword)

                if not reached_quota:
                    archived_for_keyword, _ = self._flush_scoring_buffer(
                        scoring_buffer,
                        all_data,
                        archived_for_keyword,
                    )

                self._ensure_progress_newline()
                if self.show_progress_bar and not self.crawl_debug_log:
                    self._render_progress(kw_index, total_keywords, "keyword processing")

                if self.max_items > 0 and archived_for_keyword < self.max_items:
                    self._log_warning(
                        f"keyword '{kw}' archived {archived_for_keyword}/{self.max_items}, scanned up to page limit {self._effective_page_limit()}"
                    )

            return all_data
        finally:
            try:
                self.matcher.shutdown()
            except Exception:
                pass
