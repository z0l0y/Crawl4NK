import json
import heapq
import logging
import math
import random
import re
import sys
import threading
import time
import urllib.parse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.exceptions import Timeout
from urllib3.util.retry import Retry

from matcher import TextMatcher
from scheduler import SearchPageScheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def _configure_retry_log_handler():
    retry_logger_names = (
        "urllib3.connectionpool",
        "urllib3.util.retry",
        "requests.packages.urllib3.connectionpool",
        "requests.packages.urllib3.util.retry",
    )
    for logger_name in retry_logger_names:
        logger = logging.getLogger(logger_name)
        if any(getattr(handler, "_crawl4nk_newline", False) for handler in logger.handlers):
            continue

        handler = logging.StreamHandler()
        handler._crawl4nk_newline = True
        handler.setFormatter(logging.Formatter("\n%(asctime)s - %(levelname)s - %(message)s"))
        logger.handlers = [handler]
        logger.propagate = False
        logger.setLevel(logging.WARNING)


_configure_retry_log_handler()


class NowcoderCrawler:
    def __init__(self, config):
        self.config = config
        self.keywords = config.get("keywords", ["backend interview"])
        self.max_pages = max(int(config.get("max_pages", 1) or 1), 1)
        self.max_items = max(int(config.get("max_items_per_keyword", 10) or 0), 0)
        self.max_pages_hard_limit = max(int(config.get("max_pages_hard_limit", 50) or 50), self.max_pages)
        self.fill_valid_quota = bool(config.get("fill_valid_quota", False))
        self.search_page_strategy = str(config.get("search_page_strategy", "bfs") or "bfs").strip().lower()
        if self.search_page_strategy not in SearchPageScheduler.SUPPORTED_STRATEGIES:
            self.search_page_strategy = "bfs"
        try:
            best_first_frontload = int(config.get("best_first_frontload_pages", 3) or 3)
        except Exception:
            best_first_frontload = 3
        try:
            best_first_explore_stride = int(config.get("best_first_explore_stride", 5) or 5)
        except Exception:
            best_first_explore_stride = 5
        self.best_first_frontload_pages = max(best_first_frontload, 1)
        self.best_first_explore_stride = max(best_first_explore_stride, 2)
        self.filter_rules = config.get("filter_rules", {})
        self.score_filter_cfg = self.filter_rules.get("score_filter", {})
        self.ac_backend = str(config.get("ac_backend", "auto") or "auto").strip().lower()
        self.ac_native_min_patterns = int(config.get("ac_native_min_patterns", 64) or 64)
        self.crawl_debug_log = bool(config.get("crawl_debug_log", config.get("debug_log", False)))
        self.init_profile_log = bool(config.get("init_profile_log", False))
        self.show_progress_bar = bool(config.get("show_progress_bar", True))
        self.keyword_status_log = bool(config.get("keyword_status_log", False))
        self.search_page_progress_log = bool(config.get("search_page_progress_log", True))
        self.search_page_stop_reason_log = bool(config.get("search_page_stop_reason_log", True))
        self.pipeline_stage_progress_log = bool(config.get("pipeline_stage_progress_log", True))
        self.activity_status_enabled = bool(config.get("activity_status_enabled", True))
        try:
            progress_interval = float(config.get("progress_render_interval_sec", 0.08) or 0.08)
        except Exception:
            progress_interval = 0.08
        self.progress_render_interval_sec = max(progress_interval, 0.03)
        try:
            activity_interval = float(config.get("activity_status_interval_sec", 0.15) or 0.15)
        except Exception:
            activity_interval = 0.15
        self.activity_status_interval_sec = max(activity_interval, 0.05)
        self.timing_profile_log = bool(config.get("timing_profile_log", False))
        self.timing_profile_keyword_log = bool(config.get("timing_profile_keyword_log", self.timing_profile_log))
        self.timing_profile_topn = max(int(config.get("timing_profile_topn", 6) or 6), 1)
        self.timing_profile_live_log = bool(config.get("timing_profile_live_log", self.timing_profile_log))
        self.progress_prefer_status = bool(config.get("progress_prefer_status", True))
        self.timing_profile_live_topn = max(
            int(config.get("timing_profile_live_topn", self.timing_profile_topn) or self.timing_profile_topn),
            1,
        )
        try:
            live_interval = float(config.get("timing_profile_live_interval_sec", 0.35) or 0.35)
        except Exception:
            live_interval = 0.35
        self.timing_profile_live_interval_sec = max(live_interval, 0.1)
        self.request_connect_timeout = float(config.get("request_connect_timeout", 5) or 5)
        self.request_read_timeout = float(config.get("request_read_timeout", 15) or 15)
        self.request_retry_count = int(config.get("request_retry_count", 2) or 2)
        self.request_retry_backoff = float(config.get("request_retry_backoff", 0.5) or 0.5)
        self.proxy_rotation_enabled = bool(config.get("proxy_rotation_enabled", False))
        raw_proxy_pool = config.get("proxy_pool", [])
        if isinstance(raw_proxy_pool, str):
            raw_proxy_pool = [raw_proxy_pool]
        self.proxy_pool = [str(item or "").strip() for item in (raw_proxy_pool or []) if str(item or "").strip()]
        self.http_proxy = str(config.get("http_proxy", "") or "").strip()
        self.https_proxy = str(config.get("https_proxy", "") or "").strip()
        self._proxy_cursor = 0
        try:
            comment_min_interval = float(config.get("comment_request_min_interval_sec", 0.25) or 0.25)
        except Exception:
            comment_min_interval = 0.25
        try:
            comment_max_interval = float(config.get("comment_request_max_interval_sec", 0.8) or 0.8)
        except Exception:
            comment_max_interval = 0.8
        self.comment_request_min_interval_sec = max(comment_min_interval, 0.0)
        self.comment_request_max_interval_sec = max(comment_max_interval, 0.0)
        if self.comment_request_max_interval_sec < self.comment_request_min_interval_sec:
            self.comment_request_min_interval_sec, self.comment_request_max_interval_sec = (
                self.comment_request_max_interval_sec,
                self.comment_request_min_interval_sec,
            )
        self.fetch_comments_enabled = bool(config.get("fetch_comments_enabled", False))
        self.score_heap_selection_enabled = bool(self.score_filter_cfg.get("heap_selection_enabled", True))
        raw_heap_multiplier = self.score_filter_cfg.get(
            "heap_pool_multiplier",
            self.score_filter_cfg.get("heap_oversample_factor", 1.5),
        )
        try:
            heap_multiplier = float(raw_heap_multiplier)
        except (TypeError, ValueError):
            heap_multiplier = 1.5
        self.score_heap_pool_multiplier = max(heap_multiplier, 1.0)
        self.score_heap_max_pool = max(int(self.score_filter_cfg.get("heap_max_pool", 0) or 0), 0)

        self.score_heap_elastic_enabled = bool(self.score_filter_cfg.get("heap_elastic_enabled", True))
        raw_heap_elastic_max_multiplier = self.score_filter_cfg.get("heap_elastic_max_multiplier", 2.2)
        try:
            heap_elastic_max_multiplier = float(raw_heap_elastic_max_multiplier)
        except (TypeError, ValueError):
            heap_elastic_max_multiplier = 2.2
        self.score_heap_elastic_max_multiplier = max(heap_elastic_max_multiplier, self.score_heap_pool_multiplier)

        raw_heap_elastic_step_ratio = self.score_filter_cfg.get("heap_elastic_step_ratio", 0.25)
        try:
            heap_elastic_step_ratio = float(raw_heap_elastic_step_ratio)
        except (TypeError, ValueError):
            heap_elastic_step_ratio = 0.25
        self.score_heap_elastic_step_ratio = min(max(heap_elastic_step_ratio, 0.05), 1.0)

        self.score_heap_elastic_replace_threshold = max(
            int(self.score_filter_cfg.get("heap_elastic_replace_threshold", 6) or 6),
            1,
        )
        self.score_heap_elastic_shrink_idle_rounds = max(
            int(self.score_filter_cfg.get("heap_elastic_shrink_idle_rounds", 3) or 3),
            1,
        )
        legacy_filtered_posts_log = bool(config.get("filtered_posts_log", False))
        legacy_filtered_posts_log_limit = int(config.get("filtered_posts_log_limit", 50) or 50)
        self.score_filtered_posts_log = bool(
            config.get("score_filtered_posts_log", legacy_filtered_posts_log)
        )
        self.score_filtered_posts_log_limit = int(
            config.get("score_filtered_posts_log_limit", legacy_filtered_posts_log_limit)
            or legacy_filtered_posts_log_limit
        )
        self.score_filtered_count = 0
        self.score_filtered_examples = []
        self._search_page_stats = {}
        self._search_external_stop_reason = {}
        self._score_automaton_building_notified = False
        self._score_automaton_ready_notified = False
        self._progress_active = False
        self._timing_totals = {
            "search_api": 0.0,
            "detail_fetch": 0.0,
            "score_eval": 0.0,
            "comment_fetch": 0.0,
        }
        self._timing_counts = {
            "search_api": 0,
            "detail_fetch": 0,
            "score_eval": 0,
            "comment_fetch": 0,
        }
        self._timing_live_last_ts = 0.0
        self._timing_live_label = ""
        self._timing_live_started_at = 0.0
        self._timing_live_line_len = 0
        self._activity_last_ts = 0.0
        self._activity_frame_index = 0
        self._activity_line_len = 0
        self._activity_frames = "|/-\\"
        self._live_line_len = 0
        self._progress_last_render_ts = 0.0
        self._progress_last_signature = ""
        self._metrics = {
            "search_requests": 0,
            "search_success": 0,
            "search_non_200": 0,
            "detail_api_requests": 0,
            "detail_api_success": 0,
            "detail_page_requests": 0,
            "detail_page_success": 0,
            "comment_requests": 0,
            "comment_success": 0,
            "network_timeout": 0,
            "network_error": 0,
            "candidate_seen": 0,
            "archived_items": 0,
        }

        if self.crawl_debug_log:
            logging.getLogger().setLevel(logging.INFO)
        else:
            logging.getLogger().setLevel(logging.WARNING)

        matcher_init_start = time.perf_counter()
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
        matcher_init_elapsed = time.perf_counter() - matcher_init_start
        if self.init_profile_log:
            logging.warning(
                f"[InitProfile] TextMatcher init took {matcher_init_elapsed:.3f}s | backend={self.ac_backend}"
            )
        self.score_filter_enabled = bool(self.matcher.score_enabled)
        self.score_filter_threshold = int(self.matcher.score_threshold)
        self.score_parallel_batch_size = max(int(getattr(self.matcher, "score_parallel_batch_size", 8) or 8), 1)
        if not self.score_filter_enabled:
            self.score_heap_selection_enabled = False
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
            self._timing_live_line_len = 0
            self._activity_line_len = 0
            self._live_line_len = 0
            self._progress_last_signature = ""

    def _log_warning(self, message: str):
        self._ensure_progress_newline()
        logging.warning(message)

    def _log_error(self, message: str):
        self._ensure_progress_newline()
        logging.error(message)

    def _metric_inc(self, key: str, amount: int = 1):
        if key not in self._metrics:
            return
        self._metrics[key] = int(self._metrics.get(key, 0) or 0) + int(amount)

    def _build_request_proxies(self) -> dict:
        if self.proxy_rotation_enabled and self.proxy_pool:
            proxy_url = self.proxy_pool[self._proxy_cursor % len(self.proxy_pool)]
            self._proxy_cursor += 1
            return {"http": proxy_url, "https": proxy_url}

        if self.http_proxy or self.https_proxy:
            return {
                "http": self.http_proxy or self.https_proxy,
                "https": self.https_proxy or self.http_proxy,
            }

        if self.proxy_pool:
            proxy_url = self.proxy_pool[0]
            return {"http": proxy_url, "https": proxy_url}

        return {}

    def _request(self, method: str, url: str, **kwargs):
        request_kwargs = dict(kwargs)
        if "proxies" not in request_kwargs:
            proxies = self._build_request_proxies()
            if proxies:
                request_kwargs["proxies"] = proxies
        return self.session.request(method=method, url=url, **request_kwargs)

    def get_metrics_snapshot(self) -> dict:
        snapshot = dict(self._metrics)
        search_requests_raw = int(snapshot.get("search_requests", 0) or 0)
        search_requests = max(search_requests_raw, 1)
        detail_attempts_raw = int(snapshot.get("detail_api_requests", 0) or 0) + int(
            snapshot.get("detail_page_requests", 0) or 0
        )
        detail_attempts = max(detail_attempts_raw, 1)
        comment_requests_raw = int(snapshot.get("comment_requests", 0) or 0)
        comment_requests = max(comment_requests_raw, 1)
        candidate_seen = int(snapshot.get("candidate_seen", 0) or 0)
        archived_items = int(snapshot.get("archived_items", 0) or 0)

        detail_api_requests_raw = int(snapshot.get("detail_api_requests", 0) or 0)
        detail_api_requests = max(detail_api_requests_raw, 1)
        detail_page_requests_raw = int(snapshot.get("detail_page_requests", 0) or 0)
        detail_page_requests = max(detail_page_requests_raw, 1)

        search_elapsed = max(float(self._timing_totals.get("search_api", 0.0) or 0.0), 1e-9)
        detail_elapsed = max(float(self._timing_totals.get("detail_fetch", 0.0) or 0.0), 1e-9)
        comment_elapsed = max(float(self._timing_totals.get("comment_fetch", 0.0) or 0.0), 1e-9)

        crawl_elapsed = 0.0
        if self._timing_live_started_at > 0:
            crawl_elapsed = max(time.perf_counter() - self._timing_live_started_at, 0.0)

        snapshot["search_success_rate"] = round(
            (int(snapshot.get("search_success", 0) or 0) / float(search_requests)) * 100.0,
            2,
        )
        snapshot["detail_success_rate"] = round(
            (
                int(snapshot.get("detail_api_success", 0) or 0)
                + int(snapshot.get("detail_page_success", 0) or 0)
            )
            / float(detail_attempts)
            * 100.0,
            2,
        )
        snapshot["comment_success_rate"] = round(
            (int(snapshot.get("comment_success", 0) or 0) / float(comment_requests)) * 100.0,
            2,
        )
        snapshot["detail_api_hit_rate"] = round(
            (int(snapshot.get("detail_api_success", 0) or 0) / float(detail_api_requests)) * 100.0,
            2,
        )
        snapshot["detail_page_hit_rate"] = round(
            (int(snapshot.get("detail_page_success", 0) or 0) / float(detail_page_requests)) * 100.0,
            2,
        )
        snapshot["search_qps"] = round(search_requests_raw / search_elapsed, 2)
        snapshot["detail_qps"] = round(detail_attempts_raw / detail_elapsed, 2)
        snapshot["comment_qps"] = round(comment_requests_raw / comment_elapsed, 2)
        snapshot["archive_yield_rate"] = round((archived_items / float(max(candidate_seen, 1))) * 100.0, 2)
        snapshot["crawl_elapsed_sec"] = round(crawl_elapsed, 3)
        snapshot["timing_totals"] = dict(self._timing_totals)
        snapshot["timing_counts"] = dict(self._timing_counts)
        return snapshot

    def _render_live_line(self, display: str, finalize: bool = False):
        pad_len = max(self._live_line_len - len(display), 0)
        print(f"\r{display}{' ' * pad_len}", end="", flush=True)
        if finalize:
            print()
            self._progress_active = False
            self._live_line_len = 0
            self._progress_last_signature = ""
            return

        self._live_line_len = len(display)
        self._progress_active = True

    def _record_timing(self, stage: str, elapsed: float):
        if stage not in self._timing_totals:
            return
        try:
            delta = float(elapsed)
        except Exception:
            return
        if delta < 0:
            return

        self._timing_totals[stage] += delta
        self._timing_counts[stage] = self._timing_counts.get(stage, 0) + 1
        if self.timing_profile_log:
            self._render_timing_live()

    def _snapshot_timing(self) -> tuple[dict, dict]:
        return dict(self._timing_totals), dict(self._timing_counts)

    def _diff_timing(self, before_totals: dict, before_counts: dict) -> tuple[dict, dict]:
        stages = set(self._timing_totals.keys()) | set(before_totals.keys())
        totals = {}
        counts = {}
        for stage in stages:
            totals[stage] = max(self._timing_totals.get(stage, 0.0) - before_totals.get(stage, 0.0), 0.0)
            counts[stage] = max(self._timing_counts.get(stage, 0) - before_counts.get(stage, 0), 0)
        return totals, counts

    def _print_timing_summary(self, label: str, totals: dict, counts: dict, total_elapsed: float | None = None):
        if not self.timing_profile_log:
            return

        entries = []
        for stage, value in totals.items():
            if value <= 0:
                continue
            entries.append((stage, float(value), int(counts.get(stage, 0) or 0)))

        if not entries and total_elapsed is None:
            return

        entries.sort(key=lambda item: item[1], reverse=True)
        entries = entries[: self.timing_profile_topn]

        parts = []
        if total_elapsed is not None:
            parts.append(f"total={total_elapsed:.3f}s")
        for stage, value, call_count in entries:
            if call_count > 0:
                avg = value / float(call_count)
                parts.append(f"{stage}={value:.3f}s(avg {avg:.3f}s x{call_count})")
            else:
                parts.append(f"{stage}={value:.3f}s")

        self._ensure_progress_newline()
        print(f"[TimeProfile] {label}: " + " | ".join(parts))

    def _render_timing_live(self, label: str | None = None, force: bool = False):
        if (not self.timing_profile_log) or (not self.timing_profile_live_log):
            return
        if self.progress_prefer_status and self.show_progress_bar:
            if self.search_page_progress_log or self.keyword_status_log or self.activity_status_enabled:
                return
        if self.crawl_debug_log:
            return
        if not getattr(sys.stdout, "isatty", lambda: False)():
            return

        if label is not None:
            self._timing_live_label = str(label or "").strip()

        if not self._timing_live_label:
            return

        now = time.perf_counter()
        if (not force) and (now - self._timing_live_last_ts < self.timing_profile_live_interval_sec):
            return

        self._timing_live_last_ts = now
        elapsed = 0.0
        if self._timing_live_started_at > 0:
            elapsed = max(now - self._timing_live_started_at, 0.0)

        entries = []
        for stage, value in self._timing_totals.items():
            if value > 0:
                entries.append((stage, float(value)))
        entries.sort(key=lambda item: item[1], reverse=True)
        entries = entries[: self.timing_profile_live_topn]

        parts = [f"total={elapsed:.1f}s"]
        if entries:
            parts.extend([f"{stage}:{value:.1f}s" for stage, value in entries])

        display = f"[TimeProfile-Live][{self._timing_live_label}] " + " | ".join(parts)
        self._timing_live_line_len = len(display)
        self._render_live_line(display)

    def _log_network_exception(self, stage: str, identifier: str, exc: Exception):
        if isinstance(exc, (Timeout, TimeoutError)):
            self._metric_inc("network_timeout")
            msg = f"{stage} timeout, skipped: {identifier}"
            if self.crawl_debug_log:
                msg += f" ({exc})"
            self._log_warning(msg)
            return

        self._metric_inc("network_error")
        msg = f"{stage} request failed: {identifier}"
        if self.crawl_debug_log:
            msg += f" ({exc})"
        self._log_error(msg)

    def _render_progress(self, current: int, total: int, prefix: str, force: bool = False):
        if total <= 0:
            return

        safe_total = max(int(total), 1)
        safe_current = min(max(int(current), 0), safe_total)
        now = time.perf_counter()
        signature = f"{prefix}|{safe_current}|{safe_total}"
        is_complete = safe_current >= safe_total

        if not force:
            if signature == self._progress_last_signature:
                return
            if (not is_complete) and (now - self._progress_last_render_ts < self.progress_render_interval_sec):
                return

        self._progress_last_render_ts = now
        self._progress_last_signature = signature

        width = 30
        ratio = min(max(safe_current / safe_total, 0), 1)
        filled = int(width * ratio)
        bar = "#" * filled + "-" * (width - filled)
        percent = int(ratio * 100)
        display = f"{prefix}: [{bar}] {safe_current}/{safe_total} ({percent:3d}%)"
        self._render_live_line(display, finalize=is_complete)

    def _render_keyword_status(self, keyword: str, candidates: int, archived: int):
        self._render_live_line(f"[{keyword}] candidates processed: {candidates}, archived: {archived}")

    def _finish_search_progress(self, keyword: str, scanned_pages: int, page_limit: int, stop_reason: str):
        if (not self.show_progress_bar) or self.crawl_debug_log:
            return

        safe_scanned = max(int(scanned_pages or 0), 0)
        safe_limit = max(int(page_limit or 0), 1)
        if self.search_page_progress_log:
            final_scanned = max(safe_scanned, 1)
            self._render_progress(final_scanned, safe_limit, f"[search][{keyword}]")

        if self.search_page_stop_reason_log:
            self._ensure_progress_newline()
            print(
                f"[search][{keyword}] stop_reason={stop_reason} | scanned={safe_scanned}/{safe_limit}"
            )

    def _render_activity_status(self, message: str, force: bool = False):
        if not self.show_progress_bar:
            return
        if not self.activity_status_enabled:
            return
        if self.crawl_debug_log:
            return
        if not getattr(sys.stdout, "isatty", lambda: False)():
            return

        now = time.perf_counter()
        if (not force) and (now - self._activity_last_ts < self.activity_status_interval_sec):
            return
        self._activity_last_ts = now

        frame = self._activity_frames[self._activity_frame_index % len(self._activity_frames)]
        self._activity_frame_index += 1

        display = f"{message} {frame}"
        self._activity_line_len = len(display)
        self._render_live_line(display)

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
        if not self.fill_valid_quota:
            return self.max_pages

        if self.max_items <= 0:
            return self.max_pages

        adaptive = max(self.max_pages, self.max_items * 3, self.max_pages * 4)
        return min(adaptive, self.max_pages_hard_limit)

    def _keyword_stage_target(self, heap_mode: bool, heap_limit: int) -> int:
        if self.max_items <= 0 or self.fill_valid_quota:
            return 0
        if heap_mode:
            return max(int(heap_limit or 0), self.max_items)
        return self.max_items

    def _is_valid_content(self, title, content_text=""):
        if self.filter_rules.get("must_contain"):
            return self.matcher.match(title, content_text)
        return True

    def _record_score_filtered_post(self, post: dict, score_report: dict):
        self.score_filtered_count += 1

        if not self.score_filtered_posts_log:
            return

        limit = max(self.score_filtered_posts_log_limit, 0)
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
                "raw_content_chars": breakdown.get("raw_content_chars", breakdown.get("content_chars", 0)),
                "alg_score": breakdown.get("alg_score", 0),
                "alg_hot_score": breakdown.get("alg_hot_score", 0),
                "alg_problem_hits": breakdown.get("alg_problem_hits", []),
                "alg_topic_hits": breakdown.get("alg_topic_hits", []),
                "alg_problem_id_hits": breakdown.get("alg_problem_id_hits", []),
                "alg_hot_title_hits": breakdown.get("alg_hot_title_hits", []),
                "alg_hot_id_hits": breakdown.get("alg_hot_id_hits", []),
                "alg_hot_matches": breakdown.get("alg_hot_matches", []),
                "length_score": breakdown.get("length_score", 0),
                "legacy_length_score": breakdown.get("legacy_length_score", breakdown.get("length_score", 0)),
                "match_score": breakdown.get("match_score", 0),
                "length_ratio": breakdown.get("length_ratio", 0),
                "legacy_length_ratio": breakdown.get("legacy_length_ratio", breakdown.get("length_ratio", 0)),
                "match_ratio": breakdown.get("match_ratio", 0),
                "structured_question_count": breakdown.get("structured_question_count", 0),
                "structured_questions": breakdown.get("structured_questions", []),
                "structured_question_missing_penalty": breakdown.get("structured_question_missing_penalty", 0),
                "structured_question_noise_penalty": breakdown.get("structured_question_noise_penalty", 0),
                "promo_blocked": breakdown.get("promo_blocked", False),
                "promo_hits": breakdown.get("promo_hits", []),
                "promo_penalty": breakdown.get("promo_penalty", 0),
                "numbered_ad_blocked": breakdown.get("numbered_ad_blocked", False),
                "numbered_ad_penalty": breakdown.get("numbered_ad_penalty", 0),
                "numbered_ad_segments": breakdown.get("numbered_ad_segments", 0),
                "numbered_ad_suspicious_segments": breakdown.get("numbered_ad_suspicious_segments", 0),
                "numbered_ad_suspicious_ratio": breakdown.get("numbered_ad_suspicious_ratio", 0),
                "numbered_ad_marker_hit_count": breakdown.get("numbered_ad_marker_hit_count", 0),
                "numbered_ad_marker_hits": breakdown.get("numbered_ad_marker_hits", []),
                "tail_tags": breakdown.get("tail_tags", []),
            }
        )

    def iter_search_posts(self, keyword):
        url = "https://gw-c.nowcoder.com/api/sparta/pc/search"
        seen_posts = set()
        page_limit = self._effective_page_limit()
        page_scheduler = SearchPageScheduler(
            page_limit=page_limit,
            strategy=self.search_page_strategy,
            best_first_frontload=self.best_first_frontload_pages,
            best_first_explore_stride=self.best_first_explore_stride,
        )
        empty_page_streak = 0
        scanned_pages = 0
        stop_reason = "running"
        external_reason = ""

        try:
            for page in page_scheduler.iter_pages():
                scanned_pages += 1
                page_label = f"{scanned_pages}/{page_limit}"
                if self.search_page_strategy != "bfs":
                    page_label += f" (real:{page})"
                self._render_activity_status(f"[search][{keyword}] page {page_label}", force=True)
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
                    search_api_start = time.perf_counter()
                    self._metric_inc("search_requests")
                    resp = self._request("POST", url, json=payload, timeout=self.search_timeout)
                    self._record_timing("search_api", time.perf_counter() - search_api_start)
                    if resp.status_code == 200:
                        self._metric_inc("search_success")
                        data = resp.json()
                        records = data.get("data", {}).get("records", [])
                        if not records:
                            empty_page_streak += 1
                            if self.search_page_progress_log and self.show_progress_bar and not self.crawl_debug_log:
                                self._render_progress(scanned_pages, page_limit, f"[search][{keyword}]")
                            if scanned_pages <= self.max_pages:
                                stop_reason = "结果页为空(提前结束)"
                                break
                            if empty_page_streak >= 2:
                                stop_reason = f"连续{empty_page_streak}页无结果"
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
                                self._metric_inc("candidate_seen")

                                yield {
                                    "id": post_id,
                                    "uuid": post_uuid,
                                    "detail_api_type": detail_api_type,
                                    "title": title,
                                    "keyword": keyword,
                                    "search_content": raw_content,
                                }

                        if matched_in_page == 0 and scanned_pages > self.max_pages:
                            empty_page_streak += 1
                            if empty_page_streak >= 2:
                                stop_reason = "连续页仅噪声/无可归档候选"
                                break
                    else:
                        self._metric_inc("search_non_200")
                        if scanned_pages > self.max_pages:
                            empty_page_streak += 1
                            if empty_page_streak >= 2:
                                stop_reason = f"连续{empty_page_streak}页请求非200"
                                break

                    if self.search_page_progress_log and self.show_progress_bar and not self.crawl_debug_log:
                        self._render_progress(scanned_pages, page_limit, f"[search][{keyword}]")

                    time.sleep(random.uniform(1.5, 3.5))
                except Exception as e:
                    self._log_network_exception("search", f"{keyword} page {page}", e)

            external_reason = str(self._search_external_stop_reason.pop(keyword, "") or "").strip()
            if stop_reason == "running":
                if external_reason:
                    stop_reason = external_reason
                else:
                    stop_reason = "达到搜索页上限"
        finally:
            if not external_reason:
                external_reason = str(self._search_external_stop_reason.pop(keyword, "") or "").strip()
            else:
                self._search_external_stop_reason.pop(keyword, None)

            if stop_reason == "running":
                if external_reason:
                    stop_reason = external_reason
                else:
                    stop_reason = "外部提前停止"
            self._search_page_stats[keyword] = {
                "scanned_pages": scanned_pages,
                "page_limit": page_limit,
                "stop_reason": stop_reason,
            }
            self._finish_search_progress(keyword, scanned_pages, page_limit, stop_reason)

    def search_posts(self, keyword):
        posts = []
        for item in self.iter_search_posts(keyword):
            posts.append(item)
            if self.max_items > 0 and (not self.fill_valid_quota) and len(posts) >= self.max_items:
                self._search_external_stop_reason[keyword] = f"达到候选数上限 {self.max_items}"
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
                self._metric_inc("detail_api_requests")
                api_resp = self._request("GET", api_url, headers=api_headers, timeout=self.detail_api_timeout)
                if api_resp.status_code != 200:
                    continue

                res_json = api_resp.json()
                data = res_json.get("data")
                if isinstance(data, dict) and data.get("content"):
                    self._metric_inc("detail_api_success")
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

    def _use_heap_selection_mode(self) -> bool:
        return self.max_items > 0 and self.score_heap_selection_enabled and self.score_filter_enabled

    def _heap_pool_size(self) -> int:
        if self.max_items <= 0:
            return 0
        return max(int(math.ceil(self.max_items * self.score_heap_pool_multiplier)), self.max_items)

    def _heap_pool_max_size(self, base_limit: int) -> int:
        if self.max_items <= 0:
            return base_limit

        desired = max(int(math.ceil(self.max_items * self.score_heap_elastic_max_multiplier)), base_limit)
        if self.score_heap_max_pool > 0:
            desired = max(base_limit, min(desired, self.score_heap_max_pool))
        return desired

    def _heap_resize_step(self) -> int:
        if self.max_items <= 0:
            return 1
        return max(int(math.ceil(self.max_items * self.score_heap_elastic_step_ratio)), 1)

    @staticmethod
    def _heap_sort_key(item: dict) -> tuple:
        score = int(item.get("quality_score", 0) or 0)
        content_chars = int(item.get("quality_score_breakdown", {}).get("content_chars", 0) or 0)
        return score, content_chars

    def _push_heap_item(self, heap_items: list, item: dict, heap_limit: int) -> str:
        if heap_limit <= 0:
            return "skip"

        score_key = self._heap_sort_key(item)
        unique_key = str(item.get("id", item.get("uuid", "")) or "")
        payload = (score_key[0], score_key[1], unique_key, item)

        if len(heap_items) < heap_limit:
            heapq.heappush(heap_items, payload)
            return "push"

        if payload > heap_items[0]:
            heapq.heapreplace(heap_items, payload)
            return "replace"

        return "skip"

    def _resize_heap_limit(
        self,
        heap_items: list,
        current_limit: int,
        base_limit: int,
        max_limit: int,
        replace_count_in_round: int,
        idle_rounds: int,
    ) -> tuple[int, int]:
        if not self.score_heap_elastic_enabled or current_limit <= 0:
            return current_limit, idle_rounds

        next_limit = current_limit
        next_idle_rounds = idle_rounds
        step = self._heap_resize_step()

        should_expand = (
            replace_count_in_round >= self.score_heap_elastic_replace_threshold
            and current_limit < max_limit
        )

        if should_expand:
            next_limit = min(current_limit + step, max_limit)
            next_idle_rounds = 0
            if self.crawl_debug_log and next_limit != current_limit:
                logging.info(
                    f"[heap-elastic] expand limit {current_limit} -> {next_limit} | replacements={replace_count_in_round}"
                )
        else:
            next_idle_rounds += 1
            if current_limit > base_limit and next_idle_rounds >= self.score_heap_elastic_shrink_idle_rounds:
                next_limit = max(current_limit - step, base_limit)
                next_idle_rounds = 0
                if self.crawl_debug_log and next_limit != current_limit:
                    logging.info(f"[heap-elastic] shrink limit {current_limit} -> {next_limit}")

        if next_limit < current_limit:
            while len(heap_items) > next_limit:
                heapq.heappop(heap_items)

        return next_limit, next_idle_rounds

    def _finalize_heap_items(self, heap_items: list) -> list:
        if not heap_items:
            return []

        selected = [entry[3] for entry in heap_items]
        selected.sort(
            key=lambda item: (
                int(item.get("quality_score", 0) or 0),
                int(item.get("quality_score_breakdown", {}).get("content_chars", 0) or 0),
                str(item.get("id", item.get("uuid", "")) or ""),
            ),
            reverse=True,
        )
        return selected

    def _archive_limit_reached(self, archived_for_keyword: int, heap_mode: bool = False) -> bool:
        if heap_mode:
            return False
        return self.max_items > 0 and (not self.fill_valid_quota) and archived_for_keyword >= self.max_items

    def _flush_scoring_buffer(
        self,
        scoring_buffer: list,
        archived_for_keyword: int,
        heap_mode: bool = False,
        keyword_heap: list | None = None,
        keyword_heap_limit: int = 0,
    ) -> tuple[int, bool, int, int, list]:
        if not scoring_buffer:
            return archived_for_keyword, False, 0, 0, []

        if self.score_filter_enabled and hasattr(self.matcher, "is_scoring_automaton_building"):
            try:
                if self.matcher.is_scoring_automaton_building() and (not self._score_automaton_building_notified):
                    self._render_activity_status("[score-automaton] 构建中", force=True)
                    self._score_automaton_building_notified = True
            except Exception:
                pass

        self._render_activity_status(f"[score] evaluating {len(scoring_buffer)} posts")

        inputs = [
            {
                "title": item.get("title", ""),
                "content": item.get("content", ""),
                "tags": item.get("tags", []),
            }
            for item in scoring_buffer
        ]
        score_eval_start = time.perf_counter()
        score_reports = self.matcher.evaluate_posts_quality_parallel(inputs)
        self._record_timing("score_eval", time.perf_counter() - score_eval_start)

        if self.score_filter_enabled and hasattr(self.matcher, "is_scoring_automaton_ready"):
            try:
                if self.matcher.is_scoring_automaton_ready() and (not self._score_automaton_ready_notified):
                    self._render_activity_status("[score-automaton] 已就绪", force=True)
                    self._score_automaton_ready_notified = True
            except Exception:
                pass

        reached_limit = False
        replace_count_in_round = 0
        comment_fetch_count = 0
        archived_items = []
        for item, score_report in zip(scoring_buffer, score_reports):
            if score_report.get("enabled"):
                item["quality_score"] = score_report.get("score", 0)
                item["quality_score_breakdown"] = score_report.get("breakdown", {})
                item["algorithm_matches"] = score_report.get("breakdown", {}).get("alg_hot_matches", [])
                item["question_outline"] = score_report.get("breakdown", {}).get("structured_questions", [])
                item["question_outline_count"] = int(
                    score_report.get("breakdown", {}).get("structured_question_count", 0) or 0
                )

                if not score_report.get("passed", True):
                    self._record_score_filtered_post(item, score_report)
                    if self.crawl_debug_log:
                        logging.info(
                            f"[-] score below threshold ({score_report.get('score', 0)}/"
                            f"{score_report.get('threshold', self.score_filter_threshold)}), filtered: {item['title']}"
                        )
                    continue

            if heap_mode:
                if keyword_heap is None:
                    keyword_heap = []
                push_action = self._push_heap_item(keyword_heap, item, keyword_heap_limit)
                if push_action == "replace":
                    replace_count_in_round += 1
                archived_for_keyword = len(keyword_heap)
                continue

            if self.fetch_comments_enabled:
                item["comments"] = self.get_comments(item["id"])
                comment_fetch_count += 1
            else:
                item["comments"] = []
            archived_items.append(item)
            archived_for_keyword += 1
            self._metric_inc("archived_items")
            if self.crawl_debug_log:
                logging.info(f"archived: {item['title']}")

            if self._archive_limit_reached(archived_for_keyword, heap_mode=heap_mode):
                reached_limit = True
                break

        scoring_buffer.clear()
        return archived_for_keyword, reached_limit, replace_count_in_round, comment_fetch_count, archived_items

    def get_post_detail(self, post_id, post_uuid, detail_api_type=None):
        canonical_url = f"https://www.nowcoder.com/feed/main/detail/{post_uuid}"
        if detail_api_type == "content-data" and post_id is not None:
            canonical_url = f"https://www.nowcoder.com/discuss/{post_id}"

        url = canonical_url
        detail_fetch_start = time.perf_counter()
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
            self._metric_inc("detail_page_requests")
            resp = self._request("GET", url, timeout=self.detail_page_timeout)
            if resp.status_code == 200 and not content:
                content = self._extract_content_from_initial_state(resp.text, post_uuid)

                soup = BeautifulSoup(resp.text, "html.parser")
                if not content:
                    content = self._extract_content_from_html(soup)

                if content:
                    content = self._clean_detail_content(content)
                    self._metric_inc("detail_page_success")

                tags = [tag.get_text(strip=True) for tag in soup.find_all("a", class_="discuss-tag-item") if soup]
                return {"content": content, "tags": tags, "url": url}
            elif content:
                return {"content": self._clean_detail_content(content), "tags": [], "url": canonical_url}

            time.sleep(random.uniform(1.0, 2.0))
        except Exception as e:
            self._log_network_exception("detail", f"UUID: {post_uuid}", e)
        finally:
            self._record_timing("detail_fetch", time.perf_counter() - detail_fetch_start)

        return {"content": "", "tags": [], "url": url}

    def get_comments(self, post_id):
        if not self.fetch_comments_enabled:
            return []

        comments_list = []
        url = "https://gw-c.nowcoder.com/api/sparta/reply/v2/reply/list"
        payload = {
            "entityId": post_id,
            "entityType": 8,
            "page": 1,
            "pageSize": 50,
            "order": 1,
        }
        comment_fetch_start = time.perf_counter()
        try:
            self._metric_inc("comment_requests")
            resp = self._request("POST", url, json=payload, timeout=self.comment_timeout)
            if resp.status_code == 200:
                self._metric_inc("comment_success")
                data = resp.json().get("data", {}).get("records", [])
                for c in data:
                    comment_text = c.get("content", "")
                    comments_list.append(comment_text)
                    sub_replies = c.get("replyList", [])
                    for sub in sub_replies:
                        comments_list.append(sub.get("content", ""))
            if self.comment_request_max_interval_sec > 0:
                delay = random.uniform(
                    self.comment_request_min_interval_sec,
                    self.comment_request_max_interval_sec,
                )
                if delay > 0:
                    time.sleep(delay)
        except Exception as e:
            self._log_network_exception("comment", f"ID: {post_id}", e)
        finally:
            self._record_timing("comment_fetch", time.perf_counter() - comment_fetch_start)
        return comments_list

    def crawl(self, on_archived_item=None):
        all_data = []

        def _emit_archived_item(item: dict):
            if callable(on_archived_item):
                on_archived_item(item)
            else:
                all_data.append(item)

        crawl_total_start = time.perf_counter()
        self._timing_live_started_at = crawl_total_start
        self._timing_live_last_ts = 0.0
        self._timing_live_label = ""
        try:
            total_keywords = len(self.keywords)
            for kw_index, kw in enumerate(self.keywords, start=1):
                keyword_total_start = time.perf_counter()
                self._search_external_stop_reason.pop(kw, None)
                self._render_timing_live(label=f"keyword={kw}", force=True)
                if self.timing_profile_keyword_log:
                    keyword_before_totals, keyword_before_counts = self._snapshot_timing()
                else:
                    keyword_before_totals, keyword_before_counts = None, None

                archived_for_keyword = 0
                handled_candidates = 0
                detail_processed = 0
                scored_processed = 0
                comment_processed = 0
                scoring_buffer = []
                heap_mode = self._use_heap_selection_mode()
                keyword_heap = [] if heap_mode else None
                heap_base_limit = self._heap_pool_size() if heap_mode else 0
                keyword_heap_limit = heap_base_limit
                keyword_heap_max_limit = self._heap_pool_max_size(heap_base_limit) if heap_mode else 0
                heap_idle_rounds = 0
                stage_target = self._keyword_stage_target(heap_mode, keyword_heap_limit)
                detail_progress_completed = False
                score_progress_completed = False
                comment_progress_completed = False

                reached_quota = False
                for p in self.iter_search_posts(kw):
                    if self._archive_limit_reached(archived_for_keyword, heap_mode=heap_mode):
                        self._search_external_stop_reason[kw] = f"达到归档目标 {self.max_items}"
                        break

                    self._render_activity_status(
                        f"[pipeline][{kw}] detail={detail_processed + 1}"
                        f"{('/' + str(stage_target)) if stage_target > 0 else ''}"
                        f" | scored={scored_processed} | archived={archived_for_keyword}"
                    )
                    handled_candidates += 1
                    detail = self.get_post_detail(p.get("id"), p.get("uuid"), p.get("detail_api_type"))
                    detail_processed += 1

                    if (
                        self.pipeline_stage_progress_log
                        and self.show_progress_bar
                        and (not self.crawl_debug_log)
                        and stage_target > 0
                    ):
                        if not detail_progress_completed:
                            detail_current = min(detail_processed, stage_target)
                            self._render_progress(detail_current, stage_target, f"[detail][{kw}]")
                            if detail_current >= stage_target:
                                detail_progress_completed = True

                    final_content = detail.get("content")
                    if not final_content:
                        final_content = p.get("search_content", "")

                    if final_content:
                        p.update(detail)
                        p["content"] = final_content
                        scoring_buffer.append(p)

                        if len(scoring_buffer) >= self.score_parallel_batch_size:
                            score_batch_size = len(scoring_buffer)
                            archived_for_keyword, reached_quota, heap_replace_count, comment_batch_count, archived_batch = self._flush_scoring_buffer(
                                scoring_buffer,
                                archived_for_keyword,
                                heap_mode=heap_mode,
                                keyword_heap=keyword_heap,
                                keyword_heap_limit=keyword_heap_limit,
                            )
                            for archived_item in archived_batch:
                                _emit_archived_item(archived_item)
                            scored_processed += score_batch_size
                            comment_processed += comment_batch_count

                            if heap_mode:
                                keyword_heap_limit, heap_idle_rounds = self._resize_heap_limit(
                                    keyword_heap or [],
                                    keyword_heap_limit,
                                    heap_base_limit,
                                    keyword_heap_max_limit,
                                    heap_replace_count,
                                    heap_idle_rounds,
                                )
                                archived_for_keyword = len(keyword_heap or [])
                                old_stage_target = stage_target
                                stage_target = max(stage_target, self._keyword_stage_target(heap_mode, keyword_heap_limit))
                                if stage_target > old_stage_target:
                                    if detail_processed < stage_target:
                                        detail_progress_completed = False
                                    if scored_processed < stage_target:
                                        score_progress_completed = False
                                    if comment_processed < stage_target:
                                        comment_progress_completed = False

                            if (
                                self.pipeline_stage_progress_log
                                and self.show_progress_bar
                                and (not self.crawl_debug_log)
                                and stage_target > 0
                            ):
                                if not score_progress_completed:
                                    score_current = min(scored_processed, stage_target)
                                    self._render_progress(score_current, stage_target, f"[score][{kw}]")
                                    if score_current >= stage_target:
                                        score_progress_completed = True
                                if (not heap_mode) and comment_processed > 0 and (not comment_progress_completed):
                                    comment_current = min(comment_processed, stage_target)
                                    self._render_progress(comment_current, stage_target, f"[comment][{kw}]")
                                    if comment_current >= stage_target:
                                        comment_progress_completed = True

                            if reached_quota:
                                self._search_external_stop_reason[kw] = f"达到归档目标 {self.max_items}"
                                if self.crawl_debug_log:
                                    logging.info(f"keyword '{kw}' reached archive target {self.max_items}, stop early")
                                break

                            if (
                                heap_mode
                                and (not self.fill_valid_quota)
                                and keyword_heap_limit > 0
                                and archived_for_keyword >= keyword_heap_limit
                            ):
                                reached_quota = True
                                self._search_external_stop_reason[kw] = f"达到候选池上限 {keyword_heap_limit}"
                                if self.crawl_debug_log:
                                    logging.info(
                                        f"keyword '{kw}' filled heap pool {keyword_heap_limit}, stop early"
                                    )
                                break
                    else:
                        if self.crawl_debug_log:
                            logging.warning(f"[-] drop post with empty content: {p['title']} [{p['id']}]")

                    if self.keyword_status_log and self.show_progress_bar and not self.crawl_debug_log:
                        self._render_keyword_status(kw, handled_candidates, archived_for_keyword)

                if not reached_quota:
                    if self.show_progress_bar and (not self.crawl_debug_log) and scoring_buffer:
                        self._ensure_progress_newline()
                        print(f"[pipeline][{kw}] search阶段结束，进入评分收尾...")

                    score_batch_size = len(scoring_buffer)
                    archived_for_keyword, _, _, comment_batch_count, archived_batch = self._flush_scoring_buffer(
                        scoring_buffer,
                        archived_for_keyword,
                        heap_mode=heap_mode,
                        keyword_heap=keyword_heap,
                        keyword_heap_limit=keyword_heap_limit,
                    )
                    for archived_item in archived_batch:
                        _emit_archived_item(archived_item)
                    scored_processed += score_batch_size
                    comment_processed += comment_batch_count
                    if (
                        self.pipeline_stage_progress_log
                        and self.show_progress_bar
                        and (not self.crawl_debug_log)
                        and stage_target > 0
                    ):
                        if not score_progress_completed:
                            score_current = min(scored_processed, stage_target)
                            self._render_progress(score_current, stage_target, f"[score][{kw}]")
                            if score_current >= stage_target:
                                score_progress_completed = True
                        if (not heap_mode) and comment_processed > 0 and (not comment_progress_completed):
                            comment_current = min(comment_processed, stage_target)
                            self._render_progress(comment_current, stage_target, f"[comment][{kw}]")
                            if comment_current >= stage_target:
                                comment_progress_completed = True

                if heap_mode:
                    ranked_items = self._finalize_heap_items(keyword_heap or [])
                    comment_total = len(ranked_items) if self.fetch_comments_enabled else 0
                    if self.fetch_comments_enabled and self.show_progress_bar and (not self.crawl_debug_log) and comment_total > 0:
                        self._ensure_progress_newline()
                        print(f"[pipeline][{kw}] 进入评论抓取，共 {comment_total} 篇...")

                    for index, item in enumerate(ranked_items, start=1):
                        if self.fetch_comments_enabled:
                            self._render_activity_status(f"[comment][{kw}] fetching {index}/{comment_total}", force=True)
                            item["comments"] = self.get_comments(item["id"])
                            comment_processed = index
                            if (
                                self.pipeline_stage_progress_log
                                and self.show_progress_bar
                                and (not self.crawl_debug_log)
                                and comment_total > 0
                            ):
                                self._render_progress(comment_processed, comment_total, f"[comment][{kw}]")
                        else:
                            item["comments"] = []
                        _emit_archived_item(item)
                        self._metric_inc("archived_items")
                    archived_for_keyword = len(ranked_items)

                if self.show_progress_bar and (not self.crawl_debug_log):
                    self._ensure_progress_newline()
                    target_text = str(stage_target) if stage_target > 0 else "动态"
                    comments_text = str(comment_processed) if self.fetch_comments_enabled else "disabled"
                    print(
                        f"[pipeline][{kw}] done | detail={detail_processed} | score={scored_processed} "
                        f"| comments={comments_text} | archived={archived_for_keyword} | target={target_text}"
                    )

                self._ensure_progress_newline()
                if self.show_progress_bar and (not self.crawl_debug_log) and total_keywords > 1:
                    self._render_progress(kw_index, total_keywords, "[keywords] overall")

                if self.max_items > 0 and archived_for_keyword < self.max_items:
                    search_stat = self._search_page_stats.get(kw, {})
                    effective_limit = self._effective_page_limit()
                    scan_info = ""
                    if isinstance(search_stat, dict) and search_stat:
                        effective_limit = int(search_stat.get("page_limit", effective_limit) or effective_limit)
                        scan_info = (
                            f" | search_scanned={search_stat.get('scanned_pages', 0)}/"
                            f"{effective_limit}"
                            f" | reason={search_stat.get('stop_reason', '')}"
                        )
                    self._log_warning(
                        f"keyword '{kw}' archived {archived_for_keyword}/{self.max_items}, scanned up to page limit {effective_limit}{scan_info}"
                    )

                if self.timing_profile_keyword_log and keyword_before_totals is not None:
                    keyword_totals, keyword_counts = self._diff_timing(keyword_before_totals, keyword_before_counts)
                    self._print_timing_summary(
                        f"keyword={kw}",
                        keyword_totals,
                        keyword_counts,
                        time.perf_counter() - keyword_total_start,
                    )

            self._render_timing_live(label="crawl_overall", force=True)
            return all_data
        finally:
            if self.timing_profile_log:
                self._print_timing_summary(
                    "crawl_overall",
                    self._timing_totals,
                    self._timing_counts,
                    time.perf_counter() - crawl_total_start,
                )
            try:
                self.matcher.shutdown()
            except Exception:
                pass

    def iter_crawl_items(self, queue_size: int = 64):
        from queue import Queue

        safe_queue_size = max(int(queue_size or 1), 1)
        queue = Queue(maxsize=safe_queue_size)
        sentinel = object()
        worker_errors = []

        def _on_archived_item(item: dict):
            queue.put(item)

        def _worker():
            try:
                self.crawl(on_archived_item=_on_archived_item)
            except Exception as e:
                worker_errors.append(e)
            finally:
                queue.put(sentinel)

        worker = threading.Thread(target=_worker, daemon=True, name="crawl4nk-stream")
        worker.start()

        while True:
            item = queue.get()
            if item is sentinel:
                break
            yield item

        worker.join(timeout=0.2)
        if worker_errors:
            raise worker_errors[0]
