import json
import os
import re
import time
import urllib.parse
from typing import Dict, List

import pandas as pd
import requests


class DataProcessor:
    NOISE_WORDS = {
        "java", "python", "golang", "go", "c++", "cpp", "javascript", "js", "sql",
        "前端", "后端", "算法", "测试", "开发", "研发", "产品", "运营", "设计", "运维",
        "工程师", "面试", "面经", "实习", "校招", "秋招", "春招", "offer", "八股", "笔试",
        "高级", "中级", "初级", "资深", "专家", "总监", "主管", "leader", "岗位", "职位", "方向",
        "总结", "分享", "记录", "经验", "题目", "题解", "教程", "攻略", "agent",
        "大厂", "小厂", "信息", "慎投", "双非"
    }

    NON_COMPANY_FRAGMENTS = {
        "面经", "慎投", "保姆级", "教学", "双非", "纯八股", "八股", "不感兴趣",
        "三月", "四月", "事业部", "合集", "攻略", "经验贴", "问答",
        "大厂", "小厂", "怒砍", "这些公司", "总结....",
        "没有公司", "无公司", "公司名", "公司名称",
        "转码", "非科班", "科班"
    }

    COMPANY_ALIAS_MAP = {
        "字节跳动": "字节",
        "字节跳动tiktok": "字节",
        "tiktok": "字节",
        "飞书": "字节",
        "豆包": "字节",
        "抖音": "字节",
        "今日头条": "字节",
        "番茄小说": "字节",
        "番茄畅听": "字节",
        "西瓜视频": "字节",
        "火山引擎": "字节",
        "巨量引擎": "字节",
        "剪映": "字节",
        "抖音电商": "字节",
        "懂车帝": "字节",
        "荣耀": "字节",
        "阿里巴巴": "阿里",
        "淘天": "阿里",
        "蚂蚁": "阿里",
        "蚂蚁集团": "阿里",
        "蚂蚁金服": "阿里",
        "支付宝": "阿里",
        "菜鸟网络": "阿里",
        "饿了么": "阿里",
        "高德地图": "阿里",
        "qq": "腾讯",
        "微信": "腾讯",
        "wechat": "腾讯",
        "qq音乐": "腾讯",
        "腾讯视频": "腾讯",
        "微信视频号": "腾讯",
        "美团点评": "美团",
        "京东数科": "京东",
        "pdd": "拼多多",
        "shopee": "虾皮",
        "哔哩哔哩": "b站",
        "bilibili": "b站",
        "腾讯teg": "腾讯",
        "腾讯云": "腾讯"
    }

    PRODUCT_ALIAS_TO_COMPANY = {
        "飞书": "字节",
        "豆包": "字节",
        "番茄小说": "字节",
        "番茄畅听": "字节",
        "西瓜视频": "字节",
        "火山引擎": "字节",
        "巨量引擎": "字节",
        "剪映": "字节",
        "今日头条": "字节",
        "懂车帝": "字节",
        "支付宝": "阿里",
        "高德地图": "阿里",
        "饿了么": "阿里",
        "微信": "腾讯",
        "QQ": "腾讯",
        "QQ音乐": "腾讯"
    }

    TITLE_NICKNAME_TO_COMPANY = {
        "鹅厂": "腾讯",
        "tx": "腾讯",
        "ali": "阿里",
        "猪厂": "网易",
        "拼夕夕": "拼多多",
        "bili": "b站",
        "菊厂": "华为",
    }

    OWNER_ALIAS_TO_COMPANY_DEFAULT = {
        "懂车帝": "字节",
        "荣耀": "字节",
        "番茄小说": "字节",
        "番茄畅听": "字节",
        "西瓜视频": "字节",
        "火山引擎": "字节",
        "巨量引擎": "字节",
        "剪映": "字节",
        "今日头条": "字节",
        "抖音电商": "字节",
        "高德地图": "阿里",
        "饿了么": "阿里",
        "菜鸟网络": "阿里",
        "QQ音乐": "腾讯",
        "腾讯视频": "腾讯",
        "微信视频号": "腾讯",
    }

    OWNER_RELATION_HINTS = (
        "是哪个公司的",
        "属于",
        "旗下",
        "母公司",
        "控股",
        "集团",
    )

    GENERIC_NON_COMPANY_PATTERNS = (
        r"(没有|无|不含|缺少).{0,4}(公司|厂|企业)",
        r"(公司|企业).{0,4}(没有|无|不含|缺少)",
        r"(公司名|公司名称|公司简称)",
    )

    def __init__(self, data: List[Dict], config: Dict = None):
        self.data = data
        self.config = config or {}
        self.config_path = str(
            self.config.get("companies_path", "data/companies/companies.json")
            or "data/companies/companies.json"
        )
        self.company_debug_log = bool(self.config.get("company_debug_log", False))
        self.show_progress_bar = bool(self.config.get("show_progress_bar", True))
        self.export_progress_log = bool(self.config.get("export_progress_log", True))
        self.include_id_column = bool(self.config.get("include_id_column", False))
        self.include_algorithm_annotations = bool(self.config.get("include_algorithm_annotations", True))
        self.include_question_outline = bool(self.config.get("include_question_outline", False))
        self.fetch_comments_enabled = bool(self.config.get("fetch_comments_enabled", False))
        self.include_comments_column = bool(self.config.get("include_comments_column", self.fetch_comments_enabled))
        self.company_must_in_list = bool(self.config.get("company_must_in_list", True))
        self.drop_unknown_company_posts = bool(self.config.get("drop_unknown_company_posts", True))
        self.require_company_in_title = bool(self.config.get("require_company_in_title", True))
        self.company_owner_inference_enabled = bool(self.config.get("company_owner_inference_enabled", True))
        self.company_owner_query_timeout = float(self.config.get("company_owner_query_timeout", 6) or 6)
        self.company_owner_max_suggestions = max(int(self.config.get("company_owner_max_suggestions", 12) or 12), 5)
        self.company_cache_mode = str(self.config.get("company_cache_mode", "online") or "online").strip().lower()
        if self.company_cache_mode not in {"online", "reuse_then_refresh"}:
            self.company_cache_mode = "online"
        self.company_runtime_cache_enabled = bool(self.config.get("company_runtime_cache_enabled", True))
        runtime_cache_path = str(
            self.config.get("company_runtime_cache_path", "ENV/.cache/company_resolver_cache.json")
            or "ENV/.cache/company_resolver_cache.json"
        ).strip()
        if runtime_cache_path and not os.path.isabs(runtime_cache_path):
            runtime_cache_path = os.path.join(os.getcwd(), runtime_cache_path)
        self.company_runtime_cache_path = runtime_cache_path
        self.company_cache_refresh_after_run = bool(self.config.get("company_cache_refresh_after_run", True))
        self.company_cache_refresh_max_tokens = max(
            int(self.config.get("company_cache_refresh_max_tokens", 80) or 80),
            0,
        )
        self.company_runtime_cache_max_entries = max(
            int(self.config.get("company_runtime_cache_max_entries", 20000) or 20000),
            500,
        )
        legacy_filtered_posts_log = bool(self.config.get("filtered_posts_log", False))
        legacy_filtered_posts_log_limit = int(self.config.get("filtered_posts_log_limit", 50) or 50)
        self.unknown_company_filtered_posts_log = bool(
            self.config.get("unknown_company_filtered_posts_log", legacy_filtered_posts_log)
        )
        self.unknown_company_filtered_posts_log_limit = int(
            self.config.get("unknown_company_filtered_posts_log_limit", legacy_filtered_posts_log_limit)
            or legacy_filtered_posts_log_limit
        )
        self.max_items_per_keyword = int(self.config.get("max_items_per_keyword", 0) or 0)
        self.dropped_unknown_company_count = 0
        self.dropped_unknown_company_examples = []

        self.companies = self._load_companies()
        self.owner_alias_to_company = self._load_owner_aliases()
        self.dynamic_verify_skip_tokens = self._build_dynamic_verify_skip_tokens()

        self.company_cache = {}
        self.owner_company_cache = {}
        self.baidu_suggestion_cache = {}
        self._runtime_cache_dirty = False
        self._pending_company_verify_tokens = set()
        self._pending_owner_infer_tokens = set()
        self._load_runtime_cache()

    def _mark_runtime_cache_dirty(self):
        if self.company_runtime_cache_enabled:
            self._runtime_cache_dirty = True

    def _cache_put(self, bucket: Dict, key: str, value):
        if bucket.get(key) != value:
            bucket[key] = value
            self._mark_runtime_cache_dirty()

    def _truncate_cache_mapping(self, mapping: Dict, limit: int):
        if not isinstance(mapping, dict) or limit <= 0 or len(mapping) <= limit:
            return
        retained = list(mapping.items())[-limit:]
        mapping.clear()
        mapping.update(retained)

    def _load_runtime_cache(self):
        if not self.company_runtime_cache_enabled or not self.company_runtime_cache_path:
            return
        if not os.path.exists(self.company_runtime_cache_path):
            return

        try:
            with open(self.company_runtime_cache_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception as e:
            self._company_log(f"[Cache] 读取公司缓存失败，将忽略旧缓存: {e}")
            return

        if not isinstance(payload, dict):
            return

        raw_company_cache = payload.get("company_cache", {})
        if isinstance(raw_company_cache, dict):
            for raw_key, raw_value in raw_company_cache.items():
                key = str(raw_key or "").strip()
                if not key:
                    continue
                self.company_cache[key] = bool(raw_value)

        raw_owner_cache = payload.get("owner_company_cache", {})
        if isinstance(raw_owner_cache, dict):
            for raw_key, raw_value in raw_owner_cache.items():
                key = str(raw_key or "").strip()
                if not key:
                    continue
                value = str(raw_value or "").strip()
                self.owner_company_cache[key] = value

        raw_baidu_cache = payload.get("baidu_suggestion_cache", {})
        if isinstance(raw_baidu_cache, dict):
            for raw_key, raw_value in raw_baidu_cache.items():
                key = str(raw_key or "").strip()
                if not key:
                    continue

                suggestions = []
                if isinstance(raw_value, list):
                    for item in raw_value[:20]:
                        token = str(item or "").strip()
                        if token:
                            suggestions.append(token)
                self.baidu_suggestion_cache[key] = suggestions

        self._truncate_cache_mapping(self.company_cache, self.company_runtime_cache_max_entries)
        self._truncate_cache_mapping(self.owner_company_cache, self.company_runtime_cache_max_entries)
        self._truncate_cache_mapping(self.baidu_suggestion_cache, self.company_runtime_cache_max_entries)

        self._runtime_cache_dirty = False
        self._company_log(
            "[Cache] 公司缓存加载完成: "
            f"company={len(self.company_cache)} owner={len(self.owner_company_cache)} sugg={len(self.baidu_suggestion_cache)}"
        )

    def _save_runtime_cache(self):
        if not self.company_runtime_cache_enabled or not self.company_runtime_cache_path:
            return
        if not self._runtime_cache_dirty:
            return

        self._truncate_cache_mapping(self.company_cache, self.company_runtime_cache_max_entries)
        self._truncate_cache_mapping(self.owner_company_cache, self.company_runtime_cache_max_entries)
        self._truncate_cache_mapping(self.baidu_suggestion_cache, self.company_runtime_cache_max_entries)

        payload = {
            "version": "v1",
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "company_cache": self.company_cache,
            "owner_company_cache": self.owner_company_cache,
            "baidu_suggestion_cache": self.baidu_suggestion_cache,
        }

        try:
            dir_path = os.path.dirname(self.company_runtime_cache_path)
            if dir_path:
                os.makedirs(dir_path, exist_ok=True)
            with open(self.company_runtime_cache_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            self._runtime_cache_dirty = False
            self._company_log(
                "[Cache] 公司缓存已写入: "
                f"{self.company_runtime_cache_path} "
                f"(company={len(self.company_cache)}, owner={len(self.owner_company_cache)}, sugg={len(self.baidu_suggestion_cache)})"
            )
        except Exception as e:
            self._company_log(f"[Cache] 写入公司缓存失败: {e}")

    def _refresh_runtime_cache_after_run(self):
        if not self.company_runtime_cache_enabled:
            return
        if self.company_cache_mode != "reuse_then_refresh":
            return
        if not self.company_cache_refresh_after_run:
            return

        pending_tokens = []
        pending_seen = set()
        for token in list(self._pending_company_verify_tokens) + list(self._pending_owner_infer_tokens):
            key = str(token or "").strip()
            if not key or key in pending_seen:
                continue
            pending_seen.add(key)
            pending_tokens.append(key)

        if not pending_tokens:
            return

        if self.company_cache_refresh_max_tokens > 0:
            pending_tokens = pending_tokens[:self.company_cache_refresh_max_tokens]
        if not pending_tokens:
            return

        start = time.perf_counter()
        refreshed_count = 0
        owner_infer_hits = 0

        self._company_log(f"[Cache] 末尾增量刷新开始，待刷新词数: {len(pending_tokens)}")
        for token in pending_tokens:
            refreshed_count += 1
            is_company = self._verify_company_online(token, force_online=True)
            if is_company:
                if token not in self.companies:
                    self.companies.add(token)
                    self._save_new_company_to_config(token)
                continue

            inferred_owner = self._infer_owner_company_online(token, force_online=True)
            if inferred_owner:
                owner_infer_hits += 1

        elapsed = time.perf_counter() - start
        self._pending_company_verify_tokens.clear()
        self._pending_owner_infer_tokens.clear()
        self._company_log(
            f"[Cache] 末尾增量刷新完成: 刷新={refreshed_count}, 归属命中={owner_infer_hits}, 耗时={elapsed:.2f}s"
        )

    def _load_companies(self) -> set:
        companies = set()

        if not os.path.exists(self.config_path):
            print(f"[警告] 未找到 {self.config_path} 文件，公司匹配库目前为空！")
            return companies

        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                config_data = json.load(f)
        except Exception as e:
            print(f"[错误] 读取 {self.config_path} 失败: {e}")
            return companies

        if not isinstance(config_data, list):
            return companies

        for company_name in config_data:
            if self._is_plausible_company_name(company_name):
                companies.add(company_name)
            else:
                self._company_log(f"[Debug] 跳过可疑公司词: '{company_name}'")

        return companies

    def _load_owner_aliases(self) -> Dict[str, str]:
        merged = dict(self.OWNER_ALIAS_TO_COMPANY_DEFAULT)

        configured = self.config.get("company_owner_aliases", {})
        if isinstance(configured, dict):
            for raw_alias, raw_company in configured.items():
                alias = str(raw_alias or "").strip()
                company = str(raw_company or "").strip()
                if not alias or not company:
                    continue
                merged[alias] = company

        normalized = {}
        for alias, company in merged.items():
            alias_token = str(alias or "").strip()
            company_token = self._normalize_company_name(str(company or "").strip())
            if alias_token and company_token:
                normalized[alias_token] = company_token

        return normalized

    def _build_dynamic_verify_skip_tokens(self) -> set:
        tokens = set()
        for alias_map in (
            self.PRODUCT_ALIAS_TO_COMPANY,
            self.TITLE_NICKNAME_TO_COMPANY,
            self.owner_alias_to_company,
            self.COMPANY_ALIAS_MAP,
        ):
            for alias in alias_map.keys():
                alias_token = str(alias or "").strip().lower()
                if alias_token:
                    tokens.add(alias_token)
        return tokens

    def _company_log(self, message: str):
        if self.company_debug_log:
            print(message)

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

    def _save_new_company_to_config(self, new_company: str):
        if not self._is_plausible_company_name(new_company):
            self._company_log(f"[Debug] 跳过可疑公司词，不落盘: '{new_company}'")
            return

        try:
            config_data = []
            if os.path.exists(self.config_path):
                with open(self.config_path, "r", encoding="utf-8") as f:
                    config_data = json.load(f)

            if new_company not in config_data:
                config_data.append(new_company)
                dir_path = os.path.dirname(self.config_path)
                if dir_path:
                    os.makedirs(dir_path, exist_ok=True)
                with open(self.config_path, "w", encoding="utf-8") as f:
                    json.dump(config_data, f, ensure_ascii=False, indent=4)
                self._company_log(f"[*] 持久化: 已将新公司 '{new_company}' 保存至 {self.config_path}。")
        except Exception as e:
            self._company_log(f"[!] 保存新公司至配置失败: {e}")

    def _is_noise_token(self, word: str) -> bool:
        if not word:
            return True
        lower_word = word.lower()
        return lower_word in self.NOISE_WORDS

    def _is_generic_non_company_phrase(self, word: str) -> bool:
        token = str(word or "").strip().lower()
        if not token:
            return True

        for pattern in self.GENERIC_NON_COMPANY_PATTERNS:
            if re.search(pattern, token):
                return True

        return False

    def _is_plausible_company_name(self, name: str) -> bool:
        word = (name or "").strip()
        if not word:
            return False
        if len(word) < 2 or len(word) > 25:
            return False
        if word.isdigit() or self._is_noise_token(word):
            return False
        if self._is_generic_non_company_phrase(word):
            return False

        if any(fragment in word for fragment in self.NON_COMPANY_FRAGMENTS):
            return False

        if re.search(r"[?？!！]", word):
            return False

        if re.search(r"[.。…]{2,}", word):
            return False

        if re.fullmatch(r"[\u4e00-\u9fff]{9,}", word) and not any(
            marker in word
            for marker in (
                "科技",
                "集团",
                "有限公司",
                "控股",
                "金融",
                "网络",
                "信息",
                "软件",
                "云计算",
                "研究院",
                "研究所",
                "中心",
                "银行",
                "证券",
                "电信",
                "移动",
                "联通",
            )
        ):
            return False

        return True

    def _normalize_company_name(self, name: str) -> str:
        if not name:
            return name

        return self.COMPANY_ALIAS_MAP.get(name.lower(), name)

    def _title_token_position(self, title: str, token: str) -> int:
        if not title or not token:
            return -1

        if re.search(r"[a-zA-Z0-9]", token):
            pattern = rf"(?<![a-zA-Z0-9]){re.escape(token)}(?![a-zA-Z0-9])"
            match = re.search(pattern, title, flags=re.IGNORECASE)
            return match.start() if match else -1

        return title.lower().find(token.lower())

    def _title_contains_token(self, title: str, token: str) -> bool:
        return self._title_token_position(title, token) >= 0

    def _score_title_candidate(self, title: str, token: str, source: str) -> int:
        position = self._title_token_position(title, token)
        if position < 0:
            return -1

        source_weight = {
            "product_alias": 130,
            "owner_alias": 126,
            "nickname_alias": 120,
            "alias_map": 110,
            "known_company": 100,
            "dynamic_verified": 95,
            "owner_inferred": 92,
            "title_pattern": 70
        }.get(source, 60)

        score = source_weight

        if position == 0:
            score += 40
        elif position <= 2:
            score += 30
        elif position <= 6:
            score += 20
        else:
            score += max(5, 15 - min(position, 15))

        score += min(len(token), 12) * 3

        sep_pattern = rf"(^|[-\[\]【】|/，：_\s]){re.escape(token)}($|[-\[\]【】|/，：_\s])"
        if re.search(sep_pattern, title, flags=re.IGNORECASE):
            score += 20

        return score

    def _looks_like_company_token(self, word: str) -> bool:
        if not word:
            return False

        word = word.strip()
        if len(word) < 2 or len(word) > 20:
            return False
        if word.isdigit() or self._is_noise_token(word):
            return False

        if re.fullmatch(r"[a-zA-Z][a-zA-Z0-9&._-]{2,20}", word):
            return True

        if re.search(r"[\u4e00-\u9fff]", word):
            return re.fullmatch(r"[\u4e00-\u9fffA-Za-z0-9&._-]+", word) is not None and self._is_plausible_company_name(word)

        return False

    def _is_company_by_suggestions(self, word: str, suggestions: List[str]) -> bool:
        if not suggestions:
            return False

        word_lower = word.lower()
        strong_company_markers = ("有限公司", "股份有限公司", "集团", "控股", "公司")
        weak_company_markers = ("企业", "科技", "信息", "网络", "软件", "电子", "银行", "证券", "保险", "资本", "物流", "医药", "药业", "投资")
        non_company_markers = ("教程", "题", "是什么意思", "怎么", "区别", "学习", "岗位", "工资", "简历")
        major_brand_tokens = ("阿里巴巴", "腾讯", "百度", "字节跳动", "美团", "京东", "网易", "快手", "拼多多", "华为", "小米", "滴滴")

        normalized = [((s or "").strip()) for s in suggestions[:10]]

        for s in normalized:
            s = (s or "").strip()
            if not s:
                continue
            if word_lower in s.lower() and any(marker in s for marker in strong_company_markers):
                return True

        mentions = 0
        prefix_hits = 0
        strong_hits = 0
        weak_hits = 0
        negative_hits = 0

        for s in normalized:
            if not s:
                continue
            s_lower = s.lower()

            if word_lower in s_lower:
                mentions += 1
            if s_lower.startswith(word_lower):
                prefix_hits += 1
            if any(marker in s for marker in strong_company_markers):
                strong_hits += 1
            if any(marker in s for marker in weak_company_markers):
                weak_hits += 1
            if any(marker in s for marker in non_company_markers):
                negative_hits += 1

        if re.fullmatch(r"[a-zA-Z][a-zA-Z0-9&._-]{1,20}", word):
            brand_context_hits = sum(1 for s in normalized if word_lower in s.lower() and ("官网" in s or "登录" in s or "收款" in s or "企业" in s))
            return mentions >= 2 and (prefix_hits >= 1 or brand_context_hits >= 1)

        if re.fullmatch(r"[\u4e00-\u9fff]{2,3}", word):
            brand_hint = any(word in s and any(token in s for token in major_brand_tokens) for s in normalized)
            return strong_hits >= 1 or brand_hint

        score = mentions + prefix_hits + (weak_hits * 2) + (strong_hits * 3) - negative_hits
        return mentions >= 2 and score >= 6

    def _query_baidu_suggestions(self, keyword: str) -> List[str]:
        token = str(keyword or "").strip()
        if not token:
            return []

        if token in self.baidu_suggestion_cache:
            return self.baidu_suggestion_cache[token]

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        }
        suggestions: List[str] = []

        try:
            sugg_url = f"https://www.baidu.com/sugrec?prod=pc&wd={urllib.parse.quote(token)}"
            sugg_resp = requests.get(sugg_url, headers=headers, timeout=self.company_owner_query_timeout)
            if sugg_resp.status_code == 200:
                data = sugg_resp.json()
                suggestions = [item.get("q", "") for item in data.get("g", []) if isinstance(item, dict)]
        except Exception as e:
            self._company_log(f"[Debug] 请求建议词失败: {e}")

        normalized = []
        for item in suggestions[:20]:
            suggestion = str(item or "").strip()
            if suggestion:
                normalized.append(suggestion)

        self._cache_put(self.baidu_suggestion_cache, token, normalized)
        return normalized

    def _infer_owner_company_from_suggestions(self, word: str, suggestions: List[str]) -> str:
        if not suggestions:
            return ""

        alias_to_company = {}
        for comp in self.companies:
            alias_to_company[comp] = comp
        alias_to_company.update(self.COMPANY_ALIAS_MAP)
        alias_to_company.update(self.PRODUCT_ALIAS_TO_COMPANY)
        alias_to_company.update(self.TITLE_NICKNAME_TO_COMPANY)
        alias_to_company.update(self.owner_alias_to_company)

        company_scores = {}
        for suggestion in suggestions[: self.company_owner_max_suggestions]:
            text = str(suggestion or "").strip()
            if not text:
                continue

            relation_bonus = 2 if any(hint in text for hint in self.OWNER_RELATION_HINTS) else 0
            word_bonus = 1 if self._title_contains_token(text, word) else 0

            for alias, company in alias_to_company.items():
                alias_token = str(alias or "").strip()
                company_token = self._normalize_company_name(str(company or "").strip())
                if not alias_token or not company_token:
                    continue
                if not self._title_contains_token(text, alias_token):
                    continue

                gain = 2 + relation_bonus + word_bonus
                company_scores[company_token] = company_scores.get(company_token, 0) + gain

        if not company_scores:
            return ""

        best_company, best_score = max(company_scores.items(), key=lambda item: item[1])
        if best_score < 4:
            return ""

        return best_company

    def _infer_owner_company_online(self, word: str, force_online: bool = False) -> str:
        if not self.company_owner_inference_enabled:
            return ""

        token = str(word or "").strip()
        if len(token) < 2:
            return ""

        if token in self.owner_company_cache:
            return self.owner_company_cache[token]

        for alias, company in self.owner_alias_to_company.items():
            if self._title_contains_token(token, alias):
                resolved = self._normalize_company_name(company)
                self._cache_put(self.owner_company_cache, token, resolved)
                return resolved

        if self.company_cache_mode == "reuse_then_refresh" and (not force_online):
            self._pending_owner_infer_tokens.add(token)
            return ""

        suggestions = self._query_baidu_suggestions(token)
        inferred = self._infer_owner_company_from_suggestions(token, suggestions)

        if not inferred:
            owner_query = f"{token}是哪个公司的"
            owner_suggestions = self._query_baidu_suggestions(owner_query)
            inferred = self._infer_owner_company_from_suggestions(token, owner_suggestions)

        self._cache_put(self.owner_company_cache, token, inferred or "")
        return inferred or ""

    def _verify_company_online(self, word: str, force_online: bool = False) -> bool:
        word = (word or "").strip()
        if len(word) < 2 or len(word) > 20:
            return False
        if not self._is_plausible_company_name(word):
            return False
        if self._is_noise_token(word):
            return False
        if word in self.company_cache:
            return self.company_cache[word]

        if self.company_cache_mode == "reuse_then_refresh" and (not force_online):
            self._pending_company_verify_tokens.add(word)
            return False

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9",
        }

        try:
            time.sleep(0.25)
            aiqicha_url = f"https://aiqicha.baidu.com/s?q={urllib.parse.quote(word)}&t=0"
            resp = requests.get(aiqicha_url, headers=headers, timeout=self.company_owner_query_timeout)

            if resp.status_code == 200:
                text = resp.text
                shell_page = ('"queryWord":""' in text and "window.pageData" in text and "company-list" not in text)

                if not shell_page:
                    page_hit = word in text and any(k in text for k in ("公司", "企业", "有限公司", "股份", "集团"))
                    if page_hit:
                        self._cache_put(self.company_cache, word, True)
                        self._company_log(f"[-] 动态检测到新公司(爱企查直链): '{word}'")
                        return True
                else:
                    self._company_log(f"[Debug] 爱企查返回壳页，切换建议词校验: '{word}'")
        except Exception as e:
            self._company_log(f"[Debug] 请求爱企查校验失败: {e}")

        suggestions = self._query_baidu_suggestions(word)
        if suggestions:
            is_company = self._is_company_by_suggestions(word, suggestions)
            self._cache_put(self.company_cache, word, is_company)
            if is_company:
                self._company_log(f"[-] 动态检测到新公司(建议词): '{word}'")
            return is_company

        self._cache_put(self.company_cache, word, False)
        return False

    def _extract_company(self, title: str, content: str):
        title = (title or "").strip()
        if not title:
            self._company_log("[DataProcessor] 提取到的公司名称为: '其他'")
            return "其他"

        candidates = {}

        def add_candidate(company_name: str, matched_token: str, source: str):
            company_name = self._normalize_company_name((company_name or "").strip())
            matched_token = (matched_token or "").strip()
            if not company_name or not matched_token:
                return

            if self.company_must_in_list and company_name not in self.companies:
                return

            score = self._score_title_candidate(title, matched_token, source)
            if score < 0:
                return

            position = self._title_token_position(title, matched_token)
            old = candidates.get(company_name)
            current = {
                "score": score,
                "token": matched_token,
                "source": source,
                "position": position
            }

            if not old:
                candidates[company_name] = current
                return

            if (
                current["score"] > old["score"]
                or (current["score"] == old["score"] and len(current["token"]) > len(old["token"]))
                or (
                    current["score"] == old["score"]
                    and len(current["token"]) == len(old["token"])
                    and current["position"] < old["position"]
                )
            ):
                candidates[company_name] = current

        for alias, company_name in self.PRODUCT_ALIAS_TO_COMPANY.items():
            if self._title_contains_token(title, alias):
                add_candidate(company_name, alias, "product_alias")

        for alias, company_name in self.TITLE_NICKNAME_TO_COMPANY.items():
            if self._title_contains_token(title, alias):
                add_candidate(company_name, alias, "nickname_alias")

        for alias, company_name in self.owner_alias_to_company.items():
            if self._title_contains_token(title, alias):
                add_candidate(company_name, alias, "owner_alias")

        for alias, company_name in self.COMPANY_ALIAS_MAP.items():
            if self._title_contains_token(title, alias):
                add_candidate(company_name, alias, "alias_map")

        for comp in self.companies:
            if not self._is_plausible_company_name(comp):
                continue
            if self._title_contains_token(title, comp):
                add_candidate(comp, comp, "known_company")

        title_segments = re.split(r'[-\[\]【】|/，：_+\s]+', title)
        for seg in title_segments:
            seg = seg.strip()
            if not seg:
                continue

            seg = re.sub(r'^(\d{3,4}|\d+\.\d+|\d+届)', '', seg)
            seg = re.sub(r'(前端|后端|测试|运维|算法|数据|产品|运营|设计|产品经理|运营专员|开发|研发|全栈|客户端|服务端|架构|大模型|ai应用|ai|java|c\+\+|cpp|python|golang|go|工程师|规划师|实习生|全职|端|岗)', ' ', seg, flags=re.IGNORECASE)
            seg = re.sub(r'(一二三面|初面|终面|一面|二面|三面|四面|hr面|主管面|总监面|交叉面|面试|面经|面|笔试|机试|测评)', ' ', seg, flags=re.IGNORECASE)
            seg = re.sub(r'(求职|招聘|内推|实习|校招|社招|日常|暑期|秋招|春招|提前批|分享|记录|攒人品|已挂|意向|offer|挂了|挂|上岸|白菜|sp|ssp|依然)', ' ', seg, flags=re.IGNORECASE)

            for p_clean in seg.split():
                p_clean = p_clean.strip()
                p_clean = re.sub(r'^[+“”，。、？！：；]+|[+“”，。、？！：；]+$', '', p_clean)
                if not self._looks_like_company_token(p_clean):
                    continue

                if p_clean.lower() in self.dynamic_verify_skip_tokens:
                    continue

                normalized = self._normalize_company_name(p_clean)
                existing = candidates.get(normalized)
                if existing and existing.get("source") in ("product_alias", "owner_alias", "known_company"):
                    continue

                if any(
                    existing_company != normalized and self._title_contains_token(p_clean, existing_company)
                    for existing_company in candidates.keys()
                ):
                    continue

                if normalized.lower() != p_clean.lower():
                    add_candidate(normalized, p_clean, "alias_map")
                    continue

                if self.company_must_in_list:
                    if normalized in self.companies:
                        add_candidate(normalized, p_clean, "known_company")
                    else:
                        self._company_log(
                            f"[Debug] 严格白名单模式，跳过非 companies 公司词: '{p_clean}'"
                        )
                    continue

                self._company_log(f"[Debug] 开始在线校验可能的新公司: '{p_clean}' (来自标题: '{title}')")
                if self._verify_company_online(p_clean):
                    if p_clean not in self.companies:
                        self.companies.add(p_clean)
                        self._save_new_company_to_config(p_clean)
                    add_candidate(normalized, p_clean, "dynamic_verified")
                else:
                    inferred_owner = self._infer_owner_company_online(p_clean)
                    if inferred_owner:
                        add_candidate(inferred_owner, p_clean, "owner_inferred")
                        self._company_log(f"[-] 归属推断命中: '{p_clean}' -> '{inferred_owner}'")
                    else:
                        self._company_log(f"[Debug] 校验失败，该词被认为不是公司名: '{p_clean}'")

        if not candidates:
            self._company_log("[DataProcessor] 提取到的公司名称为: '其他'")
            return "其他"

        best_company, _ = max(
            candidates.items(),
            key=lambda item: (item[1]["score"], len(item[1]["token"]), -item[1]["position"])
        )

        if self.company_must_in_list and best_company not in self.companies:
            self._company_log(
                f"[Debug] 严格白名单模式，最终候选 '{best_company}' 不在 companies 中，回退为其他"
            )
            return "其他"

        self._company_log(f"[DataProcessor] 提取到的公司名称为: '{best_company}'")
        return best_company

    def _format_algorithm_annotations(self, matches) -> str:
        if not isinstance(matches, list) or not matches:
            return ""

        lines = []
        for item in matches[:20]:
            if not isinstance(item, dict):
                continue

            title = str(item.get("title", "") or "").strip()
            if not title:
                continue

            problem_id = item.get("id")
            frontend_id = str(item.get("frontend_id", "") or "").strip()
            if problem_id is not None:
                try:
                    prefix = str(int(problem_id))
                except Exception:
                    prefix = str(problem_id)
            elif frontend_id:
                prefix = frontend_id
            else:
                prefix = ""

            title_part = f"{prefix}. {title}" if prefix else title

            meta_parts = []
            frequency = item.get("frequency")
            if frequency is not None:
                try:
                    freq_num = int(frequency)
                except Exception:
                    freq_num = None
                if freq_num is not None and freq_num > 0:
                    meta_parts.append(f"频度:{freq_num}")

            difficulty = str(item.get("difficulty", "") or "").strip()
            if difficulty:
                meta_parts.append(f"难度:{difficulty}")

            line = title_part
            if meta_parts:
                line += " | " + " | ".join(meta_parts)

            url = str(item.get("url", item.get("link", "")) or "").strip()
            if url:
                line += f" | 链接:{url}"

            lines.append(line)

        return "\n".join(lines)

    def _build_expected_columns(self) -> List[str]:
        columns = ["标题", "公司", "搜索关键词", "帖子链接", "正文"]
        if self.include_comments_column:
            columns.append("评论及回复")
        if self.include_question_outline:
            columns.append("面试题清单")
        if self.include_algorithm_annotations:
            columns.append("算法标注")
        if self.include_id_column:
            columns = ["ID"] + columns
        return columns

    def _format_question_outline(self, items) -> str:
        if not isinstance(items, list) or not items:
            return ""

        normalized_lines = []
        seen = set()
        for item in items[:30]:
            line = str(item or "").strip()
            if not line:
                continue
            line = re.sub(r"\s+", " ", line)
            if line in seen:
                continue
            seen.add(line)
            normalized_lines.append(line)

        return "\n".join(normalized_lines)

    def _sanitize_html_text(self, text: str) -> str:
        cleaned = re.sub(r"<br\s*/?>|</p>|</div>", "\n", str(text or ""), flags=re.IGNORECASE)
        cleaned = re.sub(r"<[^>]+>", "", cleaned).strip()
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return cleaned

    def _build_clean_record(self, item: Dict, title: str, company: str, keyword: str, content: str, comments: str) -> Dict:
        record = {
            "标题": title,
            "公司": company,
            "搜索关键词": keyword,
            "帖子链接": item.get("url", ""),
            "正文": content,
        }

        if self.include_comments_column:
            record["评论及回复"] = comments

        if self.include_question_outline:
            record["面试题清单"] = self._format_question_outline(item.get("question_outline", []))

        if self.include_algorithm_annotations:
            record["算法标注"] = self._format_algorithm_annotations(item.get("algorithm_matches", []))

        if self.include_id_column:
            record = {"ID": item.get("id"), **record}

        return record

    def _render_clean_progress(self, current: int, total: int):
        if self.show_progress_bar and not self.company_debug_log:
            self._render_progress(current, total, "清洗打标进度")

    def _render_export_progress(self, current: int, total: int, prefix: str):
        if self.show_progress_bar and self.export_progress_log and not self.company_debug_log:
            self._render_progress(current, total, prefix)

    def _record_dropped_unknown_company(self, keyword: str, title: str, url: str):
        unknown_example = {
            "title": title,
            "keyword": keyword,
            "url": url,
        }
        self.dropped_unknown_company_count += 1
        self.dropped_unknown_company_examples.append(unknown_example)

    def process(self):
        cleaned_data = []
        self.dropped_unknown_company_count = 0
        self.dropped_unknown_company_examples = []
        kept_keyword_counter = {}
        expected_columns = self._build_expected_columns()

        try:
            total_items = len(self.data)
            for idx, item in enumerate(self.data, start=1):
                keyword = item.get("keyword", "")

                if self.max_items_per_keyword > 0 and keyword:
                    if kept_keyword_counter.get(keyword, 0) >= self.max_items_per_keyword:
                        self._render_clean_progress(idx, total_items)
                        continue

                title = item.get("title", "")
                content = self._sanitize_html_text(item.get("content", ""))
                comments = ""
                if self.include_comments_column:
                    comments = self._sanitize_html_text("\n".join(item.get("comments", [])))

                company = self._extract_company(title, content)
                record = self._build_clean_record(item, title, company, keyword, content, comments)

                should_drop_unknown = self.drop_unknown_company_posts or self.require_company_in_title
                if should_drop_unknown and company == "其他":
                    self._record_dropped_unknown_company(
                        keyword=keyword,
                        title=title,
                        url=item.get("url", ""),
                    )
                    self._render_clean_progress(idx, total_items)
                    continue

                cleaned_data.append(record)
                if keyword:
                    kept_keyword_counter[keyword] = kept_keyword_counter.get(keyword, 0) + 1

                self._render_clean_progress(idx, total_items)

            df = pd.DataFrame(cleaned_data, columns=expected_columns)
            return df
        finally:
            self._refresh_runtime_cache_after_run()
            self._save_runtime_cache()

    def _sanitize_markdown_text(self, text: str) -> str:
        if text is None:
            return ""

        lines = str(text).splitlines()
        cleaned_lines = []
        for line in lines:
            current = line.rstrip()
            current = current.replace("\t", "    ")
            current = re.sub(r'^\s{0,3}#{1,6}\s*', '', current)

            if re.fullmatch(r'\s*[-*_]{3,}\s*', current):
                continue

            current = re.sub(r'^\s*>\s*', '', current)
            current = re.sub(r'^\s*[-*+]\s+', '', current)
            current = re.sub(r'^\s*\d+\.\s+', '', current)
            current = current.replace("```", "'''")

            cleaned_lines.append(current)

        result = "\n".join(cleaned_lines)
        result = re.sub(r'\n{3,}', '\n\n', result).strip()
        return result

    def _sanitize_outline_text(self, text: str) -> str:
        if text is None:
            return ""

        lines = str(text).splitlines()
        cleaned_lines = []
        for line in lines:
            current = line.rstrip().replace("\t", "    ")
            current = re.sub(r'^\s{0,3}#{1,6}\s*', '', current)
            current = current.replace("```", "'''")
            if current.strip():
                cleaned_lines.append(current)

        result = "\n".join(cleaned_lines)
        result = re.sub(r'\n{3,}', '\n\n', result).strip()
        return result

    def _merge_body_with_outline(self, content: str, outline: str) -> str:
        body = str(content or "").strip()
        clean_outline = str(outline or "").strip()
        if not clean_outline:
            return body

        if not body:
            return clean_outline

        first_outline_line = clean_outline.splitlines()[0].strip() if clean_outline.splitlines() else ""
        if first_outline_line and first_outline_line in body:
            return body

        return f"{clean_outline}\n\n{body}".strip()

    def save_to_excel(self, df: pd.DataFrame, filename="nowcoder_data.xlsx"):
        try:
            with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='面经汇总')
                workbook = writer.book
                worksheet = writer.sheets['面经汇总']
                
                wrap_format = workbook.add_format({'text_wrap': True, 'valign': 'top'})
                top_format = workbook.add_format({'valign': 'top'})
                title_format = workbook.add_format({'text_wrap': True, 'valign': 'top'})
                link_format = workbook.add_format({'font_color': 'blue', 'underline': 1, 'valign': 'top'})
                header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1, 'valign': 'top'})
                
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)

                if '标题' in df.columns:
                    title_col_idx = df.columns.get_loc('标题')
                    total_rows = len(df)
                    for row_num, title_value in enumerate(df['标题'].tolist(), start=1):
                        worksheet.write(row_num, title_col_idx, title_value, title_format)
                        self._render_export_progress(row_num, total_rows, "Excel写入进度")

                if '帖子链接' in df.columns:
                    link_col_idx = df.columns.get_loc('帖子链接')
                    for row_num, link_value in enumerate(df['帖子链接'].tolist(), start=1):
                        cell_value = "" if pd.isna(link_value) else str(link_value)
                        if cell_value and re.match(r'^https?://', cell_value, flags=re.IGNORECASE):
                            worksheet.write_url(row_num, link_col_idx, cell_value, link_format, string=cell_value)
                        else:
                            worksheet.write(row_num, link_col_idx, cell_value, top_format)

                from xlsxwriter.utility import xl_col_to_name

                column_widths = {
                    'ID': 15,
                    '标题': 30,
                    '公司': 10,
                    '搜索关键词': 15,
                    '帖子链接': 45,
                    '正文': 80,
                    '评论及回复': 80,
                    '面试题清单': 80,
                    '算法标注': 90,
                }

                for col_idx, col_name in enumerate(df.columns):
                    col_letter = xl_col_to_name(col_idx)
                    col_range = f"{col_letter}:{col_letter}"
                    width = column_widths.get(col_name, 20)

                    if col_name == '标题':
                        worksheet.set_column(col_range, width, title_format)
                    elif col_name in ('正文', '评论及回复', '面试题清单', '算法标注'):
                        worksheet.set_column(col_range, width, wrap_format)
                    else:
                        worksheet.set_column(col_range, width, top_format)

            print(f"数据已成功排版并存入全局单个文件: '{filename}'")
                
        except Exception as e:
            print(f"保存至 Excel 失败: {e}")

    def save_to_markdown(self, df: pd.DataFrame, filename="nowcoder_data.md"):
        if df.empty:
            print("没有可保存的数据！")
            return
            
        try:
            import datetime
            with open(filename, 'w', encoding='utf-8') as f:
                f.write("# 牛客网面经汇总\n\n")
                f.write(f"> 自动抓取时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}，共计 {len(df)} 篇面经。\n\n")
                f.write("---\n\n")

                total_rows = len(df)
                for row_index, (_, row) in enumerate(df.iterrows(), start=1):
                    title = row.get('标题', '无标题')
                    company = row.get('公司', '未分类')
                    url = row.get('帖子链接', '无链接')
                    content = self._sanitize_markdown_text(row.get('正文', '无内容'))
                    algorithm_notes = self._sanitize_markdown_text(row.get('算法标注', ''))

                    f.write(f"## {title}\n\n")
                    f.write(f"- **公司/标签**: `{company}`\n")
                    f.write(f"- **链接**: {url}\n\n")
                    f.write(f"### 正文\n\n{content}\n\n")
                    if algorithm_notes and str(algorithm_notes).strip():
                        f.write(f"### 算法题特别标注\n\n{algorithm_notes}\n\n")
                    f.write("---\n\n")
                    self._render_export_progress(row_index, total_rows, "Markdown写入进度")
                    
            print(f"数据已成功排版并存入 Markdown 文件: '{filename}'")
            
        except Exception as e:
            print(f"保存至 Markdown 失败: {e}")

    def save_to_txt(self, df: pd.DataFrame, filename="nowcoder_data.txt"):
        if df.empty:
            print("没有可保存的数据！")
            return

        try:
            with open(filename, 'w', encoding='utf-8') as f:
                total_rows = len(df)
                for row_index, (_, row) in enumerate(df.iterrows(), start=1):
                    title = row.get('标题', '无标题')
                    company = row.get('公司', '未分类')
                    url = row.get('帖子链接', '无链接')
                    content = row.get('正文', '无内容')
                    algorithm_notes = row.get('算法标注', '')

                    f.write(f"【标题】 {title}\n")
                    f.write(f"【公司】 {company}\n")
                    f.write(f"【链接】 {url}\n")
                    f.write(f"【正文】\n{str(content or '')}\n")
                    if algorithm_notes and str(algorithm_notes).strip():
                        f.write(f"\n【算法题特别标注】\n{algorithm_notes}\n")
                    f.write("\n" + "="*80 + "\n\n")
                    self._render_export_progress(row_index, total_rows, "TXT写入进度")

            print(f"数据已成功排版并存入纯文本 TXT 文件: '{filename}'")

        except Exception as e:
            print(f"保存至 TXT 失败: {e}")

    def display_stats(self, df: pd.DataFrame):
        print("\n--- 爬取数据总况 ---")
        print(f"总计爬取帖子数量: {len(df)}")

        should_drop_unknown = self.drop_unknown_company_posts or self.require_company_in_title
        if should_drop_unknown and self.dropped_unknown_company_count > 0:
            print(f"已过滤未识别公司帖子: {self.dropped_unknown_company_count} 篇")

            if self.unknown_company_filtered_posts_log:
                limit = max(self.unknown_company_filtered_posts_log_limit, 0)
                examples = self.dropped_unknown_company_examples[:limit] if limit > 0 else self.dropped_unknown_company_examples
                print(f"\n过滤帖子明细(展示 {len(examples)}/{self.dropped_unknown_company_count}):")
                for i, item in enumerate(examples, start=1):
                    print(f"  [{i}] 标题: {item.get('title', '')}")
                    if item.get('keyword'):
                        print(f"      关键词: {item.get('keyword')}")
                    if item.get('url'):
                        print(f"      链接: {item.get('url')}")

        if df.empty or '公司' not in df.columns:
            print("\n提及公司统计: 暂无可用数据")
            print("\n--------------------\n")
            return

        print("\n提及公司统计:")
        categories_split = df['公司'].str.split(', ').explode()
        counts = categories_split.value_counts()
        for comp, count in counts.items():
            if str(comp).strip():
                print(f"  - {comp}: {count} 篇")
        print("\n--------------------\n")