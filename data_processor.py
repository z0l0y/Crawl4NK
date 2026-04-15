import re
import pandas as pd
from typing import List, Dict
import requests
import time
import urllib.parse

class DataProcessor:
    def __init__(self, data: List[Dict], config: Dict = None):
        self.data = data
        self.config = config or {}
        self.config_path = "companies.json"
        self.company_debug_log = bool(self.config.get("company_debug_log", False))
        self.show_progress_bar = bool(self.config.get("show_progress_bar", True))
        self.include_id_column = bool(self.config.get("include_id_column", False))
        self.drop_unknown_company_posts = bool(self.config.get("drop_unknown_company_posts", True))
        self.filtered_posts_log = bool(self.config.get("filtered_posts_log", False))
        self.filtered_posts_log_limit = int(self.config.get("filtered_posts_log_limit", 50) or 50)
        self.max_items_per_keyword = int(self.config.get("max_items_per_keyword", 0) or 0)
        self.dropped_unknown_company_count = 0
        self.dropped_unknown_company_examples = []
        
        self.companies = set()
        try:
            import json, os
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                    if isinstance(config_data, list):
                        for company_name in config_data:
                            if self._is_plausible_company_name(company_name):
                                self.companies.add(company_name)
                            else:
                                self._company_log(f"[Debug] 跳过可疑公司词: '{company_name}'")
            else:
                print(f"[警告] 未找到 {self.config_path} 文件，公司匹配库目前为空！")
        except Exception as e:
            print(f"[错误] 读取 {self.config_path} 失败: {e}")

        self.company_cache = {}

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
            import json, os
            config_data = []
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
            
            if new_company not in config_data:
                config_data.append(new_company)
                with open(self.config_path, 'w', encoding='utf-8') as f:
                    json.dump(config_data, f, ensure_ascii=False, indent=4)
                self._company_log(f"[*] 持久化: 已将新公司 '{new_company}' 保存至 {self.config_path}。")
        except Exception as e:
            self._company_log(f"[!] 保存新公司至配置失败: {e}")

    def _is_noise_token(self, word: str) -> bool:
        if not word:
            return True
        lower_word = word.lower()
        noise_words = {
            "java", "python", "golang", "go", "c++", "cpp", "javascript", "js", "sql",
            "前端", "后端", "算法", "测试", "开发", "研发", "产品", "运营", "设计", "运维",
            "工程师", "面试", "面经", "实习", "校招", "秋招", "春招", "offer", "八股", "笔试",
            "高级", "中级", "初级", "资深", "专家", "总监", "主管", "leader", "岗位", "职位", "方向",
            "总结", "分享", "记录", "经验", "题目", "题解", "教程", "攻略", "agent",
            "大厂", "小厂", "信息", "慎投", "双非"
        }
        return lower_word in noise_words

    def _is_plausible_company_name(self, name: str) -> bool:
        word = (name or "").strip()
        if not word:
            return False
        if len(word) < 2 or len(word) > 25:
            return False
        if word.isdigit() or self._is_noise_token(word):
            return False

        non_company_fragments = {
            "面经", "慎投", "保姆级", "教学", "双非", "纯八股", "八股", "不感兴趣",
            "三月", "四月", "事业部", "合集", "攻略", "经验贴", "问答",
            "大厂", "小厂", "怒砍", "这些公司", "总结...."
        }
        if any(fragment in word for fragment in non_company_fragments):
            return False

        if re.search(r"[?？!！]", word):
            return False

        if re.search(r"[.。…]{2,}", word):
            return False

        if re.fullmatch(r"[\u4e00-\u9fff]{9,}", word) and not any(
            marker in word for marker in ("科技", "集团", "有限公司", "控股", "金融", "网络", "信息", "软件", "云计算")
        ):
            return False

        return True

    def _normalize_company_name(self, name: str) -> str:
        if not name:
            return name

        alias_map = {
            "字节跳动": "字节",
            "字节跳动tiktok": "字节",
            "tiktok": "字节",
            "飞书": "字节",
            "豆包": "字节",
            "抖音": "字节",
            "阿里巴巴": "阿里",
            "淘天": "阿里",
            "蚂蚁": "阿里",
            "支付宝": "阿里",
            "qq": "腾讯",
            "微信": "腾讯",
            "wechat": "腾讯",
            "pdd": "拼多多",
            "shopee": "虾皮",
            "哔哩哔哩": "b站",
            "bilibili": "b站",
            "腾讯teg": "腾讯",
            "腾讯云": "腾讯"
        }

        return alias_map.get(name.lower(), name)

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
            "known_company": 100,
            "dynamic_verified": 95,
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

    def _verify_company_online(self, word: str) -> bool:
        word = (word or "").strip()
        if len(word) < 2 or len(word) > 20:
            return False
        if not self._is_plausible_company_name(word):
            return False
        if self._is_noise_token(word):
            return False
        if word in self.company_cache:
            return self.company_cache[word]

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9",
        }

        try:
            time.sleep(0.25)
            aiqicha_url = f"https://aiqicha.baidu.com/s?q={urllib.parse.quote(word)}&t=0"
            resp = requests.get(aiqicha_url, headers=headers, timeout=6)

            if resp.status_code == 200:
                text = resp.text
                shell_page = ('"queryWord":""' in text and "window.pageData" in text and "company-list" not in text)

                if not shell_page:
                    page_hit = word in text and any(k in text for k in ("公司", "企业", "有限公司", "股份", "集团"))
                    if page_hit:
                        self.company_cache[word] = True
                        self._company_log(f"[-] 动态检测到新公司(爱企查直链): '{word}'")
                        return True
                else:
                    self._company_log(f"[Debug] 爱企查返回壳页，切换建议词校验: '{word}'")
        except Exception as e:
            self._company_log(f"[Debug] 请求爱企查校验失败: {e}")

        try:
            sugg_url = f"https://www.baidu.com/sugrec?prod=pc&wd={urllib.parse.quote(word)}"
            sugg_resp = requests.get(sugg_url, headers={"User-Agent": headers["User-Agent"]}, timeout=6)
            if sugg_resp.status_code == 200:
                data = sugg_resp.json()
                suggestions = [item.get("q", "") for item in data.get("g", []) if isinstance(item, dict)]
                is_company = self._is_company_by_suggestions(word, suggestions)
                self.company_cache[word] = is_company
                if is_company:
                    self._company_log(f"[-] 动态检测到新公司(建议词): '{word}'")
                return is_company
        except Exception as e:
            self._company_log(f"[Debug] 请求建议词校验公司失败: {e}")

        self.company_cache[word] = False
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

        product_alias_to_company = {
            "飞书": "字节",
            "豆包": "字节",
            "支付宝": "阿里",
            "微信": "腾讯",
            "QQ": "腾讯"
        }
        for alias, company_name in product_alias_to_company.items():
            if self._title_contains_token(title, alias):
                add_candidate(company_name, alias, "product_alias")

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

                normalized = self._normalize_company_name(p_clean)
                existing = candidates.get(normalized)
                if existing and existing.get("source") in ("product_alias", "known_company"):
                    continue

                if normalized.lower() != p_clean.lower():
                    continue

                self._company_log(f"[Debug] 开始在线校验可能的新公司: '{p_clean}' (来自标题: '{title}')")
                if self._verify_company_online(p_clean):
                    if p_clean not in self.companies:
                        self.companies.add(p_clean)
                        self._save_new_company_to_config(p_clean)
                    add_candidate(normalized, p_clean, "dynamic_verified")
                else:
                    self._company_log(f"[Debug] 校验失败，该词被认为不是公司名: '{p_clean}'")

        if not candidates:
            self._company_log("[DataProcessor] 提取到的公司名称为: '其他'")
            return "其他"

        best_company, _ = max(
            candidates.items(),
            key=lambda item: (item[1]["score"], len(item[1]["token"]), -item[1]["position"])
        )

        self._company_log(f"[DataProcessor] 提取到的公司名称为: '{best_company}'")
        return best_company

    def process(self):
        cleaned_data = []
        self.dropped_unknown_company_count = 0
        self.dropped_unknown_company_examples = []
        kept_keyword_counter = {}
        expected_columns = ["标题", "公司", "搜索关键词", "帖子链接", "正文", "评论及回复"]
        if self.include_id_column:
            expected_columns = ["ID"] + expected_columns

        total_items = len(self.data)
        for idx, item in enumerate(self.data, start=1):
            keyword = item.get("keyword", "")
            if self.max_items_per_keyword > 0 and keyword:
                if kept_keyword_counter.get(keyword, 0) >= self.max_items_per_keyword:
                    if self.show_progress_bar and not self.company_debug_log:
                        self._render_progress(idx, total_items, "清洗打标进度")
                    continue

            title = item.get("title", "")
            content = item.get("content", "")
            comments = "\n".join(item.get("comments", []))

            content = re.sub(r'<br\s*/?>|</p>|</div>', '\n', content, flags=re.IGNORECASE)
            content = re.sub(r'<[^>]+>', '', content).strip()
            content = re.sub(r'\n{3,}', '\n\n', content)

            comments = re.sub(r'<br\s*/?>|</p>|</div>', '\n', comments, flags=re.IGNORECASE)
            comments = re.sub(r'<[^>]+>', '', comments).strip()
            comments = re.sub(r'\n{3,}', '\n\n', comments)

            company = self._extract_company(title, content)
            if self.drop_unknown_company_posts and company == "其他":
                self.dropped_unknown_company_count += 1
                self.dropped_unknown_company_examples.append({
                    "title": title,
                    "keyword": keyword,
                    "url": item.get("url", "")
                })
                if self.show_progress_bar and not self.company_debug_log:
                    self._render_progress(idx, total_items, "清洗打标进度")
                continue

            record = {
                "标题": title,
                "公司": company,
                "搜索关键词": keyword,
                "帖子链接": item.get("url", ""),
                "正文": content,
                "评论及回复": comments
            }
            if self.include_id_column:
                record = {"ID": item.get("id"), **record}

            cleaned_data.append(record)
            if keyword:
                kept_keyword_counter[keyword] = kept_keyword_counter.get(keyword, 0) + 1

            if self.show_progress_bar and not self.company_debug_log:
                self._render_progress(idx, total_items, "清洗打标进度")

        df = pd.DataFrame(cleaned_data, columns=expected_columns)
        return df

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

    def save_to_excel(self, df: pd.DataFrame, filename="nowcoder_data.xlsx"):
        try:
            with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='面经汇总')
                workbook = writer.book
                worksheet = writer.sheets['面经汇总']
                
                wrap_format = workbook.add_format({'text_wrap': True, 'valign': 'top'})
                title_format = workbook.add_format({'text_wrap': True, 'valign': 'bottom'})
                header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1, 'valign': 'top'})
                
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)

                if '标题' in df.columns:
                    title_col_idx = df.columns.get_loc('标题')
                    for row_num, title_value in enumerate(df['标题'].tolist(), start=1):
                        worksheet.write(row_num, title_col_idx, title_value, title_format)

                from xlsxwriter.utility import xl_col_to_name

                column_widths = {
                    'ID': 15,
                    '标题': 30,
                    '公司': 10,
                    '搜索关键词': 15,
                    '帖子链接': 45,
                    '正文': 80,
                    '评论及回复': 80,
                }

                for col_idx, col_name in enumerate(df.columns):
                    col_letter = xl_col_to_name(col_idx)
                    col_range = f"{col_letter}:{col_letter}"
                    width = column_widths.get(col_name, 20)

                    if col_name == '标题':
                        worksheet.set_column(col_range, width, title_format)
                    elif col_name in ('正文', '评论及回复'):
                        worksheet.set_column(col_range, width, wrap_format)
                    else:
                        worksheet.set_column(col_range, width)

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
                
                for idx, row in df.iterrows():
                    title = row.get('标题', '无标题')
                    company = row.get('公司', '未分类')
                    url = row.get('帖子链接', '无链接')
                    content = self._sanitize_markdown_text(row.get('正文', '无内容'))
                    comments = self._sanitize_markdown_text(row.get('评论及回复', ''))

                    f.write(f"## {title}\n\n")
                    f.write(f"- **公司/标签**: `{company}`\n")
                    f.write(f"- **链接**: {url}\n\n")
                    f.write(f"### 正文\n\n{content}\n\n")
                    if comments and str(comments).strip():
                        f.write(f"### 评论摘录\n\n{comments}\n\n")
                    f.write("---\n\n")
                    
            print(f"数据已成功排版并存入 Markdown 文件: '{filename}'")
            
        except Exception as e:
            print(f"保存至 Markdown 失败: {e}")

    def save_to_txt(self, df: pd.DataFrame, filename="nowcoder_data.txt"):
        if df.empty:
            print("没有可保存的数据！")
            return

        try:
            with open(filename, 'w', encoding='utf-8') as f:
                for idx, row in df.iterrows():
                    title = row.get('标题', '无标题')
                    company = row.get('公司', '未分类')
                    url = row.get('帖子链接', '无链接')
                    content = row.get('正文', '无内容')
                    comments = row.get('评论及回复', '')

                    f.write(f"【标题】 {title}\n")
                    f.write(f"【公司】 {company}\n")
                    f.write(f"【链接】 {url}\n")
                    f.write(f"【正文】\n{content}\n")
                    if comments and str(comments).strip():
                        f.write(f"\n【评论】\n{comments}\n")
                    f.write("\n" + "="*80 + "\n\n")

            print(f"数据已成功排版并存入纯文本 TXT 文件: '{filename}'")

        except Exception as e:
            print(f"保存至 TXT 失败: {e}")

    def display_stats(self, df: pd.DataFrame):
        print("\n--- 爬取数据总况 ---")
        print(f"总计爬取帖子数量: {len(df)}")

        if self.drop_unknown_company_posts and self.dropped_unknown_company_count > 0:
            print(f"已过滤未识别公司帖子: {self.dropped_unknown_company_count} 篇")

            if self.filtered_posts_log:
                limit = max(self.filtered_posts_log_limit, 0)
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