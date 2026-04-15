import requests
import time
import random
import logging
from requests.adapters import HTTPAdapter
from requests.exceptions import Timeout, RequestException
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from matcher import TextMatcher

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class NowcoderCrawler:
    def __init__(self, config):
        self.config = config
        self.keywords = config.get("keywords", ["后端面经"])
        self.max_pages = config.get("max_pages", 1)
        self.max_items = config.get("max_items_per_keyword", 10)
        self.fill_valid_quota = bool(config.get("drop_unknown_company_posts", True))
        self.filter_rules = config.get("filter_rules", {})
        self.crawl_debug_log = bool(config.get("crawl_debug_log", config.get("debug_log", False)))
        self.show_progress_bar = bool(config.get("show_progress_bar", True))
        self.request_connect_timeout = float(config.get("request_connect_timeout", 5) or 5)
        self.request_read_timeout = float(config.get("request_read_timeout", 15) or 15)
        self.request_retry_count = int(config.get("request_retry_count", 2) or 2)
        self.request_retry_backoff = float(config.get("request_retry_backoff", 0.5) or 0.5)
        self._progress_active = False

        if self.crawl_debug_log:
            logging.getLogger().setLevel(logging.INFO)
        else:
            logging.getLogger().setLevel(logging.WARNING)

        self.matcher = TextMatcher(self.filter_rules.get('must_contain', []))
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Content-Type": "application/json",
            "Origin": "https://www.nowcoder.com",
            "Referer": "https://www.nowcoder.com/",
            "Cookie": config.get("cookie", "")
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
            msg = f"{stage} 超时，已跳过: {identifier}"
            if self.crawl_debug_log:
                msg += f" ({exc})"
            self._log_warning(msg)
            return

        msg = f"{stage} 请求失败: {identifier}"
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
        
    def _is_valid_title(self, title):
        must_not_contain = self.filter_rules.get('must_not_contain', [])
        for word in must_not_contain:
            if word.lower() in title.lower():
                return False
        return True

    def _is_valid_content(self, title, content_text=''):
        if self.filter_rules.get('must_contain'):
            return self.matcher.match(title, content_text)
        return True
        
    def search_posts(self, keyword):
        url = "https://gw-c.nowcoder.com/api/sparta/pc/search"
        posts = []
        enforce_limit_in_search = (self.max_items > 0) and (not self.fill_valid_quota)

        for page in range(1, self.max_pages + 1):
            if enforce_limit_in_search and len(posts) >= self.max_items:
                break
                
            payload = {
                "type": "all",
                "query": keyword,
                "page": page,
                "tag": [],
                "order": "create"
            }
            try:
                if self.crawl_debug_log:
                    logging.info(f"正在搜索关键词 '{keyword}' 第 {page} 页...")
                resp = self.session.post(url, json=payload, timeout=self.search_timeout)
                if resp.status_code == 200:
                    data = resp.json()
                    records = data.get("data", {}).get("records", [])
                    if not records:
                        break

                    for record in records:
                        item_data = record.get("data", {})
                        moment_data = item_data.get("momentData", {})
                        content_data = item_data.get("contentData", {})
                        
                        post_id = moment_data.get("id") or content_data.get("id")
                        post_uuid = moment_data.get("uuid") or content_data.get("uuid")
                        detail_api_type = "moment-data" if moment_data.get("uuid") else "content-data"
                        title = moment_data.get("title") or content_data.get("title") or "无标题"
                        raw_content = content_data.get("content") or moment_data.get("content") or ""
                        
                        if post_id and post_uuid:
                            if not self._is_valid_title(title):
                                if self.crawl_debug_log:
                                    logging.info(f"[NowcoderCrawler.search_posts] [-] 标题检验未通过(涉黑名单等),快速失败丢弃帖子: {title}")
                                continue
                                
                            if not self._is_valid_content(title, raw_content):
                                if self.crawl_debug_log:
                                    logging.info(f"[NowcoderCrawler.search_posts] [-] 正文匹配未通过,丢弃帖子: {title}")
                                continue

                            if self.crawl_debug_log:
                                logging.info(f"[NowcoderCrawler.search_posts] [+] 命中目标,准备抓取解析: {title}")
                            posts.append({
                                    "id": post_id,
                                    "uuid": post_uuid,
                                    "detail_api_type": detail_api_type,
                                    "title": title,
                                    "keyword": keyword,
                                    "search_content": raw_content
                                })
                            
                            if enforce_limit_in_search and len(posts) >= self.max_items:
                                break
                                
                time.sleep(random.uniform(1.5, 3.5))
            except Exception as e:
                self._log_network_exception("搜索", f"{keyword} 第{page}页", e)
        return posts

    def get_post_detail(self, post_id, post_uuid, detail_api_type=None):
        canonical_url = f"https://www.nowcoder.com/feed/main/detail/{post_uuid}"
        if detail_api_type == "content-data" and post_id is not None:
            canonical_url = f"https://www.nowcoder.com/discuss/{post_id}"

        url = canonical_url
        try:
            api_headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*"
            }
            content = ""

            candidate_details = []
            if detail_api_type == "content-data":
                if post_id is not None:
                    candidate_details.append(("content-data", str(post_id)))
                if post_uuid:
                    candidate_details.append(("moment-data", str(post_uuid)))
            elif detail_api_type == "moment-data":
                if post_uuid:
                    candidate_details.append(("moment-data", str(post_uuid)))
                if post_id is not None:
                    candidate_details.append(("content-data", str(post_id)))
            else:
                if post_id is not None:
                    candidate_details.append(("content-data", str(post_id)))
                if post_uuid:
                    candidate_details.append(("moment-data", str(post_uuid)))

            visited = set()
            for api_type, detail_id in candidate_details:
                key = (api_type, detail_id)
                if key in visited:
                    continue
                visited.add(key)

                try:
                    api_url = f"https://gw-c.nowcoder.com/api/sparta/detail/{api_type}/detail/{detail_id}"
                    api_resp = self.session.get(api_url, headers=api_headers, timeout=self.detail_api_timeout)
                    if api_resp.status_code == 200:
                        res_json = api_resp.json()
                        if res_json.get("data") and isinstance(res_json["data"], dict) and res_json["data"].get("content"):
                            content = res_json["data"]["content"]
                            break
                except Exception as e:
                    if self.crawl_debug_log:
                        logging.debug(f"API 获取失败，尝试下一个API: {e}")
            
            url = canonical_url
            resp = self.session.get(url, timeout=self.detail_page_timeout)
            if resp.status_code == 200 and not content:
                import re, json, urllib.parse
                match = re.search(r'window\.__INITIAL_STATE__\s*=\s*(.*?)(?:;\(function|;\s*</script>|</script>)', resp.text, flags=re.DOTALL)
                if match:
                    try:
                        raw_data = match.group(1).strip(' \t\r\n;')
                        if raw_data.startswith('%7B') or raw_data.startswith('%22'):
                            raw_data = urllib.parse.unquote(raw_data)
                            
                        data = json.loads(raw_data)
                        if isinstance(data, str):
                            data = json.loads(data)
                            
                        for key, v in data.get('prefetchData', {}).items():
                            if isinstance(v, dict) and 'ssrCommonData' in v:
                                ssr_data = v['ssrCommonData']
                                for data_key in ['contentData', 'momentData']:
                                    if data_key in ssr_data:
                                        item_dict = ssr_data[data_key]
                                        if isinstance(item_dict, dict) and 'content' in item_dict and item_dict['content']:
                                            content = item_dict['content']
                                            break
                                if content:
                                    break
                    except Exception as e:
                        if self.crawl_debug_log:
                            self._log_warning(f"解析 JSON 错误 (UUID: {post_uuid}): {e}")

                soup = BeautifulSoup(resp.text, 'html.parser')
                
                if not content:
                    content_div = soup.find('div', class_='feed-content-text') or \
                                  soup.find('div', class_='nc-slate-editor-content') or \
                                  soup.find('div', class_='nc-post-content') or \
                                  soup.find('div', class_='post-content') or \
                                  soup.find('div', class_='feed-content') or \
                                  soup.find('div', class_='content-box')
                    if content_div:
                        for tag in content_div.find_all(['br', 'p', 'div']):
                            tag.insert_after('\n')
                        content = content_div.get_text(separator=' ', strip=True)
                
                if content:
                    content = content.replace('\u200b', '')
                
                tags = [tag.get_text(strip=True) for tag in soup.find_all('a', class_='discuss-tag-item') if soup]
                return {
                    "content": content,
                    "tags": tags,
                    "url": url
                }
            elif content:
                return {"content": content.replace('\u200b', ''), "tags": [], "url": canonical_url}

            time.sleep(random.uniform(1.0, 2.0))
        except Exception as e:
            self._log_network_exception("获取详情", f"UUID: {post_uuid}", e)
        return {"content": "", "tags": [], "url": url}

    def get_comments(self, post_id):
        comments_list = []
        url = "https://gw-c.nowcoder.com/api/sparta/reply/v2/reply/list"
        payload = {
            "entityId": post_id,
            "entityType": 8,
            "page": 1,
            "pageSize": 50,
            "order": 1
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
            self._log_network_exception("获取评论", f"ID: {post_id}", e)
        return comments_list

    def crawl(self):
        all_data = []
        queued_posts = []

        total_keywords = len(self.keywords)
        for kw_index, kw in enumerate(self.keywords, start=1):
            posts = self.search_posts(kw)
            queued_posts.extend(posts)
            if self.show_progress_bar and not self.crawl_debug_log:
                self._render_progress(kw_index, total_keywords, "关键词检索进度")

        total_posts = len(queued_posts)
        for post_index, p in enumerate(queued_posts, start=1):
            detail = self.get_post_detail(p.get("id"), p.get("uuid"), p.get("detail_api_type"))
                
            final_content = detail.get("content")
            if not final_content:
                final_content = p.get("search_content", "")
                
            if final_content:
                p.update(detail)
                p["content"] = final_content
                p["comments"] = self.get_comments(p["id"])
                all_data.append(p)
                if self.crawl_debug_log:
                    logging.info(f"成功拉取: {p['title']}")
            else:
                if self.crawl_debug_log:
                    logging.warning(f"    [-] 丢弃帖子，未能正确抓取到任何正文 (API搜索端与详情页均空): {p['title']} [{p['id']}]")

            if self.show_progress_bar and not self.crawl_debug_log:
                self._render_progress(post_index, total_posts, "抓取详情进度")
        return all_data
