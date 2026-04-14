import requests
import time
import random
import logging
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class NowcoderCrawler:
    def __init__(self, config):
        self.config = config
        self.keywords = config.get("keywords", ["后端面经"])
        self.max_pages = config.get("max_pages", 1)
        self.max_items = config.get("max_items_per_keyword", 10)
        self.filter_rules = config.get("filter_rules", {})
        self.debug_log = config.get("debug_log", False)
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Content-Type": "application/json",
            "Origin": "https://www.nowcoder.com",
            "Referer": "https://www.nowcoder.com/",
            "Cookie": config.get("cookie", "")
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
    def _is_valid_post(self, title):
        title_lower = title.lower()
        must_contain = self.filter_rules.get("must_contain", [])
        must_not_contain = self.filter_rules.get("must_not_contain", [])
        
        for word in must_not_contain:
            if word.lower() in title_lower:
                return False
                
        if must_contain:
            has_valid_word = False
            for word in must_contain:
                if word.lower() in title_lower:
                    has_valid_word = True
                    break
            if not has_valid_word:
                return False
                
        return True
        
    def search_posts(self, keyword):
        url = "https://gw-c.nowcoder.com/api/sparta/pc/search"
        posts = []
        for page in range(1, self.max_pages + 1):
            if len(posts) >= self.max_items:
                break
                
            payload = {
                "type": "all",
                "query": keyword,
                "page": page,
                "tag": [],
                "order": "create"
            }
            try:
                logging.info(f"正在搜索关键词 '{keyword}' 第 {page} 页...")
                resp = self.session.post(url, json=payload, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    records = data.get("data", {}).get("records", [])
                    for record in records:
                        item_data = record.get("data", {})
                        moment_data = item_data.get("momentData", {})
                        content_data = item_data.get("contentData", {})
                        
                        post_id = moment_data.get("id") or content_data.get("id")
                        post_uuid = moment_data.get("uuid") or content_data.get("uuid")
                        title = moment_data.get("title") or content_data.get("title") or "无标题"
                        raw_content = content_data.get("content") or moment_data.get("content") or ""
                        
                        if post_id and post_uuid:
                            is_valid = self._is_valid_post(title)
                            if not is_valid:
                                if self.debug_log:
                                    logging.info(f"    [-] 不符合要求，丢弃帖子: {title}")
                            else:
                                if self.debug_log:
                                    logging.info(f"    [+] 命中目标，准备抓取并解析: {title}")
                                posts.append({
                                    "id": post_id,
                                    "uuid": post_uuid,
                                    "title": title,
                                    "keyword": keyword,
                                    "search_content": raw_content
                                })
                            
                            if len(posts) >= self.max_items:
                                break
                                
                time.sleep(random.uniform(1.5, 3.5))
            except Exception as e:
                logging.error(f"搜索出错: {e}")
        return posts

    def get_post_detail(self, post_uuid):
        url = f"https://www.nowcoder.com/feed/main/detail/{post_uuid}?sourceSSR=search"
        try:
            api_headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*"
            }
            content = ""
            for api_type in ['content-data', 'moment-data']:
                try:
                    api_url = f"https://gw-c.nowcoder.com/api/sparta/detail/{api_type}/detail/{post_uuid}"
                    api_resp = self.session.get(api_url, headers=api_headers, timeout=5)
                    if api_resp.status_code == 200:
                        res_json = api_resp.json()
                        if res_json.get("data") and isinstance(res_json["data"], dict) and res_json["data"].get("content"):
                            content = res_json["data"]["content"]
                            break
                except Exception as e:
                    logging.debug(f"API 获取失败，尝试下一个API: {e}")
            
            url = f"https://www.nowcoder.com/feed/main/detail/{post_uuid}?sourceSSR=search"
            resp = self.session.get(url, timeout=10)
            if resp.status_code == 200 and not content:
                import re, json
                match = re.search(r'window\.__INITIAL_STATE__\s*=\s*({.*?});?', resp.text)
                if match:
                    try:
                        data = json.loads(match.group(1))
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
                        logging.error(f"解析 JSON 错误: {e}")

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
                return {"content": content.replace('\u200b', ''), "tags": [], "url": f"https://www.nowcoder.com/feed/main/detail/{post_uuid}"}

            time.sleep(random.uniform(1.0, 2.0))
        except Exception as e:
            logging.error(f"获取详情失败 (UUID: {post_uuid}): {e}")
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
            resp = self.session.post(url, json=payload, timeout=10)
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
            logging.error(f"获取评论失败 (ID: {post_id}): {e}")
        return comments_list

    def crawl(self):
        all_data = []
        for kw in self.keywords:
            posts = self.search_posts(kw)
            for p in posts:
                detail = self.get_post_detail(p["uuid"])
                
                final_content = detail.get("content")
                if not final_content:
                    final_content = p.get("search_content", "")
                
                if final_content:
                    p.update(detail)
                    p["content"] = final_content
                    p["comments"] = self.get_comments(p["id"])
                    all_data.append(p)
                    logging.info(f"成功拉取: {p['title']}")
                else:
                    if self.debug_log:
                        logging.warning(f"    [-] 丢弃帖子，未能正确抓取到任何正文 (API搜索端与详情页均空): {p['title']} [{p['id']}]")
        return all_data
