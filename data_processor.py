import re
import pandas as pd
from typing import List, Dict

class DataProcessor:
    def __init__(self, data: List[Dict]):
        self.data = data
        self.companies = [
            "腾讯", "字节", "字节跳动", "阿里", "淘天", "蚂蚁", "美团", "百度", "京东", "快手", 
            "滴滴", "拼多多", "pdd", "虾皮", "shopee", "携程", "蔚来", "网易", "小红书", 
            "b站", "哔哩哔哩", "华为", "微软", "深信服", "微众", "货拉拉", "米哈游", "得物"
        ]

    def _extract_company(self, title: str, content: str):
        full_text = f"{title} {content}".lower()
        matched_companies = []
        for comp in self.companies:
            if comp.lower() in full_text:
                if comp in ["字节跳动"]: comp = "字节"
                if comp in ["pdd"]: comp = "拼多多"
                if comp in ["淘天", "蚂蚁"]: comp = "阿里"
                if comp in ["shopee"]: comp = "虾皮"
                if comp in ["哔哩哔哩"]: comp = "b站"
                
                if comp not in matched_companies:
                    matched_companies.append(comp)
        
        return ", ".join(matched_companies) if matched_companies else "其他"

    def process(self):
        cleaned_data = []
        for item in self.data:
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
            
            cleaned_data.append({
                "ID": item.get("id"),
                "标题": title,
                "公司": company,
                "搜索关键词": item.get("keyword", ""),
                "帖子链接": item.get("url", ""),
                "正文": content,
                "评论及回复": comments
            })
            
        df = pd.DataFrame(cleaned_data)
        return df

    def save_to_excel(self, df: pd.DataFrame, filename="nowcoder_data.xlsx"):
        try:
            with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='面经汇总')
                workbook = writer.book
                worksheet = writer.sheets['面经汇总']
                
                wrap_format = workbook.add_format({'text_wrap': True, 'valign': 'top'})
                header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1, 'valign': 'top'})
                
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                
                worksheet.set_column('A:A', 15)
                worksheet.set_column('B:B', 30, wrap_format)
                worksheet.set_column('C:C', 10)
                worksheet.set_column('D:D', 15)
                worksheet.set_column('E:E', 45)
                worksheet.set_column('F:F', 80, wrap_format)
                worksheet.set_column('G:G', 80, wrap_format)

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
                    content = row.get('正文', '无内容')
                    comments = row.get('评论及回复', '')

                    f.write(f"## [{title}]({url})\n\n")
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
        print("\n提及公司统计:")
        categories_split = df['公司'].str.split(', ').explode()
        counts = categories_split.value_counts()
        for comp, count in counts.items():
            if str(comp).strip():
                print(f"  - {comp}: {count} 篇")
        print("\n--------------------\n")