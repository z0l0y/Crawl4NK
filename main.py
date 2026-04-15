import argparse
import json
import os
from datetime import datetime
from crawler import NowcoderCrawler
from data_processor import DataProcessor

def load_config():
    config_file = "config.json"
    if not os.path.exists(config_file):
        print("未找到 config.json 配置文件，将使用默认配置。")
        return {}
    with open(config_file, "r", encoding="utf-8") as f:
        return json.load(f)

def main():
    config = load_config()
    
    parser = argparse.ArgumentParser(description="Nowcoder Interview Experience Crawler")
    parser.add_argument("--keywords", type=str, nargs="+", help="在牛客网搜索的关键词，覆盖配置文件的关键词")
    parser.add_argument("--pages", type=int, help="最多搜索的页数")
    parser.add_argument("--items", type=int, help="每个关键词最多爬取的帖子数")
    parser.add_argument("--output", type=str, help="输出Excel文件名")
    
    args = parser.parse_args()

    if args.keywords:
        config["keywords"] = args.keywords
    if args.pages:
        config["max_pages"] = args.pages
    if args.items:
        config["max_items_per_keyword"] = args.items
    if args.output:
        config["output_file"] = args.output
        
    keywords = config.get("keywords", ["后端", "面经"])
    
    base_output = config.get("output_file", "nowcoder_data.xlsx")
    max_pages = config.get('max_pages', 5)
    max_items = config.get('max_items_per_keyword', 10)
    
    base_name, ext = os.path.splitext(base_output)
    if not ext:
        ext = ".xlsx"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename_base = f"{base_name}_p{max_pages}_i{max_items}_{timestamp}"
    
    formats = config.get("output_formats", ["xlsx"])

    if not config.get("cookie") or "请在此处" in config.get("cookie"):
        print("\n[警告] 您尚未在 config.json 中配置个人的系统 Cookie！虽然部分数据能访问，但也随时有被反爬折叠拉不到评论/搜索封禁的风险。建议在网站登录并按下 F12 获取你自己的 Cookie 配置下。\n")

    print(f"正在启动牛客网爬虫，搜索关键词: {keywords}")
    print(f"搜集策略 -> 最大搜索页次: {max_pages}, 每个关键词期望最高爬取数: {max_items}")
    print(f"内容拦截 -> 必须包含 {config.get('filter_rules', {}).get('must_contain', [])}， 排斥包含 {config.get('filter_rules', {}).get('must_not_contain', [])}")
    
    crawler = NowcoderCrawler(config=config)
    raw_data = crawler.crawl()
    
    if not raw_data:
        print("未抓取到任何符合要求的数据！请检查配置文件的关键词与过滤规则，或者您的网络、Cookie。")
        return
        
    print(f"\n抓取完成，共获取 {len(raw_data)} 篇候选帖子。开始进行清洗和智能打标...")
    
    processor = DataProcessor(raw_data, config=config)
    df = processor.process()
    
    processor.display_stats(df)

    if df.empty:
        print("清洗完成后无可归档数据（标题中未识别公司名的帖子已过滤）。")
        return
    
    if "xlsx" in formats or "excel" in formats:
        processor.save_to_excel(df, f"{output_filename_base}.xlsx")
        
    if "md" in formats or "markdown" in formats:
        processor.save_to_markdown(df, f"{output_filename_base}.md")

    if "txt" in formats or "text" in formats:
        processor.save_to_txt(df, f"{output_filename_base}.txt")

if __name__ == "__main__":
    main()
