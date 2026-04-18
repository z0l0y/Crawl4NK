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
    parser.add_argument("--fill-valid-quota", action="store_true", help="按清洗后有效数据尽量补齐配额(可能抓满页)")
    parser.add_argument("--output", type=str, help="输出Excel文件名")
    
    args = parser.parse_args()

    if args.keywords:
        config["keywords"] = args.keywords
    if args.pages:
        config["max_pages"] = args.pages
    if args.items:
        config["max_items_per_keyword"] = args.items
    if args.fill_valid_quota:
        config["fill_valid_quota"] = True
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
        print("[警告] 您尚未在 config.json 中配置个人的系统 Cookie！虽然部分数据能访问，但也随时有被反爬折叠拉不到评论/搜索封禁的风险。建议在网站登录并按下 F12 获取你自己的 Cookie 配置下。")

    print(f"正在启动牛客网爬虫，搜索关键词: {keywords}")
    print(f"搜集策略 -> 最大搜索页次: {max_pages}, 每个关键词期望最高爬取数: {max_items}")
    if config.get("fill_valid_quota", False):
        print("采集模式 -> 有效公司配额优先 (可能抓满页，以提升清洗后数量稳定性)")
    else:
        print("采集模式 -> 归档配额优先 (达到 max_items_per_keyword 后提前停止)")
    filter_rules = config.get("filter_rules", {})
    force_combine = filter_rules.get("force_combine", {})
    score_filter = filter_rules.get("score_filter", {})
    print(f"内容拦截 -> 必须包含 {filter_rules.get('must_contain', [])}， 排斥包含 {filter_rules.get('must_not_contain', [])}")
    print(f"白名单覆盖 -> {filter_rules.get('allow_overrides', [])}")
    print(f"跳字符匹配 -> {filter_rules.get('skip_char_match', {}).get('enabled', False)}")
    print(f"标题强规则 -> {force_combine.get('title_force_contains', [])}")
    print(f"正文组合规则 -> {force_combine.get('content_combine_contains', [])}")
    print(f"模式缓存 -> {filter_rules.get('pattern_cache', {}).get('enabled', False)}")
    print(f"质量评分过滤 -> {score_filter.get('enabled', False)} (阈值: {score_filter.get('threshold', 90)})")
    if score_filter.get("enabled", False):
        print(
            f"算法词库评分 -> {score_filter.get('alg_enabled', True)} (词库: {score_filter.get('alg_path', 'alg.json')})"
        )
        print(
            f"面试高频题加权 -> {score_filter.get('alg_hot_enabled', True)} (总封顶: {score_filter.get('alg_hot_total_cap', 48)})"
        )
    
    crawler = NowcoderCrawler(config=config)
    raw_data = crawler.crawl()

    if crawler.score_filter_enabled:
        print(f"质量评分结果 -> 阈值 {crawler.score_filter_threshold}，已过滤 {crawler.score_filtered_count} 篇低分帖子。")
        if config.get("filtered_posts_log", False) and crawler.score_filtered_count > 0:
            print(f"评分过滤明细(展示 {len(crawler.score_filtered_examples)}/{crawler.score_filtered_count}):")
            for i, item in enumerate(crawler.score_filtered_examples, start=1):
                print(
                    f"  [{i}] {item.get('score', 0)}/{item.get('threshold', crawler.score_filter_threshold)} | 字数={item.get('content_chars', 0)} | 标题: {item.get('title', '')}"
                )
                if item.get("alg_score", 0):
                    print(f"      算法分: {item.get('alg_score', 0)}")
                if item.get("alg_hot_score", 0):
                    print(f"      高频题加权分: {item.get('alg_hot_score', 0)}")
                if item.get("alg_problem_id_hits"):
                    print(f"      算法题号命中: {item.get('alg_problem_id_hits')}")
                if item.get("alg_problem_hits"):
                    print(f"      算法题目命中: {item.get('alg_problem_hits')[:5]}")
                if item.get("alg_topic_hits"):
                    print(f"      算法类型命中: {item.get('alg_topic_hits')[:8]}")
                if item.get("alg_hot_id_hits"):
                    print(f"      高频题号命中: {item.get('alg_hot_id_hits')[:8]}")
                if item.get("alg_hot_title_hits"):
                    print(f"      高频题目命中: {item.get('alg_hot_title_hits')[:5]}")
                if item.get("alg_hot_matches"):
                    first_match = item.get("alg_hot_matches")[0]
                    if isinstance(first_match, dict):
                        print(
                            f"      TOP算法: {first_match.get('title', '')} | 频度:{first_match.get('frequency', 0)} | 难度:{first_match.get('difficulty', '')}"
                        )
                        if first_match.get("url"):
                            print(f"      TOP链接: {first_match.get('url')}")
                if item.get("keyword"):
                    print(f"      关键词: {item.get('keyword')}")
                if item.get("tail_tags"):
                    print(f"      尾部标签: {item.get('tail_tags')}")
                if item.get("url"):
                    print(f"      链接: {item.get('url')}")
    
    if not raw_data:
        if crawler.score_filter_enabled and crawler.score_filtered_count > 0:
            print("未抓取到任何符合要求的数据！当前评分阈值可能偏高，可下调 score_filter.threshold 或放宽关键词加分配置。")
        else:
            print("未抓取到任何符合要求的数据！请检查配置文件的关键词与过滤规则，或者您的网络、Cookie。")
        return
        
    print(f"抓取完成，共获取 {len(raw_data)} 篇候选帖子。开始进行清洗和智能打标...")
    
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
