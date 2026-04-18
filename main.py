import argparse
import os
from datetime import datetime
from typing import Dict, List

from config_loader import load_config
from crawler import NowcoderCrawler
from data_processor import DataProcessor


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Nowcoder Interview Experience Crawler")
    parser.add_argument("--keywords", type=str, nargs="+", help="在牛客网搜索的关键词，覆盖配置文件关键词")
    parser.add_argument("--pages", type=int, help="最多搜索的页数")
    parser.add_argument("--items", type=int, help="每个关键词最多爬取的帖子数")
    parser.add_argument("--fill-valid-quota", action="store_true", help="按清洗后有效数据尽量补齐配额")
    parser.add_argument("--output", type=str, help="输出文件名前缀")
    return parser.parse_args()


def apply_cli_overrides(config: Dict, args: argparse.Namespace) -> Dict:
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
    return config


def build_output_filename_base(base_output: str, max_pages: int, max_items: int) -> str:
    base_name, ext = os.path.splitext(base_output)
    if not ext:
        ext = ".xlsx"

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{base_name}_p{max_pages}_i{max_items}_{timestamp}"


def print_runtime_summary(config: Dict, loaded_files: List[str], keywords: List[str], max_pages: int, max_items: int):
    if loaded_files:
        print("已加载配置文件:")
        for path in loaded_files:
            print(f"  - {path}")
    else:
        print("[警告] 未找到配置文件，将使用代码内默认值。")

    if not config.get("cookie") or "请在此处" in str(config.get("cookie", "")):
        print("[警告] 你尚未在 configs/app.json 中配置 Cookie。建议登录后从浏览器复制 Cookie。")

    print(f"正在启动牛客网爬虫，搜索关键词: {keywords}")
    print(f"搜集策略 -> 最大搜索页次: {max_pages}, 每个关键词期望最高爬取数: {max_items}")
    if config.get("fill_valid_quota", False):
        print("采集模式 -> 有效公司配额优先 (可能抓满页，以提升清洗后数量稳定性)")
    else:
        print("采集模式 -> 归档配额优先 (达到 max_items_per_keyword 后提前停止)")

    filter_rules = config.get("filter_rules", {})
    force_combine = filter_rules.get("force_combine", {})
    score_filter = filter_rules.get("score_filter", {})
    require_company_in_title = bool(config.get("require_company_in_title", True))

    print(f"内容拦截 -> 必须包含 {filter_rules.get('must_contain', [])}，排斥包含 {filter_rules.get('must_not_contain', [])}")
    print(f"白名单覆盖 -> {filter_rules.get('allow_overrides', [])}")
    print(f"跳字符匹配 -> {filter_rules.get('skip_char_match', {}).get('enabled', False)}")
    print(f"标题强规则 -> {force_combine.get('title_force_contains', [])}")
    print(f"正文组合规则 -> {force_combine.get('content_combine_contains', [])}")
    print(f"标题公司要求 -> {require_company_in_title} (未识别公司将直接丢弃)")
    print(f"模式缓存 -> {filter_rules.get('pattern_cache', {}).get('enabled', False)}")
    print(f"质量评分过滤 -> {score_filter.get('enabled', False)} (阈值: {score_filter.get('threshold', 62)})")

    if score_filter.get("enabled", False):
        print(
            f"评分配比 -> 长度 {score_filter.get('length_weight', 70)} / 匹配 {score_filter.get('match_weight', 30)}"
        )
        print(f"算法词库评分 -> {score_filter.get('alg_enabled', True)} (词库: {score_filter.get('alg_path', '')})")
        print(f"面试高频题加权 -> {score_filter.get('alg_hot_enabled', True)} (总封顶: {score_filter.get('alg_hot_total_cap', 48)})")


def print_filtered_score_details(crawler: NowcoderCrawler, config: Dict):
    if not crawler.score_filter_enabled:
        return

    print(f"质量评分结果 -> 阈值 {crawler.score_filter_threshold}，已过滤 {crawler.score_filtered_count} 篇低分帖子。")
    if not config.get("filtered_posts_log", False) or crawler.score_filtered_count <= 0:
        return

    print(f"评分过滤明细(展示 {len(crawler.score_filtered_examples)}/{crawler.score_filtered_count}):")
    for index, item in enumerate(crawler.score_filtered_examples, start=1):
        print(
            f"  [{index}] {item.get('score', 0)}/{item.get('threshold', crawler.score_filter_threshold)} | "
            f"字数={item.get('content_chars', 0)} | 标题: {item.get('title', '')}"
        )

        if item.get("length_score") is not None or item.get("match_score") is not None:
            print(
                f"      长度分: {item.get('length_score', 0)} | 匹配分: {item.get('match_score', 0)} "
                f"(长度比: {item.get('length_ratio', 0)}, 匹配比: {item.get('match_ratio', 0)})"
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

        hot_matches = item.get("alg_hot_matches") or []
        if hot_matches and isinstance(hot_matches[0], dict):
            first_match = hot_matches[0]
            print(
                f"      TOP算法: {first_match.get('title', '')} | "
                f"频度:{first_match.get('frequency', 0)} | 难度:{first_match.get('difficulty', '')}"
            )
            if first_match.get("url"):
                print(f"      TOP链接: {first_match.get('url')}")

        if item.get("keyword"):
            print(f"      关键词: {item.get('keyword')}")
        if item.get("tail_tags"):
            print(f"      尾部标签: {item.get('tail_tags')}")
        if item.get("url"):
            print(f"      链接: {item.get('url')}")


def export_outputs(processor: DataProcessor, df, output_filename_base: str, formats: List[str]):
    lowered = {str(fmt).lower() for fmt in (formats or [])}

    if "xlsx" in lowered or "excel" in lowered:
        processor.save_to_excel(df, f"{output_filename_base}.xlsx")
    if "md" in lowered or "markdown" in lowered:
        processor.save_to_markdown(df, f"{output_filename_base}.md")
    if "txt" in lowered or "text" in lowered:
        processor.save_to_txt(df, f"{output_filename_base}.txt")


def main():
    config, loaded_files = load_config()
    args = parse_args()
    config = apply_cli_overrides(config, args)

    keywords = config.get("keywords", ["后端", "面经"])
    max_pages = int(config.get("max_pages", 5) or 5)
    max_items = int(config.get("max_items_per_keyword", 10) or 10)
    base_output = str(config.get("output_file", "nowcoder_data") or "nowcoder_data")
    output_filename_base = build_output_filename_base(base_output, max_pages, max_items)
    formats = config.get("output_formats", ["xlsx"])
    runtime_summary_log = bool(config.get("runtime_summary_log", False))

    if runtime_summary_log:
        print_runtime_summary(config, loaded_files, keywords, max_pages, max_items)

    crawler = NowcoderCrawler(config=config)
    raw_data = crawler.crawl()
    print_filtered_score_details(crawler, config)

    if not raw_data:
        if crawler.score_filter_enabled and crawler.score_filtered_count > 0:
            print("未抓取到任何符合要求的数据！当前评分阈值可能偏高，可下调 score_filter.threshold 或放宽关键词加分配置。")
        else:
            print("未抓取到任何符合要求的数据！请检查配置文件的关键词与过滤规则，或检查网络和 Cookie。")
        return

    print(f"抓取完成，共获取 {len(raw_data)} 篇候选帖子。开始进行清洗和智能打标...")

    processor = DataProcessor(raw_data, config=config)
    df = processor.process()
    processor.display_stats(df)

    if df.empty:
        print("清洗完成后无可归档数据（标题中未识别公司名的帖子已过滤）。")
        return

    export_outputs(processor, df, output_filename_base, formats)


if __name__ == "__main__":
    main()
