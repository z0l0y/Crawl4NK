import argparse
import os
import sys
import threading
import time
from datetime import datetime
from typing import Dict, List

from config_loader import load_config
from crawler import NowcoderCrawler
from data_processor import DataProcessor


def run_with_spinner(label: str, func, enabled: bool = True, interval_sec: float = 0.12):
    if (not enabled) or (not getattr(sys.stdout, "isatty", lambda: False)()):
        return func()

    frames = "|/-\\"
    state = {
        "index": 0,
        "line_len": 0,
    }
    stop_event = threading.Event()
    start_ts = time.perf_counter()

    def _spin():
        while not stop_event.wait(max(interval_sec, 0.05)):
            frame = frames[state["index"] % len(frames)]
            state["index"] += 1
            elapsed = max(time.perf_counter() - start_ts, 0.0)
            display = f"[{label}] {frame} {elapsed:.1f}s"
            pad_len = max(state["line_len"] - len(display), 0)
            print(f"\r{display}{' ' * pad_len}", end="", flush=True)
            state["line_len"] = len(display)

    worker = threading.Thread(target=_spin, daemon=True)
    worker.start()

    try:
        return func()
    finally:
        stop_event.set()
        worker.join(timeout=0.6)
        elapsed = max(time.perf_counter() - start_ts, 0.0)
        display = f"[{label}] 完成 {elapsed:.2f}s"
        pad_len = max(state["line_len"] - len(display), 0)
        print(f"\r{display}{' ' * pad_len}")


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
    print("页数策略 -> fill_valid_quota=false 时严格按 max_pages；fill_valid_quota=true 时允许动态扩页(受 max_pages_hard_limit 限制)")

    filter_rules = config.get("filter_rules", {})
    force_combine = filter_rules.get("force_combine", {})
    score_filter = filter_rules.get("score_filter", {})
    require_company_in_title = bool(config.get("require_company_in_title", True))
    fetch_comments_enabled = bool(config.get("fetch_comments_enabled", False))
    include_comments_column = bool(config.get("include_comments_column", fetch_comments_enabled))

    print(f"内容拦截 -> 必须包含 {filter_rules.get('must_contain', [])}，排斥包含 {filter_rules.get('must_not_contain', [])}")
    print(f"白名单覆盖 -> {filter_rules.get('allow_overrides', [])}")
    print(f"跳字符匹配 -> {filter_rules.get('skip_char_match', {}).get('enabled', False)}")
    print(f"标题强规则 -> {force_combine.get('title_force_contains', [])}")
    print(f"正文组合规则 -> {force_combine.get('content_combine_contains', [])}")
    print(f"标题公司要求 -> {require_company_in_title} (未识别公司将直接丢弃)")
    print(f"公司识别缓存模式 -> {config.get('company_cache_mode', 'online')}")
    print(f"公司识别缓存持久化 -> {bool(config.get('company_runtime_cache_enabled', True))}")
    if bool(config.get("company_runtime_cache_enabled", True)):
        print(f"公司识别缓存文件 -> {config.get('company_runtime_cache_path', '')}")
        if str(config.get("company_cache_mode", "online") or "online").strip().lower() == "reuse_then_refresh":
            print(
                f"公司缓存末尾增量刷新 -> {bool(config.get('company_cache_refresh_after_run', True))} "
                f"(最多 {config.get('company_cache_refresh_max_tokens', 80)} 词)"
            )
    print(f"模式缓存 -> {filter_rules.get('pattern_cache', {}).get('enabled', False)}")
    pattern_cache = filter_rules.get("pattern_cache", {})
    print(f"自动机缓存复用上次版本 -> {bool(pattern_cache.get('automaton_reuse_latest_on_miss', True))}")
    print(
        f"自动机缓存清理 -> {bool(pattern_cache.get('automaton_cleanup_enabled', True))} "
        f"(每桶保留 {pattern_cache.get('automaton_cleanup_keep_per_bucket', 1)} 个PKL)"
    )
    print(f"质量评分过滤 -> {score_filter.get('enabled', False)} (阈值: {score_filter.get('threshold', 62)})")
    print(f"题目清单导出 -> {bool(config.get('include_question_outline', False))}")
    print(f"评论抓取 -> {fetch_comments_enabled}")
    print(f"评论列导出 -> {include_comments_column}")
    print(f"耗时分析日志 -> {bool(config.get('timing_profile_log', False))}")
    print(f"进度条刷新节流 -> {config.get('progress_render_interval_sec', 0.08)}s")
    if fetch_comments_enabled:
        print(
            f"评论请求节流 -> {config.get('comment_request_min_interval_sec', 0.25)}~"
            f"{config.get('comment_request_max_interval_sec', 0.8)}s/次"
        )
    else:
        print("评论请求节流 -> 已跳过(评论抓取关闭)")
    if bool(config.get("timing_profile_log", False)):
        print(
            f"耗时动态刷新 -> {bool(config.get('timing_profile_live_log', True))} "
            f"(间隔: {config.get('timing_profile_live_interval_sec', 0.35)}s)"
        )

    if score_filter.get("enabled", False):
        print(
            f"评分配比 -> 长度 {score_filter.get('length_weight', 70)} / 匹配 {score_filter.get('match_weight', 30)}"
        )
        print(
            f"结构评分 -> {score_filter.get('structure_scoring_enabled', True)} "
            f"(目标分点: {score_filter.get('structure_target_points', 8)}, 最小分点: {score_filter.get('structure_min_points', 3)}, "
            f"结构分上限: {score_filter.get('structure_max_score', 50)}, 匹配分上限: {score_filter.get('match_max_score', 50)})"
        )
        print(
            f"卖课拦截 -> {score_filter.get('promo_block_enabled', True)} "
            f"(最小命中词: {score_filter.get('promo_block_min_hits', 2)})"
        )
        print(
            f"编号广告拦截 -> {score_filter.get('numbered_ad_block_enabled', True)} "
            f"(最小段数: {score_filter.get('numbered_ad_block_min_items', 2)}, 比例阈值: {score_filter.get('numbered_ad_block_ratio', 0.5)}, "
            f"标记词阈值: {score_filter.get('numbered_ad_marker_block_hits', 3)})"
        )
        print(f"算法词库评分 -> {score_filter.get('alg_enabled', True)} (词库: {score_filter.get('alg_path', '')})")
        print(f"面试高频题加权 -> {score_filter.get('alg_hot_enabled', True)} (总封顶: {score_filter.get('alg_hot_total_cap', 48)})")


def print_filtered_score_details(crawler: NowcoderCrawler, config: Dict):
    if not crawler.score_filter_enabled:
        return

    print(f"质量评分结果 -> 阈值 {crawler.score_filter_threshold}，已过滤 {crawler.score_filtered_count} 篇低分帖子。")
    if not crawler.score_filtered_posts_log or crawler.score_filtered_count <= 0:
        return

    print(f"评分过滤明细(展示 {len(crawler.score_filtered_examples)}/{crawler.score_filtered_count}):")
    for index, item in enumerate(crawler.score_filtered_examples, start=1):
        effective_chars = int(item.get("content_chars", 0) or 0)
        raw_chars = int(item.get("raw_content_chars", effective_chars) or effective_chars)
        if raw_chars > effective_chars:
            char_text = f"{effective_chars}(原始:{raw_chars})"
        else:
            char_text = str(effective_chars)

        print(
            f"  [{index}] {item.get('score', 0)}/{item.get('threshold', crawler.score_filter_threshold)} | "
            f"字数={char_text} | 标题: {item.get('title', '')}"
        )

        if item.get("length_score") is not None or item.get("match_score") is not None:
            print(
                f"      结构分: {item.get('length_score', 0)}(旧长度分:{item.get('legacy_length_score', item.get('length_score', 0))}) | 匹配分: {item.get('match_score', 0)} "
                f"(结构比: {item.get('length_ratio', 0)}, 匹配比: {item.get('match_ratio', 0)})"
            )

        structured_count = int(item.get("structured_question_count", 0) or 0)
        if structured_count > 0:
            print(f"      分点题目数: {structured_count}")
        structured_preview = item.get("structured_questions", []) or []
        if structured_preview:
            print(f"      题目清单预览: {structured_preview[:5]}")

        if item.get("promo_blocked"):
            print(f"      卖课拦截: True | 命中词: {item.get('promo_hits', [])} | 惩罚: {item.get('promo_penalty', 0)}")

        if item.get("numbered_ad_blocked"):
            print(
                f"      编号广告拦截: True | 段数: {item.get('numbered_ad_segments', 0)} | "
                f"可疑段: {item.get('numbered_ad_suspicious_segments', 0)} | 比例: {item.get('numbered_ad_suspicious_ratio', 0)} | "
                f"命中词数: {item.get('numbered_ad_marker_hit_count', 0)} | 命中词: {item.get('numbered_ad_marker_hits', [])} | 惩罚: {item.get('numbered_ad_penalty', 0)}"
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


def print_pipeline_timing(enabled: bool, stage_costs: Dict[str, float]):
    if not enabled:
        return

    ordered = sorted(stage_costs.items(), key=lambda item: item[1], reverse=True)
    parts = [f"{stage}={cost:.3f}s" for stage, cost in ordered if cost >= 0]
    print("[TimeProfile] pipeline: " + " | ".join(parts))


def main():
    pipeline_start = time.perf_counter()
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
    timing_profile_log = bool(config.get("timing_profile_log", False))
    stage_spinner_enabled = bool(config.get("stage_spinner_enabled", True))
    try:
        stage_spinner_interval_sec = float(config.get("stage_spinner_interval_sec", 0.12) or 0.12)
    except Exception:
        stage_spinner_interval_sec = 0.12
    stage_spinner_interval_sec = max(stage_spinner_interval_sec, 0.05)

    if runtime_summary_log:
        print_runtime_summary(config, loaded_files, keywords, max_pages, max_items)
        print(f"阶段转圈提示 -> {stage_spinner_enabled} (间隔: {stage_spinner_interval_sec}s)")

    crawler_init_start = time.perf_counter()
    crawler = run_with_spinner(
        "初始化匹配器与缓存",
        lambda: NowcoderCrawler(config=config),
        enabled=stage_spinner_enabled,
        interval_sec=stage_spinner_interval_sec,
    )
    crawler_init_elapsed = time.perf_counter() - crawler_init_start

    crawl_start = time.perf_counter()
    raw_data = crawler.crawl()
    crawl_elapsed = time.perf_counter() - crawl_start
    print_filtered_score_details(crawler, config)

    if not raw_data:
        print_pipeline_timing(
            timing_profile_log,
            {
                "crawler_init": crawler_init_elapsed,
                "crawl": crawl_elapsed,
                "total": time.perf_counter() - pipeline_start,
            },
        )
        if crawler.score_filter_enabled and crawler.score_filtered_count > 0:
            print("未抓取到任何符合要求的数据！当前评分阈值可能偏高，可下调 score_filter.threshold 或放宽关键词加分配置。")
        else:
            print("未抓取到任何符合要求的数据！请检查配置文件的关键词与过滤规则，或检查网络和 Cookie。")
        return

    print(f"抓取完成，共获取 {len(raw_data)} 篇候选帖子。开始进行清洗和智能打标...")

    process_start = time.perf_counter()
    processor = run_with_spinner(
        "初始化数据处理器",
        lambda: DataProcessor(raw_data, config=config),
        enabled=stage_spinner_enabled,
        interval_sec=stage_spinner_interval_sec,
    )
    df = run_with_spinner(
        "数据清洗与打标",
        lambda: processor.process(),
        enabled=stage_spinner_enabled and (not bool(config.get("show_progress_bar", True))),
        interval_sec=stage_spinner_interval_sec,
    )
    process_elapsed = time.perf_counter() - process_start
    processor.display_stats(df)

    if df.empty:
        print_pipeline_timing(
            timing_profile_log,
            {
                "crawler_init": crawler_init_elapsed,
                "crawl": crawl_elapsed,
                "process": process_elapsed,
                "total": time.perf_counter() - pipeline_start,
            },
        )
        print("清洗完成后无可归档数据（标题中未识别公司名的帖子已过滤）。")
        return

    export_start = time.perf_counter()
    run_with_spinner(
        "导出文件",
        lambda: export_outputs(processor, df, output_filename_base, formats),
        enabled=stage_spinner_enabled and (not bool(config.get("show_progress_bar", True))),
        interval_sec=stage_spinner_interval_sec,
    )
    export_elapsed = time.perf_counter() - export_start

    print_pipeline_timing(
        timing_profile_log,
        {
            "crawler_init": crawler_init_elapsed,
            "crawl": crawl_elapsed,
            "process": process_elapsed,
            "export": export_elapsed,
            "total": time.perf_counter() - pipeline_start,
        },
    )


if __name__ == "__main__":
    main()
