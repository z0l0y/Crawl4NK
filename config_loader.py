import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Tuple


PROJECT_ROOT = Path(__file__).resolve().parent
CONFIG_DIR = PROJECT_ROOT / "configs"


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception:
        return {}

    return {}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    result = deepcopy(base)

    for key, value in override.items():
        base_value = result.get(key)
        if isinstance(base_value, dict) and isinstance(value, dict):
            result[key] = _deep_merge(base_value, value)
        else:
            result[key] = deepcopy(value)

    return result


def _resolve_path(value: str) -> str:
    path = Path(value)
    if path.is_absolute():
        return str(path)
    return str((PROJECT_ROOT / path).resolve())


def _normalize_config_paths(config: Dict[str, Any]) -> Dict[str, Any]:
    normalized = deepcopy(config)

    companies_path = str(normalized.get("companies_path", "data/companies/companies.json") or "").strip()
    normalized["companies_path"] = _resolve_path(companies_path)

    filter_rules = normalized.setdefault("filter_rules", {})

    pattern_cache = filter_rules.setdefault("pattern_cache", {})
    cache_dir = str(pattern_cache.get("cache_dir", "ENV/.cache/text_matcher") or "").strip()
    if cache_dir:
        pattern_cache["cache_dir"] = _resolve_path(cache_dir)

    compression_cfg = filter_rules.setdefault("char_id_compression", {})
    dict_paths = compression_cfg.get("dictionary_paths", [])
    if isinstance(dict_paths, list):
        compression_cfg["dictionary_paths"] = [
            _resolve_path(str(path).strip())
            for path in dict_paths
            if str(path).strip()
        ]

    score_cfg = filter_rules.setdefault("score_filter", {})
    alg_path = str(score_cfg.get("alg_path", "data/algorithms/alg.json") or "").strip()
    if alg_path:
        score_cfg["alg_path"] = _resolve_path(alg_path)

    return normalized


def _candidate_config_files() -> List[Path]:
    files: List[Path] = []

    advanced = CONFIG_DIR / "advanced.json"
    advanced_tpl = CONFIG_DIR / "advanced.template.json"
    app = CONFIG_DIR / "app.json"
    app_tpl = CONFIG_DIR / "app.template.json"
    debug = CONFIG_DIR / "debug.json"
    debug_tpl = CONFIG_DIR / "debug.template.json"

    if advanced_tpl.exists():
        files.append(advanced_tpl)
    if advanced.exists():
        files.append(advanced)

    if debug_tpl.exists():
        files.append(debug_tpl)
    if debug.exists():
        files.append(debug)

    if app_tpl.exists():
        files.append(app_tpl)
    if app.exists():
        files.append(app)

    local_override = CONFIG_DIR / "local.override.json"
    if local_override.exists():
        files.append(local_override)

    return files


def load_config() -> Tuple[Dict[str, Any], List[str]]:
    merged: Dict[str, Any] = {}
    loaded_files: List[str] = []

    for path in _candidate_config_files():
        raw = _load_json(path)
        if not raw:
            continue
        merged = _deep_merge(merged, raw)
        loaded_files.append(str(path))

    normalized = _normalize_config_paths(merged)
    return normalized, loaded_files
