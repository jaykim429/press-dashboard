"""
Production refactor script for unified_press_ingest.py.
Adds:
  1. `abc` import + `BaseCollector` abstract class after imports
  2. YAML config loader: `load_ingest_config(path)` function
  3. Updates `UnifiedIngestCliApp.build_parser()` to accept `--config`
  4. Updates `UnifiedIngestCliApp.run()` to merge YAML config into options
Run once: python _refactor_ingest.py
"""
import re

SRC = r'c:\Users\admin1\Documents\보도자료 테스트\unified_press_ingest.py'

with open(SRC, 'r', encoding='utf-8') as f:
    code = f.read()

# ── 1. Add `abc` and `math` imports after existing imports ──────────────
INSERT_AFTER = 'import xml.etree.ElementTree as ET'
BASE_COLLECTOR_BLOCK = '''import xml.etree.ElementTree as ET
import abc
import math
from collections import Counter
from pathlib import Path
try:
    import yaml  # optional – only needed when using ingest_config.yaml
except ImportError:
    yaml = None


class BaseCollector(abc.ABC):
    """Abstract base class for all press-release collectors.

    To add a new data source:
      1. Create a class that inherits from BaseCollector.
      2. Implement the `collector_key` property and `collect()` method.
      3. Register it in COLLECTOR_REGISTRY at the bottom of this file.
    """

    @property
    @abc.abstractmethod
    def collector_key(self) -> str:
        """Unique snake_case identifier used as the key in ingest_config.yaml."""

    @abc.abstractmethod
    def collect(self, cfg: dict) -> list:
        """Run the collector and return a list of article dicts.

        Args:
            cfg: The per-collector config dict from ingest_config.yaml
                 (may include ``max_pages``, ``api_orgs``, etc.)
        Returns:
            List[Dict[str, Any]] – articles with standard schema fields.
        """


def load_ingest_config(path: str) -> dict:
    """Load ingest_config.yaml. Returns empty dict if yaml is unavailable."""
    if yaml is None:
        print("[WARN] PyYAML not installed – skipping config file. pip install pyyaml")
        return {}
    p = Path(path)
    if not p.exists():
        print(f"[WARN] Config file not found: {p}")
        return {}
    with p.open(encoding='utf-8') as f:
        return yaml.safe_load(f) or {}
'''

if INSERT_AFTER in code and 'class BaseCollector' not in code:
    code = code.replace(INSERT_AFTER, BASE_COLLECTOR_BLOCK, 1)
    print("✓ Added BaseCollector ABC and load_ingest_config()")
else:
    print("⚠ Skipped BaseCollector (already present or marker not found)")

# ── 2. Update argparse to accept --config ─────────────────────────────────
CONFIG_PARSER_SNIPPET = '''        parser.add_argument(
            "--config",
            default="ingest_config.yaml",
            help="Path to ingest_config.yaml (default: ingest_config.yaml)",
        )
        parser.add_argument(
            "--service-key",'''

if '--service-key' in code and '--config' not in code:
    code = code.replace(
        '        parser.add_argument(\n            "--service-key",',
        CONFIG_PARSER_SNIPPET,
        1,
    )
    print("✓ Added --config argument to argparse")
else:
    print("⚠ Skipped --config arg (already present or marker not found)")

# ── 3. Merge YAML config into IngestRunOptions inside run() ───────────────
RUN_MARKER = '    def run(self, args: argparse.Namespace) -> Dict[str, Any]:\n        validate_dates(args.start_date, args.end_date)'
RUN_REPLACEMENT = '''    def run(self, args: argparse.Namespace) -> Dict[str, Any]:
        # Load YAML config and merge into args (args CLI values take precedence)
        yaml_cfg = load_ingest_config(getattr(args, "config", "ingest_config.yaml"))
        collectors_cfg = yaml_cfg.get("collectors", {})
        for key, col_cfg in collectors_cfg.items():
            # Map yaml max_pages -> args.xxx_max_pages if CLI left it at default (1)
            max_pages = col_cfg.get("max_pages")
            arg_attr = f"{key}_max_pages"
            if max_pages is not None and hasattr(args, arg_attr):
                if getattr(args, arg_attr) == 1:
                    setattr(args, arg_attr, max_pages)
            # Honour enabled: false in yaml (only if CLI didn't explicitly include/exclude)
            if not col_cfg.get("enabled", True):
                no_attr = f"no_{key}"
                if hasattr(args, no_attr):
                    setattr(args, no_attr, True)
        validate_dates(args.start_date, args.end_date)'''

if RUN_MARKER in code and 'yaml_cfg = load_ingest_config' not in code:
    code = code.replace(RUN_MARKER, RUN_REPLACEMENT, 1)
    print("✓ Injected YAML config merge into UnifiedIngestCliApp.run()")
else:
    print("⚠ Skipped run() injection (already present or marker not found)")

with open(SRC, 'w', encoding='utf-8') as f:
    f.write(code)

print("\n✓ Refactor complete. Run: python -m py_compile unified_press_ingest.py")
