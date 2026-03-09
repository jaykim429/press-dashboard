#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from document_text_extractor import DocumentTextExtractorService


def safe_print(text: str) -> None:
    try:
        print(text)
    except UnicodeEncodeError:
        enc = sys.stdout.encoding or "utf-8"
        fixed = text.encode(enc, errors="replace").decode(enc, errors="replace")
        print(fixed)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Test-only runner for unified document extractor")
    p.add_argument("file", help="Path to any document file")
    p.add_argument("--preview", type=int, default=800)
    p.add_argument("--json", action="store_true")
    return p


def main() -> None:
    args = build_parser().parse_args()
    file_path = Path(args.file)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    svc = DocumentTextExtractorService()
    ext = svc.infer_extension(file_path.name, file_path.as_posix(), None)
    result = svc.extract(file_path, ext)
    payload = {
        "ok": result.ok,
        "extractor": result.extractor,
        "text": result.text,
        "error": result.error,
        "metadata": result.metadata,
    }
    payload["char_count"] = len(result.text or "")
    payload["file_path"] = str(file_path)
    payload["ext"] = ext

    if args.json:
        safe_print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    safe_print(f"[OK] {result.ok}")
    safe_print(f"[Extractor] {result.extractor}")
    safe_print(f"[File] {file_path}")
    safe_print(f"[Ext] {ext}")
    safe_print(f"[Chars] {len(result.text or '')}")
    if result.error:
        safe_print(f"[Error] {result.error}")
    preview = (result.text or "")[: max(0, args.preview)]
    safe_print("\n--- Preview ---")
    safe_print(preview if preview else "(empty)")


if __name__ == "__main__":
    main()
