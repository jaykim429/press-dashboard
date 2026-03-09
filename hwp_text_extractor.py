#!/usr/bin/env python3
from __future__ import annotations

import re
import shutil
import subprocess
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
import xml.etree.ElementTree as ET

try:
    import olefile  # type: ignore
except Exception:
    olefile = None


@dataclass
class HwpExtractionResult:
    ok: bool
    extractor: str
    text: str
    error: Optional[str]
    metadata: Dict[str, object]

    @property
    def char_count(self) -> int:
        return len(self.text or "")


class HwpExtractor:
    """
    Unified interface for extracting text from .hwp/.hwpx documents.
    Strategy:
    - .hwpx: parse ZIP/XML sections
    - .hwp : parse OLE PrvText stream via olefile
    """

    def __init__(self, file_path: str):
        self.file_path = str(file_path)
        self.path = Path(file_path)
        self.ext = self.path.suffix.lower().lstrip(".")

    def extract(self) -> HwpExtractionResult:
        if self.ext == "hwpx":
            return self._extract_from_hwpx()
        if self.ext == "hwp":
            return self._extract_from_hwp()
        return HwpExtractionResult(
            ok=False,
            extractor="none",
            text="",
            error=f"Unsupported extension: {self.ext}. Supported: hwp, hwpx",
            metadata={},
        )

    def _extract_from_hwpx(self) -> HwpExtractionResult:
        try:
            with zipfile.ZipFile(self.file_path, "r") as zf:
                section_files = [n for n in zf.namelist() if re.match(r"Contents/section\d+\.xml$", n)]
                section_files.sort(key=lambda x: int(re.search(r"section(\d+)\.xml$", x).group(1)))
                if not section_files:
                    return HwpExtractionResult(
                        ok=False,
                        extractor="hwpx_xml_extractor",
                        text="",
                        error="No section XML found in HWPX package",
                        metadata={"zip_entries": len(zf.namelist())},
                    )

                texts: List[str] = []
                for sec in section_files:
                    root = ET.fromstring(zf.read(sec))
                    for node in root.iter():
                        tag = node.tag
                        local = tag.split("}", 1)[1] if isinstance(tag, str) and "}" in tag else str(tag)
                        if local in {"t", "text"} and node.text:
                            val = node.text.strip()
                            if val:
                                texts.append(val)

                merged = "\n".join(texts).strip()
                return HwpExtractionResult(
                    ok=True,
                    extractor="hwpx_xml_extractor",
                    text=merged,
                    error=None,
                    metadata={"section_count": len(section_files), "zip_entries": len(zf.namelist())},
                )
        except Exception as exc:
            return HwpExtractionResult(
                ok=False,
                extractor="hwpx_xml_extractor",
                text="",
                error=str(exc),
                metadata={},
            )

    def _extract_from_hwp(self) -> HwpExtractionResult:
        # 1) Preferred path: hwp5txt CLI for fuller body extraction.
        cli_result = self._extract_from_hwp_cli()
        if cli_result.ok:
            return cli_result

        # 2) Fallback path: OLE PrvText stream (preview text, partial possible).
        ole_result = self._extract_from_hwp_ole()
        if ole_result.ok:
            ole_result.metadata["fallback_from"] = "hwp5txt"
            ole_result.metadata["hwp5txt_error"] = cli_result.error
            return ole_result

        # 3) Both failed: return consolidated diagnostics.
        return HwpExtractionResult(
            ok=False,
            extractor="hwp_hybrid_extractor",
            text="",
            error=f"hwp5txt_failed={cli_result.error}; ole_failed={ole_result.error}",
            metadata={
                "hwp5txt": cli_result.metadata,
                "ole": ole_result.metadata,
            },
        )

    def _extract_from_hwp_cli(self) -> HwpExtractionResult:
        hwp5txt = shutil.which("hwp5txt")
        if not hwp5txt:
            return HwpExtractionResult(
                ok=False,
                extractor="hwp5txt_cli_extractor",
                text="",
                error="hwp5txt not found",
                metadata={},
            )
        try:
            proc = subprocess.run(
                [hwp5txt, self.file_path],
                check=True,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            text = (proc.stdout or "").strip()
            if not text:
                return HwpExtractionResult(
                    ok=False,
                    extractor="hwp5txt_cli_extractor",
                    text="",
                    error="hwp5txt returned empty text",
                    metadata={"command": hwp5txt, "returncode": proc.returncode},
                )
            return HwpExtractionResult(
                ok=True,
                extractor="hwp5txt_cli_extractor",
                text=text,
                error=None,
                metadata={"command": hwp5txt, "returncode": proc.returncode},
            )
        except Exception as exc:
            return HwpExtractionResult(
                ok=False,
                extractor="hwp5txt_cli_extractor",
                text="",
                error=str(exc),
                metadata={"command": hwp5txt},
            )

    def _extract_from_hwp_ole(self) -> HwpExtractionResult:
        if olefile is None:
            return HwpExtractionResult(
                ok=False,
                extractor="hwp_ole_preview_extractor",
                text="",
                error="olefile is not installed",
                metadata={},
            )
        try:
            if not olefile.isOleFile(self.file_path):
                return HwpExtractionResult(
                    ok=False,
                    extractor="hwp_ole_preview_extractor",
                    text="",
                    error="Not a valid OLE(HWP) file",
                    metadata={},
                )

            ole = olefile.OleFileIO(self.file_path)
            stream_name = "PrvText"
            if not ole.exists(stream_name):
                # Some HWP files may not include preview text.
                return HwpExtractionResult(
                    ok=False,
                    extractor="hwp_ole_preview_extractor",
                    text="",
                    error="OLE stream 'PrvText' not found",
                    metadata={"streams": ole.listdir()},
                )

            raw = ole.openstream(stream_name).read()
            text = raw.decode("utf-16", errors="replace").strip()
            return HwpExtractionResult(
                ok=True,
                extractor="hwp_ole_preview_extractor",
                text=text,
                error=None,
                metadata={"stream": stream_name},
            )
        except Exception as exc:
            return HwpExtractionResult(
                ok=False,
                extractor="hwp_ole_preview_extractor",
                text="",
                error=str(exc),
                metadata={},
            )
