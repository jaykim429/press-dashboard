#!/usr/bin/env python3
from __future__ import annotations

import csv
import io
import json
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence
from urllib.parse import urlparse
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup

try:
    from pypdf import PdfReader  # type: ignore
except Exception:
    PdfReader = None

from hwp_text_extractor import HwpExtractor


@dataclass
class ExtractionOutcome:
    ok: bool
    extractor: str
    text: str
    error: Optional[str]
    metadata: Dict[str, object]


class BaseExtractorStrategy:
    name = "base"
    supported_exts: Sequence[str] = ()

    def supports(self, ext: str) -> bool:
        return ext.lower() in set(self.supported_exts)

    def extract(self, file_path: Path) -> ExtractionOutcome:
        raise NotImplementedError


class TextLikeExtractor(BaseExtractorStrategy):
    name = "text_like_extractor"
    supported_exts = ("txt", "md", "log", "ini", "cfg", "yaml", "yml")

    def extract(self, file_path: Path) -> ExtractionOutcome:
        try:
            text = file_path.read_text(encoding="utf-8", errors="replace")
            return ExtractionOutcome(True, self.name, text, None, {"ext": file_path.suffix.lower()})
        except Exception as exc:
            return ExtractionOutcome(False, self.name, "", str(exc), {})


class JsonExtractor(BaseExtractorStrategy):
    name = "json_extractor"
    supported_exts = ("json",)

    def extract(self, file_path: Path) -> ExtractionOutcome:
        try:
            raw = file_path.read_text(encoding="utf-8", errors="replace")
            obj = json.loads(raw)
            text = json.dumps(obj, ensure_ascii=False, indent=2)
            return ExtractionOutcome(True, self.name, text, None, {})
        except Exception as exc:
            return ExtractionOutcome(False, self.name, "", str(exc), {})


class CsvExtractor(BaseExtractorStrategy):
    name = "csv_extractor"
    supported_exts = ("csv",)

    def extract(self, file_path: Path) -> ExtractionOutcome:
        try:
            rows: List[str] = []
            with file_path.open("r", encoding="utf-8", errors="replace", newline="") as f:
                reader = csv.reader(f)
                for row in reader:
                    rows.append(" | ".join(x.strip() for x in row))
            return ExtractionOutcome(True, self.name, "\n".join(rows), None, {"row_count": len(rows)})
        except Exception as exc:
            return ExtractionOutcome(False, self.name, "", str(exc), {})


class HtmlExtractor(BaseExtractorStrategy):
    name = "html_extractor"
    supported_exts = ("html", "htm")

    def extract(self, file_path: Path) -> ExtractionOutcome:
        try:
            raw = file_path.read_text(encoding="utf-8", errors="replace")
            soup = BeautifulSoup(raw, "html.parser")
            text = soup.get_text("\n", strip=True)
            return ExtractionOutcome(True, self.name, text, None, {})
        except Exception as exc:
            return ExtractionOutcome(False, self.name, "", str(exc), {})


class XmlExtractor(BaseExtractorStrategy):
    name = "xml_extractor"
    supported_exts = ("xml",)

    def extract(self, file_path: Path) -> ExtractionOutcome:
        try:
            root = ET.fromstring(file_path.read_bytes())
            parts: List[str] = []
            for node in root.iter():
                if node.text and node.text.strip():
                    parts.append(node.text.strip())
            return ExtractionOutcome(True, self.name, "\n".join(parts), None, {"node_text_count": len(parts)})
        except Exception as exc:
            return ExtractionOutcome(False, self.name, "", str(exc), {})


class PdfExtractor(BaseExtractorStrategy):
    name = "pdf_extractor"
    supported_exts = ("pdf",)

    def extract(self, file_path: Path) -> ExtractionOutcome:
        if PdfReader is None:
            return ExtractionOutcome(False, self.name, "", "pypdf is not installed", {})
        try:
            reader = PdfReader(str(file_path))
            pages = [(p.extract_text() or "").strip() for p in reader.pages]
            text = "\n\n".join(p for p in pages if p)
            return ExtractionOutcome(True, self.name, text, None, {"pages": len(reader.pages)})
        except Exception as exc:
            return ExtractionOutcome(False, self.name, "", str(exc), {})


class DocxExtractor(BaseExtractorStrategy):
    name = "docx_xml_extractor"
    supported_exts = ("docx",)

    def extract(self, file_path: Path) -> ExtractionOutcome:
        try:
            with zipfile.ZipFile(file_path, "r") as zf:
                xml = zf.read("word/document.xml")
            root = ET.fromstring(xml)
            texts: List[str] = []
            for node in root.iter():
                tag = node.tag
                local = tag.split("}", 1)[1] if isinstance(tag, str) and "}" in tag else str(tag)
                if local == "t" and node.text:
                    texts.append(node.text)
            return ExtractionOutcome(True, self.name, "\n".join(texts), None, {"parts": len(texts)})
        except Exception as exc:
            return ExtractionOutcome(False, self.name, "", str(exc), {})


class PptxExtractor(BaseExtractorStrategy):
    name = "pptx_xml_extractor"
    supported_exts = ("pptx",)

    def extract(self, file_path: Path) -> ExtractionOutcome:
        try:
            with zipfile.ZipFile(file_path, "r") as zf:
                slide_files = [n for n in zf.namelist() if n.startswith("ppt/slides/slide") and n.endswith(".xml")]
                slide_files.sort()
                texts: List[str] = []
                for sf in slide_files:
                    root = ET.fromstring(zf.read(sf))
                    for node in root.iter():
                        tag = node.tag
                        local = tag.split("}", 1)[1] if isinstance(tag, str) and "}" in tag else str(tag)
                        if local == "t" and node.text:
                            texts.append(node.text)
            return ExtractionOutcome(True, self.name, "\n".join(texts), None, {"slides": len(slide_files)})
        except Exception as exc:
            return ExtractionOutcome(False, self.name, "", str(exc), {})


class XlsxExtractor(BaseExtractorStrategy):
    name = "xlsx_xml_extractor"
    supported_exts = ("xlsx",)

    def extract(self, file_path: Path) -> ExtractionOutcome:
        try:
            with zipfile.ZipFile(file_path, "r") as zf:
                shared_strings: List[str] = []
                if "xl/sharedStrings.xml" in zf.namelist():
                    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
                    for node in root.iter():
                        tag = node.tag
                        local = tag.split("}", 1)[1] if isinstance(tag, str) and "}" in tag else str(tag)
                        if local == "t" and node.text:
                            shared_strings.append(node.text)

                sheet_files = [n for n in zf.namelist() if n.startswith("xl/worksheets/sheet") and n.endswith(".xml")]
                sheet_files.sort()
                cells: List[str] = []
                for sf in sheet_files:
                    root = ET.fromstring(zf.read(sf))
                    for c in root.iter():
                        tag = c.tag
                        local = tag.split("}", 1)[1] if isinstance(tag, str) and "}" in tag else str(tag)
                        if local != "c":
                            continue
                        t_attr = c.attrib.get("t")
                        v_node = None
                        for child in c:
                            child_tag = child.tag
                            child_local = child_tag.split("}", 1)[1] if isinstance(child_tag, str) and "}" in child_tag else str(child_tag)
                            if child_local == "v":
                                v_node = child
                                break
                        if v_node is None or v_node.text is None:
                            continue
                        val = v_node.text
                        if t_attr == "s":
                            try:
                                idx = int(val)
                                if 0 <= idx < len(shared_strings):
                                    val = shared_strings[idx]
                            except Exception:
                                pass
                        cells.append(val)
            return ExtractionOutcome(True, self.name, "\n".join(cells), None, {"cells": len(cells), "sheets": len(sheet_files)})
        except Exception as exc:
            return ExtractionOutcome(False, self.name, "", str(exc), {})


class HwpFamilyExtractor(BaseExtractorStrategy):
    name = "hwp_family_extractor"
    supported_exts = ("hwp", "hwpx")

    def extract(self, file_path: Path) -> ExtractionOutcome:
        result = HwpExtractor(str(file_path)).extract()
        return ExtractionOutcome(
            ok=result.ok,
            extractor=result.extractor,
            text=result.text,
            error=result.error,
            metadata=result.metadata,
        )


class FallbackBinaryExtractor(BaseExtractorStrategy):
    name = "binary_fallback_extractor"
    supported_exts = ()

    def supports(self, ext: str) -> bool:
        return True

    def extract(self, file_path: Path) -> ExtractionOutcome:
        try:
            raw = file_path.read_bytes()
            # Try to salvage printable chunks.
            text = raw.decode("utf-8", errors="replace")
            text = re.sub(r"[^\S\r\n]+", " ", text)
            return ExtractionOutcome(True, self.name, text, None, {"warning": "fallback decode used"})
        except Exception as exc:
            return ExtractionOutcome(False, self.name, "", str(exc), {})


class DocumentTextExtractorService:
    def __init__(self):
        self.extractors: List[BaseExtractorStrategy] = [
            HwpFamilyExtractor(),
            PdfExtractor(),
            DocxExtractor(),
            PptxExtractor(),
            XlsxExtractor(),
            HtmlExtractor(),
            XmlExtractor(),
            JsonExtractor(),
            CsvExtractor(),
            TextLikeExtractor(),
            FallbackBinaryExtractor(),
        ]

    def infer_extension(self, file_name: Optional[str], url: str, content_type: Optional[str]) -> str:
        if file_name and "." in file_name:
            return file_name.rsplit(".", 1)[-1].lower()
        path = urlparse(url).path
        if "." in path:
            return path.rsplit(".", 1)[-1].lower()
        ct = (content_type or "").lower()
        if "pdf" in ct:
            return "pdf"
        if "html" in ct:
            return "html"
        if "json" in ct:
            return "json"
        if "xml" in ct:
            return "xml"
        if "csv" in ct:
            return "csv"
        if "text/plain" in ct:
            return "txt"
        if "wordprocessingml.document" in ct:
            return "docx"
        if "presentationml.presentation" in ct:
            return "pptx"
        if "spreadsheetml.sheet" in ct:
            return "xlsx"
        return "bin"

    def extract(self, file_path: Path, ext: str) -> ExtractionOutcome:
        ext = (ext or "").lower()
        for extractor in self.extractors:
            if extractor.supports(ext):
                return extractor.extract(file_path)
        # Should not happen due to fallback extractor.
        return ExtractionOutcome(False, "none", "", f"No extractor found for ext={ext}", {})
