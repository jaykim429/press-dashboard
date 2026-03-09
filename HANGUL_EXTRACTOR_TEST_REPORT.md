# Hangul Extractor Test Report

Date: 2026-03-06  
Scope: Test-only extraction for `.hwp` / `.hwpx` using `HwpExtractor`

## 1) Test Files

- `C:\Users\admin1\Documents\보도자료 테스트\hwptest\2. 금융위원회 운영규칙 일부개정고시.hwpx`
- `C:\Users\admin1\Documents\보도자료 테스트\hwptest\코스닥시장 공시규정 시행세칙_신구조문.hwp`

## 2) Environment / Dependency

- Python: 3.10
- `olefile`: installed (`0.47`)
- `hwp5txt`: not installed (`None`)

## 3) Result Summary

### A. HWPX
- Status: `OK=True`
- Extractor: `hwpx_xml_extractor`
- Extracted chars: `1773`
- Note: ZIP/XML section parsing worked as expected.

### B. HWP
- Status: `OK=True`
- Extractor used: `hwp_ole_preview_extractor`
- Extracted chars: `772`
- Note: `hwp5txt` was unavailable, so extractor automatically used OLE `PrvText` fallback.

## 4) Hybrid Strategy Verification (`.hwp`)

Current extraction order:
1. `hwp5txt` CLI (preferred)
2. OLE `PrvText` fallback
3. consolidated error if both fail

Observed metadata during test:
- `fallback_from: hwp5txt`
- `hwp5txt_error: hwp5txt not found`

## 5) Conclusion

- `.hwpx` extraction: working
- `.hwp` extraction: working via fallback path
- Hybrid strategy is active and functioning.
