# ?? 10? ?? ?? ??? ??

- ?? ??: 2026-03-06T17:53:42
- ?? ??: `python attachment_pipeline.py --db-path press_unified.db --download-dir attachment_store --batch-size 10 --max-retry 1`

## ?? ??
- attachments_total: `8837`
- attachment_documents_total: `8823`
- attachment_extractions_total: `20`

## attachments ?? ??
- pending: `8817`
- success: `16`
- failed: `4`

## attachment_documents ?? ??
- pending: `8803`
- downloaded: `16`
- failed: `4`

## ?? ??? ?? 10?
|id|article_id|file_name|ext|status|last_processed_at|last_error|
|---:|---:|---|---|---|---|---|
|25403|15419|260306 2월 소비자물가 동향 보도자료.pdf|pdf|success|2026-03-06T17:53:08||
|25404|15420|(보도자료) 홍콩 소재 글로벌 투자기관 대상 한국경제 투자설명회 개최.pdf|pdf|failed|2026-03-06T17:53:08|Stream has ended unexpectedly|
|25405|15420|(보도자료) 홍콩 소재 글로벌 투자기관 대상 한국경제 투자설명회 개최.hwpx|hwpx|failed|2026-03-06T17:53:08|File is not a zip file|
|25406|15421|2026년4월물가연동계수(20260306).xlsx|xlsx|success|2026-03-06T17:53:07||
|25473|15448|260305(보도참고) 금융위원회 인사 보도(서기관 승진).hwp|hwp|success|2026-03-06T17:53:07||
|25474|15448|260305(보도참고) 금융위원회 인사 보도(서기관 승진).pdf|pdf|success|2026-03-06T17:53:07||
|25480|15420|(보도자료) 홍콩 소재 글로벌 투자기관 대상 한국경제 투자설명회 개최.hwpx|hwpx|success|2026-03-06T17:53:07||
|25481|15420|(보도자료) 홍콩 소재 글로벌 투자기관 대상 한국경제 투자설명회 개최.pdf|pdf|success|2026-03-06T17:53:07||
|25475|15448|260305[참고] 주요 약력.hwp|hwp|success|2026-03-06T17:53:06||
|25476|15448|260305[참고] 주요 약력.pdf|pdf|success|2026-03-06T17:53:06||

## ?? extraction 10?
|id|status|char_count|error_message|url|
|---:|---|---:|---|---|
|20|success|2937||https://www.korea.kr/common/download.do?tblKey=GMN&fileId=198378535|
|19|failed|0|Stream has ended unexpectedly|https://www.korea.kr/common/download.do?tblKey=GMN&fileId=198378819|
|18|failed|0|File is not a zip file|https://www.korea.kr/common/download.do?tblKey=GMN&fileId=198378820|
|17|success|2309||https://www.korea.kr/common/download.do?tblKey=GMN&fileId=198378873|
|16|success|2395||https://www.korea.kr/common/download.do?tblKey=GMN&fileId=198378874|
|15|success|4556||https://www.korea.kr/common/download.do?tblKey=GMN&fileId=198378716|
|14|success|114||https://www.korea.kr/common/download.do?tblKey=GMN&fileId=198379112|
|13|success|103||https://www.korea.kr/common/download.do?tblKey=GMN&fileId=198379113|
|12|success|366||https://www.korea.kr/common/download.do?tblKey=GMN&fileId=198379114|
|11|success|311||https://www.korea.kr/common/download.do?tblKey=GMN&fileId=198379115|

## ??
- ?? ??? `batch-size=10`?? ?? 10?? ???? ???.
- ?? ??? ?? PDF? ?? HTML ??(?: `invalid pdf header`)? ??? ? ??.
- ?? ?? `processing_status=failed` ? `last_error`? ????, ??? ??.
