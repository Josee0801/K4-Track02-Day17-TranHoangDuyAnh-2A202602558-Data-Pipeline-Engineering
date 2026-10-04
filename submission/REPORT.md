# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Trần Hoàng Duy Anh / 2A202602558
**Repo:** https://github.com/Josee0801/K4-Track02-Day17-TranHoangDuyAnh-2A202602558-Data-Pipeline-Engineering
**Commit bài nộp:**
**AI đã dùng và phạm vi hỗ trợ:** GitHub Copilot hỗ trợ chạy kiểm tra và soạn báo cáo.
**Nguồn tham khảo khác:** README và tài liệu hướng dẫn trong repo; không dùng nguồn ngoài.

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | 24 hàng cho 12 ticket; T-91 có nhiều trạng thái. | Feature ngày 08-12 thiếu event của u05; full recompute lệch checksum. | T-97 vẫn có dữ liệu trong Silver và Gold sau ngày xoá. |
| **Nguyên nhân gốc** | Silver append thay vì upsert; không bảo vệ trạng thái mới trước batch cũ. | Lookback bằng 0 nên không tính lại event day khi dữ liệu đến muộn. | Staging chỉ lấy khóa từ `after`, vốn null khi Debezium gửi delete. |
| **Cách sửa** | `pipeline/silver.py`: MERGE theo `ticket_id`, chỉ update khi `_lsn` mới hơn. | `pipeline/config.py`: đặt lookback 3 ngày theo P99 đo từ Bronze. | `pipeline/staging.py`: lấy khóa lần lượt từ `after`, `before`, rồi Kafka key. |
| **Khái niệm trên slide** | Ghi có khóa, idempotency và thứ tự CDC theo LSN. | Event time, lateness và lookback dựa trên phân vị đo được. | Debezium `op='d'`, tombstone và lan truyền thao tác xoá. |

## 2. Các con số

- P99 lateness từ 43 bản ghi Bronze: **3.00 ngày**; `LOOKBACK_DAYS = 3`.
- `submission/checksums.txt`: **PASS**; Gold checksum: `39e115c510ecdf526800eac227158a4f`.
- dbt build: **PASS=19**; parity: **PARITY** (`silver_tickets` `3c15dfd43701`, `gold_feature_daily` `8630e04a61d1`).

## 3. Lựa chọn công cụ / kỹ thuật

- MERGE theo khóa giữ đúng một ticket và LSN mới nhất; overwrite-partition tính lại chính xác các ngày trong lookback.
- Tombstone giữ dấu vết xóa để batch cũ không làm ticket sống lại, đồng thời xóa PII khỏi trạng thái hiện tại.
- Snapshot “as of” dựng từ Bronze giúp tái lập lịch sử và giữ phiên bản; xóa dữ liệu theo yêu cầu vẫn phải ưu tiên, purge PII khỏi snapshot/cache/bản sao theo chính sách và lưu audit không chứa PII.
- DuckDB chạy local, zero-key và đủ cho seed nhỏ; dbt kiểm chứng mô hình SQL/parity, còn Spark chỉ cần khi quy mô và tải phân tán đòi hỏi.

## 4. Hai câu hỏi suy ngẫm

1. Quyền xóa phải thắng tính bất biến nếu snapshot còn PII: xóa hoặc mã hóa hủy khóa cho nội dung cá nhân trong mọi snapshot, cache và bản sao theo chính sách lưu trữ; giữ audit tối thiểu không chứa PII, rồi tạo lại các snapshot bị ảnh hưởng và ghi nhận ngoại lệ lịch sử.
2. Đặt kiểm tra PII ở cổng Bronze→Silver: dùng nhận diện thực thể tên/người bên cạnh regex, mask trước khi ghi Silver và quarantine bản ghi không chắc chắn; đo precision/recall trên bộ dữ liệu gán nhãn, đồng thời kiểm tra tự động rằng tên/email/số điện thoại giả lập không còn trong Silver/Gold.

## 5. Output thực tế (PowerShell)

### Verify

```text
PS> .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
   [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
   [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
   [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
   [OK ] Silver  silver_tickets has exactly one row per ticket_id
   [OK ] Silver  T-91 shows its latest state: high / closed / bug
   [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
   [OK ] Silver  no email / phone number survives past Bronze
   [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
   [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
   [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
   [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
   [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
   [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
   [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
   [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
   [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
   [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
   [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt
```

### Pytest

```text
PS> .\.venv\Scripts\python.exe -m pytest -q
..................................                                       [100%]
```

### Rerun

```text
PS> .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums
```

### Lateness

```text
PS> .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3
```

### dbt build

Windows long-path giới hạn cài dependency trong `.venv` của repo; dbt 1.12.5 và dbt-duckdb 1.11.0 được cài trong `C:\dbtvenv` để chạy cùng project.

```text
PS> $env:DO_NOT_TRACK = '1'; Push-Location dbt_project; try { C:\dbtvenv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17 } finally { Pop-Location }
05:49:11  Running with dbt=1.12.5
05:49:11  Registered adapter: duckdb=1.11.0
05:49:12  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
05:49:12
05:49:12  Concurrency: 1 threads (target='dev')
05:49:12
05:49:13  1 of 19 START sql view model main.stg_events ................................... [RUN]
05:49:13  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.09s]
05:49:13  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
05:49:13  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.04s]
05:49:13  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
05:49:13  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.13s]
05:49:13  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
05:49:13  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.13s]
05:49:13  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
05:49:13  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.16s]
05:49:13  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
05:49:13  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.05s]
05:49:13  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
05:49:13  6 of 19 PASS not_null_silver_events_user_id ................................... [PASS in 0.03s]
05:49:13  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
05:49:13  7 of 19 PASS unique_silver_events_event_id ................................... [PASS in 0.03s]
05:49:13  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
05:49:13  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.04s]
05:49:13  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
05:49:13  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.10s]
05:49:13  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
05:49:13  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.03s]
05:49:13  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
05:49:13  12 of 19 PASS not_null_silver_tickets__lsn ................................... [PASS in 0.02s]
05:49:13  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
05:49:13  13 of 19 PASS not_null_silver_tickets_is_deleted .............................. [PASS in 0.02s]
05:49:13  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
05:49:13  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.02s]
05:49:13  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
05:49:13  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.02s]
05:49:13  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
05:49:13  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
05:49:13  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.05s]
05:49:13  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
05:49:13  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.04s]
05:49:13  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
05:49:14  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.04s]
05:49:14  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
05:49:14  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.08s]
05:49:14  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
05:49:14  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.05s]
05:49:14  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
05:49:14  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.04s]
05:49:14  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
05:49:14  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.04s]
05:49:14  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.37s]
05:49:14  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
05:49:14  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.03s]
05:49:14  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
05:49:14  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
05:49:14  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
05:49:14  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]
05:49:14
05:49:14  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.60 seconds (1.60s).
05:49:14
05:49:14  Completed successfully
05:49:14
05:49:14  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
```

### Parity

```text
PS> .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
   [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
   [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
