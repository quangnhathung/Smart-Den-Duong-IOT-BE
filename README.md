# Backend đèn đường thông minh — TASK-008/009

Python3.12, FastAPI/Pydantic2/Paho2, SQLite WAL; chạy local một worker. Repo độc lập cùng cấp firmware, dùng [SRS](../firmware/docs/srs.md) và [MQTT v1](../firmware/docs/techs/mqtt.md). Đây là backend/API; [dashboard HTML/CSS/JS](../dashboard/README.md) chạy bằng VS Code Live Server. Tích hợp MQTT/Wokwi và ba node cần nghiệm thu riêng.

## Cài và kiểm tra

Linux, từ thư mục backend:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python scripts/check.py
.venv/bin/python -m smartlight.init_db --db data/smartlight.db
.venv/bin/python -m uvicorn smartlight.api:app --host 127.0.0.1 --port 8000 --workers 1
```

Windows PowerShell:

```powershell
py -3.12 -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements.txt
.venv/Scripts/python.exe scripts/check.py
.venv/Scripts/python.exe -m smartlight.init_db --db data/smartlight.db
.venv/Scripts/python.exe -m uvicorn smartlight.api:app --host 127.0.0.1 --port 8000 --workers 1
```

Khởi tạo chỉ làm khi backend dừng; script version-check/idempotent, không xóa database. Một OS lock theo database ngăn collector thứ hai; file `.db.lock` có thể còn sau stop nhưng khóa OS đã nhả, không cần xóa file. Database phải nằm ổ local; không đặt WAL trên ổ mạng. Để dừng dùng Ctrl+C; sau đó có thể restart bằng đúng lệnh trên, không xóa data. Không bật nhiều worker hoặc chia sẻ DB cho backend khác.

Dependency runtime và test được pin cùng requirements.txt cho máy demo, không nâng toolchain firmware. Cấu hình qua biến môi trường SMARTLIGHT_DB/MQTT_HOST/MQTT_PORT/ORIGINS (đầy đủ prefix trong .env.example); .env.example chỉ tham khảo, ứng dụng không tự load .env. Mặc định broker127.0.0.1:1883, CORS localhost/127.0.0.1 ở cổng5500 (Live Server) và5173. Linux `export SMARTLIGHT_DB=...`, PowerShell `$env:SMARTLIGHT_DB='...'`.

## Dùng API

Mở http://127.0.0.1:8000/docs hoặc /openapi.json để xem request/response models. Backend cung cấp API; giao diện ở repo dashboard cùng cấp. Health503 khi broker/database/queue không sẵn sàng; vẫn đọc được lịch sử nếu DB còn hoạt động. Broker không chạy sẽ có log reconnect với backoff, không làm backend xóa dữ liệu hay báo online giả.

```bash
curl http://127.0.0.1:8000/api/health
curl http://127.0.0.1:8000/api/nodes
curl 'http://127.0.0.1:8000/api/nodes/node_01/history?limit=100&offset=0'
curl -X POST http://127.0.0.1:8000/api/nodes/node_01/commands -H 'Content-Type: application/json' -d '{"mode":"MANUAL","brightness":80}'
curl http://127.0.0.1:8000/api/commands/COMMAND_ID_FROM_RESPONSE
curl http://127.0.0.1:8000/api/alerts
curl 'http://127.0.0.1:8000/api/stats?node_id=node_01'
```

PowerShell dùng `Invoke-RestMethod` (GET) và `Invoke-RestMethod -Method Post -ContentType 'application/json' -Body '{"mode":"MANUAL","brightness":80}' -Uri http://127.0.0.1:8000/api/nodes/node_01/commands`.

History/stats nhận `from` và `to` ISO8601 UTC (Z/+00:00), mặc định một giờ gần nhất; experiment_id giới hạn theo phiên (nếu không truyền from/to thì dùng toàn phiên). History mặc định100, max1000/offset0; mỗi mẫu có `continuity` để frontend không nối qua ranh giới run/session/barrier. Timestamp HTTP là UTC ISO8601; `seq`, `uptime_ms`, `deadline_uptime_ms` là **chuỗi thập phân** trên HTTP để JavaScript không mất chính xác, còn MQTT vẫn dùng JSON number.

Command202 chỉ có nghĩa đã ghi nhận, không phải node đã áp dụng. AUTO không kèm brightness; MANUAL bắt buộc số nguyên0–100. 409 nếu offline/telemetry>3s/đang chờ;422 sai input;503 broker/database/queue lỗi;404 không tìm thấy. Nếu HTTP503 do deadline worker, truy vấn pending_command_id của node trước khi gửi lại vì một thao tác đã bắt đầu có thể vừa ghi nhận xong. ACK mới xác nhận APPLIED/REJECTED; lấy applied thực từ telemetry. Backend retry một lần sau2s, TIMEOUT sau10s, không resend sau disconnect/restart.

## Phiên đo và CSV

```bash
curl 'http://127.0.0.1:8000/api/experiments?limit=100&offset=0'
curl http://127.0.0.1:8000/api/experiments/1
curl -X POST http://127.0.0.1:8000/api/experiments -H 'Content-Type: application/json' -d '{"name":"EX-02 live","configuration":{"source":"live","firmware_sha256":"FILL_ACTUAL_HASH","voltage_v":12,"rated_power_w":24},"notes":"Ghi cấu hình và lịch thao tác thực tế"}'
curl -X POST http://127.0.0.1:8000/api/experiments/1/stop
curl 'http://127.0.0.1:8000/api/stats?experiment_id=1&node_id=node_01'
curl http://127.0.0.1:8000/api/experiments/1/export.csv -o experiment-1.csv
```

Dùng ID thực từ response; chỉ một phiên mở, stop lặp lại giữ thời điểm cũ. CSV có mẫu gốc, config/notes, quality, lý do loại và đoạn năng lượng giống API. Các timestamp CSV là UTC; dữ liệu văn bản tên/ghi chú bắt đầu bằng ký tự công thức được escape để mở trong bảng tính. Ghi đúng hash/cấu hình/nhãn nguồn; giá trị FILL_ACTUAL_HASH trong ví dụ không phải bằng chứng.

Năng lượng hình thang theo uptime node, không nối boot/restart/disconnect/invalid/queue-loss hoặc gap>6s. So sánh AUTO loại MANUAL, UNKNOWN, fault/verification/recovery, tải<0.95 và chuyển vùng/mode/fault. Giữ riêng tổng năng lượng và AUTO; baseline DAY0W, TWILIGHT/NIGHT24W. saving_percent=null khi baseline0. FIRST_SAMPLE là mẫu khởi tạo, chưa có đoạn để tính.

`valid_uptime_s` dùng thời gian mô phỏng; `covered_wall_s`/`missing_wall_s`/`coverage_percent` dùng khoảng thời gian nhận UTC giữa những cặp hợp lệ. Hai clock không được chia lẫn. Tổng coverage của ba node có mẫu số tổng ba khoảng quan sát, node chưa chạy vẫn góp khoảng thiếu; có thể lọc node_01 để đánh giá demo một node. Không trình bày công suất mô hình như đo điện thật.

## Kiểm tra live trên máy dev — chưa chạy tại máy này

1. Chạy broker local từ repo firmware theo setup.md; build/copy/chạy đúng một phiên Wokwi A3, lưu hash/Serial/log MQTT theo ma trận A3-01…08. Backend ở cửa sổ riêng, không cần đổi cấu hình broker/firmware.
2. GET health trả200; nodes phải có node_01 ONLINE/FRESH sau availability+telemetry mới. Node_02/03 chưa chạy giữ UNKNOWN. Đổi cảm biến và kiểm history/fault/raw/target/applied, không đọc diagnostics làm telemetry.
3. POST MANUAL80, lưu response202/command_id; poll GET command tới APPLIED/REJECTED/TIMEOUT. Đối chiếu ACK thật và applied80 sau dimming. AUTO bỏ brightness; input sai422; lệnh thứ hai khi pending409. Không giữ lệnh MANUAL quá60s để gọi là nhận lại lệnh cũ.
4. Người vận hành chủ động stop/start riêng broker: health503 và dữ liệu STALE, node vẫn AUTO; phục hồi cần subscribe/telemetry mới. Lệnh đang chờ không replay, timeout rõ. Không dừng service khác hoặc prune Docker.
5. Restart backend giữ DB: history/commands/alerts/experiments còn, online chờ telemetry mới. OPEN_LOAD không tự đóng chỉ vì mất broker. Không tuyên bố node02/03 hoạt động khi chỉ có một node.
6. Bắt đầu phiên thực, chạy kịch bản theo SRS, stop, so CSV/API trên cùng node/phiên, kiểm đơn vị/công thức/coverage/reasons. Lưu cấu hình thực/hash/giờ thao tác/CSV/log vào firmware/docs/reports/evidence/task-008/live theo thư mục lượt riêng; không ghi đè fixture hoặc log cũ.

Test tự động dùng MQTT/clock giả và DB tạm, không mở kết nối broker. Paho transport có test với Client giả chứng minh ứng dụng tạo client mới khi mất session; điều đó chưa thay thế kiểm broker/ESP32 thật hoặc kiểm stability30phút. Log/bằng chứng tại [TASK-008](../firmware/docs/reports/evidence/task-008/README.md). TestClient của bộ Starlette hiện pin có deprecation warning HTTPX; các test vẫn chạy, không suppress warning hoặc gọi đó là live PASS.

## Bổ sung dashboard TASK-009

GET `/api/experiments` trả items/total/limit/offset (limit1–1000, mặc định100), mới nhất trước theo started_at/id; GET `/api/experiments/{id}` trả cấu hình/notes/start/end hoặc404. Cùng POST đang có: tổng12 thao tác HTTP trên11 đường dẫn OpenAPI. Không thay MQTT, schema SQLite hoặc thuật toán điện năng. CORS5500 được thêm và giữ5173. Kiểm tra hồi quy hiện68 Pytest, Ruff check/format; evidence mới tại [TASK-009](../firmware/docs/reports/evidence/task-009/README.md).


## TASK-010 — sửa đồng bộ session sau restart/reconnect

Actor đồng bộ connected/session ngay sau khi lấy job khỏi queue, trước khi xử lý event. Điều này tránh loại nhầm availability ONLINE retained vừa tới trong lúc chờ queue và để node kẹt UNCONFIRMED_BOOT sau restart backend. Vẫn cần telemetry mới đúng session/boot để mở khóa; event session cũ không được phục hồi node. Không đổi MQTT/HTTP API hoặc schema SQLite.

Bộ hồi quy hiện72 Pytest và Ruff PASS, gồm4 trường hợp actor wake-up/session mới/cũ. Lỗi có2 ca tái hiện FAIL trước sửa và được xác minh phục hồi live cùng boot sau sửa. TASK-010 đã bổ sung UI REJECTED/TIMEOUT, pending/phiên mở qua restart, broker outage/reconnect/no-replay/AUTO offline và10 mẫu sensor → dashboard; xem hồ sơ completion trong firmware/docs/reports/evidence/task-010 và techs/backend.md. Môi trường QA dùng DB riêng/API8001, không thay cổng mặc định8000 hoặc dữ liệu người dùng.
