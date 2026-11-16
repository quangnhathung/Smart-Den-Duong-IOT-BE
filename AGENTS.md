# Backend đèn đường thông minh

Đọc và tuân thủ nguồn convention chung tại [firmware/AGENTS.md](../firmware/AGENTS.md) và [development.md](../firmware/docs/techs/development.md), SRS, task/reports và docs/techs liên quan trong repo firmware. Không sao chép bộ quy tắc chung vào đây.

Repo này chứa Python backend, không phải firmware Arduino. Python dùng 4 dấu cách, snake_case cho hàm/biến, PascalCase cho lớp, UPPER_SNAKE_CASE cho hằng; giữ nguyên tên MQTT/JSON. Chạy Ruff và Pytest trong .venv. Chỉ worker nghiệp vụ ghi SQLite; MQTT callback không đụng database. Hợp đồng và cách chạy ở README và tài liệu kỹ thuật chung.

Mã task/báo cáo/checklist tiếp tục dùng nguồn chung tại firmware/docs/reports; không tạo lịch sử task cạnh tranh trong repo backend. Git backend riêng, không commit/push hoặc đổi remote khi chưa được giao. Với task cross-repo, giữ nguyên công việc đã có trong cả hai repo và chỉ sửa phạm vi được giao.
