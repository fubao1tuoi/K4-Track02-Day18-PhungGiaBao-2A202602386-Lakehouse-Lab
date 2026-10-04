# Reflection

Anti-pattern tôi chọn là ghi dữ liệu streaming thành quá nhiều file nhỏ nhưng thiếu
lịch maintenance. Hệ thống LLM observability dễ gặp vấn đề này vì nhiều producer ghi
log đồng thời và dashboard cần dữ liệu mới liên tục. Micro-batch quá ngắn khiến query
phải mở nhiều file, catalog xử lý nhiều metadata và chi phí request tăng.

Trong NB2, 200 file trước OPTIMIZE làm truy vấn chậm hơn; compaction và Z-order giảm
số file và cải thiện pruning theo `user_id`. NB6 cho thấy VACUUM, orphan removal,
checkpoint và snapshot expiry giải quyết các loại rác khác nhau. File chưa từng commit
có thể không được Delta VACUUM nhìn thấy, còn Iceberg snapshot expiry chưa chắc xóa
file vật lý.

Tôi sẽ phòng tránh bằng target file size, lịch OPTIMIZE theo file count, age guard khi
dọn orphan, retention phù hợp và kiểm tra row count trước/sau. Tôi dùng OpenAI Codex
để giải thích code, đối chiếu rubric và hỗ trợ biên tập; chi tiết tại
[AI_USAGE.md](AI_USAGE.md). Tôi tự chạy notebook, kiểm tra output và chụp bằng chứng.
