# AI Usage Disclosure

## Công cụ

- OpenAI Codex.

## Phạm vi hỗ trợ

Tôi sử dụng AI để:

- đọc và giải thích cấu trúc repository, hướng dẫn lab, rubric và checklist nộp bài;
- giải thích các khái niệm Delta Lake, Iceberg, medallion, compaction, Z-order,
  time travel, maintenance, vector lifecycle và provenance;
- đối chiếu output đã chạy với các ngưỡng chấm điểm;
- đề xuất và biên tập Markdown giải thích kết quả cho tám notebook;
- chẩn đoán lỗi Windows file lock do Jupyter kernels giữ SQLite catalog;
- hỗ trợ cấu trúc, lập luận, sơ đồ, failure modes, cost model và MVP cho bonus
  `submission/bonus/ARCHITECTURE.md`;
- hướng dẫn kiểm tra notebook, screenshots, Git status và quy trình nộp bài.

## Phần tôi tự thực hiện và xác minh

- Tôi tự chạy tám notebook trong môi trường local và lưu output thực tế.
- Tôi tự kiểm tra các metric, đọc lại phần giải thích và chịu trách nhiệm về kết luận.
- Tôi tự chụp screenshots trực tiếp từ Jupyter; AI không tạo screenshots dùng để nộp.
- Tôi không dùng AI để tạo output giả, sửa số đo, bỏ assertion hoặc hạ ngưỡng PASS.
- Tôi tự rà soát architecture brief và chịu trách nhiệm bảo vệ các giả định, quyết định
  và phép tính trong design review.

## Giới hạn

Các số liệu benchmark phụ thuộc máy chạy. Các đơn giá cloud trong architecture brief
là giả định planning có dẫn nguồn và phải được kiểm tra lại theo region tại thời điểm
triển khai. Nội dung AI đề xuất không thay thế việc tự kiểm chứng code, output, bảo mật
hoặc yêu cầu pháp lý.
