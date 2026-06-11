# Government AI Agent Platform

Nền tảng AI hỗ trợ phân tích dữ liệu kinh tế/chính phủ, kết hợp **data pipeline**, **BigQuery warehouse**, **dashboard web** và **AI Agent** để biến dữ liệu thô thành insight dễ hiểu.

## Mục tiêu dự án

Dự án được xây dựng để hỗ trợ người dùng tra cứu, so sánh và phân tích các chỉ số kinh tế giữa các quốc gia thông qua giao diện web và trợ lý AI.

Một số chức năng chính:

* Thu thập và xử lý dữ liệu kinh tế từ nhiều nguồn công khai.
* Chuẩn hóa dữ liệu vào BigQuery.
* Hiển thị dữ liệu qua dashboard trực quan.
* So sánh quốc gia, chỉ số và xu hướng theo thời gian.
* Phát hiện bất thường trong dữ liệu kinh tế.
* Phân nhóm quốc gia theo đặc điểm cấu trúc.
* Hỏi đáp dữ liệu bằng AI Agent.

## Kiến trúc tổng quan

```text
User
  ↓
Next.js Dashboard
  ↓
Backend API
  ↓
AI Agent Service
  ↓
BigQuery Warehouse
  ↑
Data Pipeline
  ↑
Raw Public Datasets
```

## Tech stack

| Thành phần    | Công nghệ                                                       |
| ------------- | --------------------------------------------------------------- |
| Frontend      | Next.js, React, TypeScript, Tailwind CSS, React Query, Recharts |
| Backend       | NestJS, BigQuery Client                                         |
| AI Agent      | Python, Gemini, BigQuery tools                                  |
| Data Pipeline | Python, PySpark, pandas                                         |
| Cloud         | Google Cloud Run, BigQuery, GCS, Secret Manager                 |

## Cấu trúc chính

```text
Government-Ai-Agent-Platform/
├── fe/                         # Frontend dashboard
├── services/
│   ├── data-pipeline/          # Pipeline xử lý dữ liệu
│   └── query-agent/            # AI Agent / query service
├── infra/
│   └── gcp/                    # Cấu hình deploy Google Cloud
└── README.md
```

## Chức năng dashboard

Dashboard hiện hướng tới các nhóm màn hình chính:

* **Tổng quan**: xem nhanh trạng thái và dữ liệu nổi bật.
* **Quốc gia**: danh sách và hồ sơ từng quốc gia.
* **So sánh**: so sánh nhiều quốc gia/chỉ số.
* **Nhóm cấu trúc**: phân cụm quốc gia theo đặc điểm kinh tế.
* **Bất thường**: phát hiện chỉ số có biến động đáng chú ý.
* **Danh mục chỉ số**: tra cứu metadata chỉ số.
* **Trợ lý dữ liệu AI**: hỏi đáp bằng ngôn ngữ tự nhiên.

## Cách chạy Frontend

```bash
cd fe
npm install
npm run dev
```

Tạo file `.env.local` nếu cần:

```env
NEXT_PUBLIC_API_URL=http://localhost:3002
```

Mở trình duyệt tại:

```text
http://localhost:3000
```

## Cách chạy Data Pipeline

```bash
cd services/data-pipeline
python -m venv .venv
```

Kích hoạt môi trường ảo:

```bash
source .venv/bin/activate
```

Trên Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Cài dependencies:

```bash
pip install -e ".[dev]"
```

Chạy test:

```bash
pytest
```

## Cấu hình Cloud / BigQuery

Một số cấu hình mẫu nằm trong:

```text
infra/gcp/cloud-run/
```

Các file quan trọng:

```text
deploy.env.example
backend.env.example
ai-agent.env.example
secrets.env.example
```

Không commit file `.env` thật hoặc secret lên GitHub.

## Một số biến môi trường quan trọng

```env
BIGQUERY_PROJECT_ID=your-gcp-project-id
BIGQUERY_LOCATION=asia-southeast1
BIGQUERY_GOLD_DATASET=gov_ai_gold
BIGQUERY_ANALYTICS_DATASET=gov_ai_analytics

AI_AGENT_BASE_URL=http://localhost:8000
GEMINI_API_KEY=your-gemini-api-key
```

