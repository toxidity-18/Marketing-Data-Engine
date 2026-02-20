# Marketing Data Engine – Complete Documentation

## 📖 Introduction
**Marketing Data Engine** is a powerful data processing and analytics platform designed for digital marketing agencies. It automates the ingestion, normalization, quality control, and reporting of marketing data from multiple ad platforms (Google Ads, Meta, TikTok, LinkedIn). The tool provides a unified view of campaign performance, AI‑powered insights, and client‑ready reports – saving hours of manual work and enabling data‑driven decisions.

This documentation covers the architecture, installation, usage, API reference, and internal services of the Marketing Data Engine.

---

## ✨ Features
- **Multi‑Platform Ingestion** – Upload CSV, Excel, or JSON files from any major ad platform.
- **Smart Column Mapping** – Automatically maps platform‑specific column names to a standard schema.
- **Data Normalization** – Standardizes date formats, currencies, and removes duplicates.
- **Quality Checks** – Detects missing values, outliers, and inconsistencies; grades data quality.
- **Anomaly Detection** – Flags unusual spend, clicks, or conversions using Z‑score and IQR methods.
- **Cross‑Platform Aggregation** – Roll up data by date, campaign, or platform for comparison.
- **AI‑Powered Insights** – Ask natural language questions and get actionable recommendations.
- **Report Generation** – Export clean Excel reports or polished HTML/PDF documents.
- **Interactive Dashboard** – Streamlit frontend with real‑time visualizations and filtering.

---

## 🛠️ Technology Stack
| Component       | Technology                         |
|-----------------|------------------------------------|
| Backend API     | Python, Flask, Flask‑CORS          |
| Frontend        | Streamlit                          |
| Data Processing | Pandas, NumPy, SciPy                |
| Visualization   | Plotly                              |
| Reporting       | OpenPyXL (Excel), HTML/CSS (PDF)   |
| Deployment      | Render (API), Streamlit Cloud (UI)  |

---

## 📁 Project Structure
```
marketing-data-engine/
├── api/
│   ├── app.py                          # Flask API main application
│   └── services/
│       ├── data_ingestion.py           # File upload & platform detection
│       ├── data_normalization.py       # Column mapping & standardization
│       ├── data_quality.py             # Quality checks & anomaly detection
│       ├── data_merger.py              # Multi‑platform data merging & aggregation
│       ├── ai_insights.py               # AI‑powered insights (rule‑based + external AI)
│       └── report_generator.py          # Excel/PDF report generation
├── frontend/
│   └── app.py                           # Streamlit frontend application
├── data/
│   ├── sample/                          # Sample data files (optional)
│   ├── uploads/                         # Uploaded files (created at runtime)
│   └── reports/                          # Generated reports (created at runtime)
├── requirements.txt                      # Python dependencies
├── run.sh                                # Startup script (optional)
└── README.md                              # Project overview (this file)
```

---

## ⚙️ Installation & Setup

### Prerequisites
- Python 3.12 or higher
- pip (Python package manager)
- Git (optional)

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/marketing-data-engine.git
cd marketing-data-engine
```

### 2. Set Up Virtual Environment
```bash
python -m venv venv
source venv/bin/activate      # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Run the Backend API
```bash
python api/app.py
```
The API will start at `http://localhost:5001`.

### 5. Run the Frontend (in a separate terminal)
```bash
streamlit run frontend/app.py
```
The frontend will open at `http://localhost:8501`.

### 6. (Optional) Run with Docker
A `docker-compose.yml` is provided (see [Deployment](#deployment)) to run both services together.

---

## 🚀 Usage Guide

### Data Upload
- **Single File Upload** – Upload one CSV, Excel, or JSON file. The system detects the platform (Google Ads, Meta, etc.) and shows a preview.
- **Multi‑File Upload** – Upload multiple files from different platforms; they are merged into a unified dataset.

### Data Processing
- **Normalization** – Standardize column names, dates, currencies, and remove duplicates. Derived metrics (CTR, CPC, CPA, ROAS) are automatically calculated.
- **Quality Check** – Run a comprehensive quality report (completeness, uniqueness, validity, consistency) and get a quality grade (A+ to F).
- **Anomaly Detection** – Detect statistical outliers (Z‑score, IQR) and performance issues (e.g., campaigns with high CPA, low CTR, budget drainage).

### Analytics
- **By Date** – Aggregate metrics daily, weekly, or monthly; view spend trends and compare clicks/impressions.
- **By Campaign** – See campaign‑level performance, sorted by spend or ROAS.
- **By Platform** – Compare spend distribution and efficiency metrics across platforms.

### AI Insights
- Ask natural language questions like:
  - *“Why did ROAS drop last week?”*
  - *“Which campaigns have the highest CPA?”*
  - *“Recommend optimizations for underperforming ads.”*
- The system provides answers and actionable recommendations.

### Reports
- **Excel Report** – Multi‑sheet workbook with summary, raw data, campaign/platform summaries, daily trends, and insights.
- **PDF/HTML Report** – Client‑ready report with metrics, charts, and insights (print to PDF from browser).
- **CSV Export** – Download the current dataset as a CSV file.

---

## 📡 API Reference

All API endpoints are prefixed with `/api`. The base URL is `http://localhost:5001/api` (or your deployed URL).

### Health Check
| Method | Endpoint       | Description               |
|--------|----------------|---------------------------|
| GET    | `/health`      | Returns service status.   |

### Data Ingestion
| Method | Endpoint               | Description                                      |
|--------|------------------------|--------------------------------------------------|
| POST   | `/upload`              | Upload a single file (multipart/form‑data).      |
| POST   | `/upload-multiple`     | Upload multiple files (multipart/form‑data).     |
| GET    | `/sample-data`         | Generate sample marketing data for testing.      |

### Data Retrieval
| Method | Endpoint                 | Description                                |
|--------|--------------------------|--------------------------------------------|
| GET    | `/data/<data_id>`        | Get paginated data (rows 1‑100 by default).|
| GET    | `/data/<data_id>/stats`  | Get statistical summary of the dataset.    |

### Data Normalization
| Method | Endpoint                   | Description                                 |
|--------|----------------------------|---------------------------------------------|
| POST   | `/normalize/<data_id>`     | Normalize data (column mapping, date parsing, currency conversion). |

### Data Quality
| Method | Endpoint                         | Description                                      |
|--------|----------------------------------|--------------------------------------------------|
| GET    | `/quality/check/<data_id>`       | Run comprehensive quality checks.                |
| GET    | `/quality/anomalies/<data_id>`   | Detect anomalies and performance issues.         |

### Data Merging & Aggregation
| Method | Endpoint                             | Description                                      |
|--------|--------------------------------------|--------------------------------------------------|
| GET    | `/aggregate/date/<data_id>`          | Aggregate data by date (daily/weekly/monthly).   |
| GET    | `/aggregate/campaign/<data_id>`      | Aggregate data by campaign.                      |
| GET    | `/compare/platforms/<data_id>`       | Compare performance across platforms.            |

### AI Insights
| Method | Endpoint                 | Description                                      |
|--------|--------------------------|--------------------------------------------------|
| POST   | `/insights/<data_id>`    | Generate AI insights (optional question/focus).  |

### Report Generation
| Method | Endpoint                     | Description                                      |
|--------|------------------------------|--------------------------------------------------|
| POST   | `/report/excel/<data_id>`    | Generate an Excel report.                        |
| POST   | `/report/pdf/<data_id>`      | Generate a PDF report (HTML version).            |
| GET    | `/export/csv/<data_id>`      | Export data as CSV.                              |

### File Download
| Method | Endpoint                 | Description                                      |
|--------|--------------------------|--------------------------------------------------|
| GET    | `/download/<filename>`   | Download a generated report file.                |

---

## 🧩 Services Overview

### 1. `DataIngestionService`
- **Purpose:** Reads CSV, Excel, and JSON files from disk or buffer.
- **Key methods:**
  - `ingest_csv(file_path)`
  - `ingest_excel(file_path, sheet_name)`
  - `ingest_json(file_path)`
  - `ingest_from_buffer(buffer, filename)` – used by Streamlit frontend.
- **Features:**
  - Platform detection via column name matching.
  - Extracts basic statistics (rows, columns, missing values, duplicates).
  - Saves uploaded files to `UPLOAD_FOLDER`.

### 2. `DataNormalizationService`
- **Purpose:** Transforms raw data into a standard schema.
- **Key methods:**
  - `normalize(df, platform, currency, custom_mappings)`
- **Steps performed:**
  - Column mapping (using `STANDARD_SCHEMA` dictionary).
  - Date parsing (handles multiple formats).
  - Currency conversion (if `currency` column exists).
  - Deduplication.
  - Calculation of derived metrics (CTR, CPC, CPM, CPA, ROAS).
  - Missing value handling (fill with 0 or empty string).

### 3. `DataQualityService`
- **Purpose:** Evaluates data quality and detects anomalies.
- **Key methods:**
  - `check_quality(df)` – runs completeness, uniqueness, validity, consistency, timeliness checks; returns a quality report with score and grade.
  - `detect_anomalies(df, columns)` – uses Z‑score and IQR to find outliers.
  - `detect_performance_anomalies(df)` – flags campaigns with high CPA, low CTR, budget drainage, negative ROAS.

### 4. `DataMergerService`
- **Purpose:** Combines multiple datasets and performs aggregations.
- **Key methods:**
  - `merge_datasets(datasets, platform_names, merge_strategy)` – appends or joins datasets.
  - `aggregate_by_date(df, date_granularity)` – groups by date period and sums metrics.
  - `aggregate_by_campaign(df, include_platform_breakdown)` – campaign‑level rollup.
  - `compare_platforms(df)` – platform‑level statistics and rankings.
  - `create_unified_report(datasets)` – generates a cross‑platform summary.

### 5. `AIInsightsService`
- **Purpose:** Provides intelligent analysis and recommendations.
- **Key methods:**
  - `generate_insights(df, question, focus_area)` – returns a dictionary with `ai_insights`, `recommendations`, and optional `answer`.
- **Fallback mechanism:** If external AI API is not available, a rule‑based engine generates insights based on metrics and thresholds.
- **Question answering:** Supports natural language questions about ROAS, CPA, CTR, spend, and overall performance.

### 6. `ReportGeneratorService`
- **Purpose:** Creates Excel, HTML, and CSV reports.
- **Key methods:**
  - `generate_excel_report(df, report_name, insights)` – writes multiple sheets (summary, raw data, campaign/platform summaries, daily trend, insights).
  - `generate_pdf_report(df, report_name, client_name, insights)` – creates an HTML file that can be printed to PDF.
  - `generate_csv_export(df)` – exports the DataFrame as a CSV file.
- **HTML report:** Styled with CSS, includes key metrics, platform breakdown, and AI insights.

---

## 🔧 Configuration

### Environment Variables
- `API_URL` – Used by the frontend to connect to the backend. Defaults to `http://localhost:5001/api`.
- `AI_API_BASE` – (Optional) URL for external AI service (e.g., `http://localhost:3000/api`). If not set, rule‑based insights are used.

### File Storage
- `UPLOAD_FOLDER` – `data/uploads/` – stores uploaded files.
- `REPORT_FOLDER` – `data/reports/` – stores generated reports.
These folders are created automatically if they don’t exist.

### Allowed File Extensions
- CSV, Excel (`.xlsx`, `.xls`), JSON – defined in `ALLOWED_EXTENSIONS`.

---

## 🌐 Deployment

### Deploy on Render (Backend API)
1. Push your code to a GitHub repository.
2. Create a new **Web Service** on Render.
3. Connect your repository and set:
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `gunicorn api.app:app`
4. The API will be available at `https://your-api.onrender.com`.

### Deploy on Streamlit Community Cloud (Frontend)
1. Go to [Streamlit Community Cloud](https://streamlit.io/cloud).
2. Click **New app** and select your repository.
3. Set **Main file path** to `frontend/app.py`.
4. Under **Secrets**, add `API_URL` with value `https://your-api.onrender.com/api`.
5. Deploy – your frontend will be live at `https://your-app.streamlit.app`.

### Docker Deployment (Optional)
A `docker-compose.yml` can be used to run both services locally or on any Docker host:
```yaml
version: '3'
services:
  api:
    build: ./api
    ports:
      - "5001:5001"
    environment:
      - FLASK_ENV=production
    volumes:
      - ./data:/app/data

  frontend:
    build: ./frontend
    ports:
      - "8501:8501"
    environment:
      - API_URL=http://api:5001/api
    depends_on:
      - api
```

---

## 🤝 Contributing
Contributions are welcome! Please open an issue or pull request on GitHub. For major changes, discuss them first in an issue.

---


---

## 🙏 Acknowledgements
Built for **Belva Mar‑Tech Agency** to streamline marketing data workflows. Special thanks to all contributors and open‑source libraries that made this project possible.

---

**Marketing Data Engine v1.0** – *Turn data chaos into clarity.*