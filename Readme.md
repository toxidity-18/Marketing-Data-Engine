# Marketing Data Engine

A comprehensive data processing and analytics tool for digital marketing agencies. Built with Flask API and Streamlit frontend.

## 🎯 Purpose

This tool solves real-world pain points for marketing agencies:
- **Format Chaos**: Handle different export formats from Google Ads, Meta, TikTok, LinkedIn
- **Data Quality Issues**: Detect anomalies, missing values, and inconsistencies
- **Multi-Platform Aggregation**: Merge data from multiple ad platforms into unified views
- **Reporting Automation**: Generate client-ready reports in minutes

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    STREAMLIT FRONTEND (8501)                │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐       │
│  │Dashboard │ │  Upload  │ │Analytics │ │ Insights │       │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘       │
└─────────────────────────┬───────────────────────────────────┘
                          │ HTTP Requests
┌─────────────────────────▼───────────────────────────────────┐
│                      FLASK API (5001)                       │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐       │
│  │Ingestion │ │Normalize │ │ Quality  │ │ Insights │       │
│  │  Service │ │  Service │ │ Service  │ │ Service  │       │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘       │
└─────────────────────────────────────────────────────────────┘
```

## 📁 Project Structure

```
marketing-data-engine/
├── api/
│   ├── app.py                    # Flask API main application
│   └── services/
│       ├── data_ingestion.py     # File upload & parsing
│       ├── data_normalization.py # Column mapping & standardization
│       ├── data_quality.py       # Quality checks & anomaly detection
│       ├── data_merger.py        # Multi-platform data merging
│       ├── ai_insights.py        # AI-powered insights
│       └── report_generator.py   # Excel/PDF report generation
├── frontend/
│   └── app.py                    # Streamlit application
├── data/
│   ├── sample/                   # Sample data files
│   ├── uploads/                  # Uploaded files
│   └── reports/                  # Generated reports
├── venv/                         # Python virtual environment
├── requirements.txt              # Python dependencies
└── run.sh                        # Startup script
```

## 🚀 Quick Start

### 1. Activate Virtual Environment
```bash
cd /home/z/my-project/marketing-data-engine
source venv/bin/activate
```

### 2. Start the API Server
```bash
python api/app.py
```
API will run on http://localhost:5001

### 3. Start the Streamlit Frontend
```bash
streamlit run frontend/app.py --server.port 8501
```
Frontend will run on http://localhost:8501

### 4. Or Use the Startup Script
```bash
chmod +x run.sh
./run.sh
```

## 📡 API Endpoints

### Data Ingestion
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/upload` | POST | Upload a single data file |
| `/api/upload-multiple` | POST | Upload multiple files |
| `/api/sample-data` | GET | Generate sample data for testing |

### Data Processing
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/normalize/<data_id>` | POST | Normalize data |
| `/api/quality/check/<data_id>` | GET | Run quality checks |
| `/api/quality/anomalies/<data_id>` | GET | Detect anomalies |

### Analytics
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/aggregate/date/<data_id>` | GET | Aggregate by date |
| `/api/aggregate/campaign/<data_id>` | GET | Aggregate by campaign |
| `/api/compare/platforms/<data_id>` | GET | Compare platforms |

### AI & Reports
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/insights/<data_id>` | POST | Generate AI insights |
| `/api/report/excel/<data_id>` | POST | Generate Excel report |
| `/api/report/pdf/<data_id>` | POST | Generate PDF report |
| `/api/export/csv/<data_id>` | GET | Export as CSV |

## 🔧 Features

### 1. Smart Data Ingestion
- Auto-detects ad platform (Google Ads, Meta, TikTok, LinkedIn)
- Supports CSV, Excel, and JSON formats
- Handles multiple encodings and date formats

### 2. Data Normalization
- Standard column mapping across platforms
- Date format standardization
- Currency conversion
- Deduplication
- Derived metric calculation (CTR, CPC, CPA, ROAS)

### 3. Data Quality Checks
- Completeness analysis
- Uniqueness validation
- Value validity checks
- Consistency verification
- Anomaly detection (Z-score & IQR methods)

### 4. Multi-Platform Merging
- Combine data from multiple platforms
- Aggregate by date, campaign, or platform
- Cross-platform performance comparison

### 5. AI-Powered Insights
- Performance recommendations
- Budget optimization suggestions
- Natural language queries

### 6. Report Generation
- Multi-sheet Excel reports
- Client-ready PDF reports
- Customizable templates

## 📊 Supported Platforms

| Platform | Identifiers | Key Metrics |
|----------|-------------|-------------|
| Google Ads | Campaign, Ad Group, Keyword | Clicks, Impressions, Cost, Conversions |
| Meta Ads | Campaign, Ad Set, Ad Name | Reach, Frequency, Spend, CTR |
| TikTok Ads | Campaign, Adgroup, Ad | Spend, Impressions, Conversions |
| LinkedIn Ads | Campaign, Campaign Group | Impressions, Clicks, Spend |

## 🛠️ Technologies

- **Backend**: Python 3.12, Flask
- **Frontend**: Streamlit
- **Data Processing**: Pandas, NumPy
- **Analytics**: SciPy, Scikit-learn
- **Visualization**: Plotly
- **Reports**: OpenPyXL, HTML/CSS

## 📝 Sample Data

Sample data files are provided in `data/sample/`:
- `google_ads_sample.csv` - Google Ads export format
- `meta_ads_sample.csv` - Meta Ads export format

## 🔒 Security Notes

- Files are stored locally in `data/uploads/`
- No external API keys required for core functionality
- Session data stored in memory (use database in production)

---

Built for **Belva Mar-Tech Agency** 🚀
