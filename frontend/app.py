"""
Marketing Data Engine - Streamlit Frontend
Main application with navigation and shared state
"""

import streamlit as st
import pandas as pd
import numpy as np
import requests
import io
import json
from datetime import datetime
import os

# Page config
st.set_page_config(
    page_title="Marketing Data Engine",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# API Configuration
API_BASE_URL = os.environ.get('API_URL', 'http://localhost:5001/api')

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1e40af;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #64748b;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%);
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: #1e40af;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #64748b;
        text-transform: uppercase;
    }
    .stAlert {
        border-radius: 8px;
    }
    .uploadedFile {
        border: 2px dashed #3b82f6;
        border-radius: 8px;
        padding: 2rem;
    }
    div[data-testid="metric-container"] {
        background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%);
        border-radius: 12px;
        padding: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'data_id' not in st.session_state:
    st.session_state.data_id = None
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'current_data' not in st.session_state:
    st.session_state.current_data = None
if 'normalization_done' not in st.session_state:
    st.session_state.normalization_done = False
if 'quality_report' not in st.session_state:
    st.session_state.quality_report = None
if 'insights' not in st.session_state:
    st.session_state.insights = None

# Additional session state for persistent upload results
if 'upload_success' not in st.session_state:
    st.session_state.upload_success = False
if 'upload_metadata' not in st.session_state:
    st.session_state.upload_metadata = None
if 'upload_preview' not in st.session_state:
    st.session_state.upload_preview = None

if 'multi_success' not in st.session_state:
    st.session_state.multi_success = False
if 'multi_metadata' not in st.session_state:
    st.session_state.multi_metadata = None
if 'multi_preview' not in st.session_state:
    st.session_state.multi_preview = None

# For normalization results
if 'normalization_report' not in st.session_state:
    st.session_state.normalization_report = None
if 'normalization_preview' not in st.session_state:
    st.session_state.normalization_preview = None


def api_call(endpoint, method='GET', data=None, files=None):
    """Make API call to the Flask backend."""
    url = f"{API_BASE_URL}{endpoint}"
    try:
        if method == 'GET':
            response = requests.get(url)
        elif method == 'POST':
            if files:
                response = requests.post(url, files=files)
            else:
                response = requests.post(url, json=data)
        return response.json()
    except requests.exceptions.ConnectionError:
        st.error("⚠️ Cannot connect to API server. Make sure the Flask API is running on port 5001.")
        return None
    except Exception as e:
        st.error(f"API Error: {str(e)}")
        return None


def format_number(num, prefix='', suffix=''):
    """Format numbers with commas and optional prefix/suffix."""
    if pd.isna(num) or num is None:
        return 'N/A'
    if isinstance(num, (int, float)):
        if num >= 1000000:
            return f"{prefix}{num/1000000:.1f}M{suffix}"
        elif num >= 1000:
            return f"{prefix}{num/1000:.1f}K{suffix}"
        else:
            return f"{prefix}{num:,.2f}{suffix}" if isinstance(num, float) else f"{prefix}{num:,}{suffix}"
    return str(num)


# Sidebar
with st.sidebar:
    st.markdown("### 📊 Marketing Data Engine")
    st.markdown("---")
    
    # Navigation
    st.markdown("### Navigation")
    page = st.radio(
        "Select a page:",
        ["🏠 Dashboard", "📤 Data Upload", "🔧 Data Processing", "📈 Analytics", "🤖 AI Insights", "📄 Reports"],
        label_visibility="collapsed"
    )
    
    st.markdown("---")
    
    # Data Status
    st.markdown("### Data Status")
    if st.session_state.data_id:
        st.success(f"✅ Data Loaded")
        st.caption(f"ID: `{st.session_state.data_id[:20]}...`")
        if st.session_state.normalization_done:
            st.info("🔄 Normalized")
        if st.session_state.quality_report:
            grade = st.session_state.quality_report.get('grade', 'N/A')
            st.info(f"📊 Quality: {grade}")
    else:
        st.warning("⚠️ No data loaded")
    
    st.markdown("---")
    
    # Quick Actions
    st.markdown("### Quick Actions")
    if st.button("🔄 Reset Session", use_container_width=True):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()
    
    if st.button("📊 Load Sample Data", use_container_width=True):
        with st.spinner("Loading sample data..."):
            result = api_call('/sample-data', method='GET')
            if result and result.get('success'):
                st.session_state.data_id = result['data_id']
                st.session_state.data_loaded = True
                st.session_state.current_data = pd.DataFrame(result['preview'])
                # Clear upload flags
                st.session_state.upload_success = False
                st.session_state.multi_success = False
                st.success(f"✅ Loaded {result['total_rows']} rows of sample data!")
                st.rerun()


# Main Content
st.markdown('<p class="main-header">Marketing Data Engine</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Unified data processing for digital marketing agencies</p>', unsafe_allow_html=True)


# ============== DASHBOARD PAGE ==============
if page == "🏠 Dashboard":
    st.markdown("## 🏠 Dashboard Overview")
    
    if not st.session_state.data_loaded:
        st.info("👈 Upload data or load sample data to see the dashboard")
        
        # Feature cards
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("""
            ### 📤 Data Ingestion
            Upload CSV, Excel, or JSON files from any ad platform.
            Auto-detects Google Ads, Meta, TikTok, and LinkedIn formats.
            """)
        
        with col2:
            st.markdown("""
            ### 🔧 Data Processing
            Normalize columns, fix date formats, convert currencies.
            Quality checks and anomaly detection built-in.
            """)
        
        with col3:
            st.markdown("""
            ### 🤖 AI Insights
            Get intelligent recommendations for your campaigns.
            Ask questions in natural language.
            """)
        
        st.markdown("---")
        st.markdown("### Get Started")
        st.markdown("1. Upload your marketing data files")
        st.markdown("2. Process and normalize the data")
        st.markdown("3. Generate insights and reports")
        
    else:
        # Data is loaded - show dashboard
        data_id = st.session_state.data_id
        
        # Get data stats
        with st.spinner("Loading analytics..."):
            stats_result = api_call(f'/data/{data_id}/stats')
            
        if stats_result and stats_result.get('success'):
            stats = stats_result['stats']
            
            # Key Metrics Row
            st.markdown("### 📊 Key Metrics")
            
            col1, col2, col3, col4, col5 = st.columns(5)
            
            stats_data = stats.get('statistics', {})
            
            with col1:
                total_spend = stats_data.get('spend', {}).get('sum', 0)
                st.metric("Total Spend", format_number(total_spend, '$'))
            
            with col2:
                total_impressions = stats_data.get('impressions', {}).get('sum', 0)
                st.metric("Impressions", format_number(total_impressions))
            
            with col3:
                total_clicks = stats_data.get('clicks', {}).get('sum', 0)
                st.metric("Clicks", format_number(total_clicks))
            
            with col4:
                total_conversions = stats_data.get('conversions', {}).get('sum', 0)
                st.metric("Conversions", format_number(total_conversions))
            
            with col5:
                total_revenue = stats_data.get('conversion_value', {}).get('sum', 0)
                st.metric("Revenue", format_number(total_revenue, '$'))
            
            st.markdown("---")
            
            # Calculated Metrics
            st.markdown("### 📈 Performance Metrics")
            col1, col2, col3, col4 = st.columns(4)
            
            ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
            cpc = (total_spend / total_clicks) if total_clicks > 0 else 0
            cpa = (total_spend / total_conversions) if total_conversions > 0 else 0
            roas = (total_revenue / total_spend) if total_spend > 0 else 0
            
            with col1:
                st.metric("CTR", f"{ctr:.2f}%")
            with col2:
                st.metric("Avg CPC", format_number(cpc, '$'))
            with col3:
                st.metric("Avg CPA", format_number(cpa, '$'))
            with col4:
                st.metric("ROAS", f"{roas:.2f}x")
            
            st.markdown("---")
            
            # Quick Stats
            st.markdown("### 📋 Data Summary")
            col1, col2 = st.columns(2)
            
            with col1:
                st.info(f"**Total Rows:** {stats['count']:,}")
                st.info(f"**Total Columns:** {stats['columns']}")
            
            with col2:
                st.info(f"**Numeric Columns:** {', '.join(stats['numeric_columns'][:5])}...")


# ============== DATA UPLOAD PAGE ==============
elif page == "📤 Data Upload":
    st.markdown("## 📤 Data Upload")
    st.markdown("Upload your marketing data files for processing. Supports CSV, Excel, and JSON formats.")
    
    tab1, tab2 = st.tabs(["Single File Upload", "Multi-File Upload"])
    
    with tab1:
        st.markdown("### Upload a Single File")
        uploaded_file = st.file_uploader(
            "Choose a file",
            type=['csv', 'xlsx', 'xls', 'json'],
            help="Supported formats: CSV, Excel (.xlsx, .xls), JSON",
            key="single_uploader"
        )
        
        if uploaded_file:
            if st.button("Process File", type="primary", key="process_single"):
                with st.spinner("Processing file..."):
                    files = {'file': (uploaded_file.name, uploaded_file, uploaded_file.type)}
                    result = api_call('/upload', method='POST', files=files)
                
                if result and result.get('success'):
                    # Store in session state
                    st.session_state.data_id = result['data_id']
                    st.session_state.data_loaded = True
                    st.session_state.current_data = pd.DataFrame(result['preview'])
                    st.session_state.upload_success = True
                    st.session_state.upload_metadata = result['metadata']
                    st.session_state.upload_preview = result['preview']
                    st.session_state.normalization_done = False
                    st.session_state.quality_report = None
                    st.session_state.insights = None
                    # Clear multi flags
                    st.session_state.multi_success = False
                    
                    st.rerun()
                else:
                    st.error(f"Error: {result.get('error', 'Unknown error')}" if result else "API connection failed")
        
        # Display upload result from session state (persists after rerun)
        if st.session_state.get('upload_success') and st.session_state.get('upload_preview') is not None:
            st.success(f"✅ File processed successfully!")
            
            # Show metadata
            metadata = st.session_state.upload_metadata
            col1, col2, col3 = st.columns(3)
            with col1:
                st.info(f"**Platform:** {metadata['platform'].replace('_', ' ').title()}")
                st.info(f"**Confidence:** {metadata['platform_confidence']*100:.0f}%")
            with col2:
                st.info(f"**Rows:** {metadata['stats']['total_rows']:,}")
                st.info(f"**Columns:** {metadata['stats']['total_columns']}")
            with col3:
                st.info(f"**File Type:** {metadata['file_type'].upper()}")
                st.info(f"**Duplicates:** {metadata['stats']['duplicate_rows']}")
            
            # Preview
            st.markdown("### Data Preview")
            st.dataframe(pd.DataFrame(st.session_state.upload_preview), use_container_width=True)
            
            # Optional: clear button to reset
            if st.button("Clear Upload", key="clear_single"):
                for key in ['upload_success', 'upload_preview', 'upload_metadata']:
                    if key in st.session_state:
                        del st.session_state[key]
                st.rerun()
    
    with tab2:
        st.markdown("### Upload Multiple Files")
        st.info("Upload files from different platforms to merge them into a unified dataset.")
        
        uploaded_files = st.file_uploader(
            "Choose multiple files",
            type=['csv', 'xlsx', 'xls', 'json'],
            accept_multiple_files=True,
            key="multi_uploader"
        )
        
        if uploaded_files and len(uploaded_files) > 0:
            st.markdown(f"**{len(uploaded_files)} files selected:**")
            for f in uploaded_files:
                st.markdown(f"- {f.name}")
            
            if st.button("Process All Files", type="primary", key="process_multi"):
                with st.spinner("Processing files..."):
                    files = [('files', (f.name, f, f.type)) for f in uploaded_files]
                    result = api_call('/upload-multiple', method='POST', files=files)
                
                if result and result.get('success'):
                    # Store in session state
                    st.session_state.data_id = result['data_id']
                    st.session_state.data_loaded = True
                    st.session_state.current_data = pd.DataFrame(result['preview'])
                    st.session_state.multi_success = True
                    st.session_state.multi_metadata = {
                        'files_processed': result['files_processed'],
                        'platforms': result['platforms']
                    }
                    st.session_state.multi_preview = result['preview']
                    st.session_state.normalization_done = False
                    st.session_state.quality_report = None
                    st.session_state.insights = None
                    # Clear single flags
                    st.session_state.upload_success = False
                    
                    st.rerun()
                else:
                    st.error(f"Error: {result.get('error', 'Unknown error')}" if result else "API connection failed")
        
        # Display multi-upload result from session state
        if st.session_state.get('multi_success') and st.session_state.get('multi_preview') is not None:
            st.success(f"✅ {st.session_state.multi_metadata['files_processed']} files merged successfully!")
            st.info(f"**Platforms detected:** {', '.join(st.session_state.multi_metadata['platforms'])}")
            
            st.markdown("### Merged Data Preview")
            st.dataframe(pd.DataFrame(st.session_state.multi_preview), use_container_width=True)
            
            if st.button("Clear Upload", key="clear_multi"):
                for key in ['multi_success', 'multi_preview', 'multi_metadata']:
                    if key in st.session_state:
                        del st.session_state[key]
                st.rerun()


# ============== DATA PROCESSING PAGE ==============
elif page == "🔧 Data Processing":
    st.markdown("## 🔧 Data Processing")
    
    if not st.session_state.data_loaded:
        st.warning("⚠️ Please upload data first!")
    else:
        data_id = st.session_state.data_id
        
        tab1, tab2, tab3 = st.tabs(["Normalization", "Quality Check", "Data View"])
        
        with tab1:
            st.markdown("### Data Normalization")
            st.markdown("Standardize your data with consistent column names, date formats, and currencies.")
            
            col1, col2 = st.columns(2)
            with col1:
                currency = st.selectbox("Target Currency", ['USD', 'EUR', 'GBP', 'CAD', 'AUD'], key="norm_currency")
            with col2:
                platform = st.selectbox("Platform Override", ['auto-detect', 'google_ads', 'meta_ads', 'tiktok_ads', 'linkedin_ads'], key="norm_platform")
            
            if st.button("Normalize Data", type="primary", key="normalize_btn"):
                with st.spinner("Normalizing data..."):
                    payload = {
                        'currency': currency,
                        'platform': platform if platform != 'auto-detect' else 'unknown'
                    }
                    result = api_call(f'/normalize/{data_id}', method='POST', data=payload)
                
                if result and result.get('success'):
                    # Store in session state
                    st.session_state.normalization_done = True
                    st.session_state.normalization_report = result['normalization_report']
                    st.session_state.normalization_preview = result['preview']
                    st.rerun()
                else:
                    st.error(f"Error: {result.get('error', 'Unknown error')}" if result else "API connection failed")
            
            # Display normalization result from session state (persists after rerun)
            if st.session_state.get('normalization_done') and st.session_state.get('normalization_report'):
                st.success("✅ Data normalized successfully!")
                
                report = st.session_state.normalization_report
                st.markdown("### Normalization Report")
                
                col1, col2 = st.columns(2)
                with col1:
                    st.info(f"**Columns Mapped:** {len([s for s in report['steps_performed'] if 'Mapped' in s])}")
                    st.info(f"**Original Rows:** {report['original_rows']}")
                with col2:
                    st.info(f"**Final Rows:** {report['final_rows']}")
                    st.info(f"**Duplicates Removed:** {report['original_rows'] - report['final_rows']}")
                
                with st.expander("View Detailed Steps"):
                    for step in report['steps_performed']:
                        st.markdown(f"- {step}")
                
                # Preview
                st.markdown("### Normalized Data Preview")
                st.dataframe(pd.DataFrame(st.session_state.normalization_preview), use_container_width=True)
                
                # Optional: clear button to reset
                if st.button("Clear Normalization", key="clear_norm"):
                    for key in ['normalization_done', 'normalization_report', 'normalization_preview']:
                        if key in st.session_state:
                            del st.session_state[key]
                    st.rerun()
        
        with tab2:
            st.markdown("### Data Quality Check")
            
            if st.button("Run Quality Check", type="primary"):
                with st.spinner("Checking data quality..."):
                    result = api_call(f'/quality/check/{data_id}')
                
                if result and result.get('success'):
                    report = result['quality_report']
                    st.session_state.quality_report = report
                    
                    # Quality Score
                    score = report['score']
                    grade = report['grade']
                    
                    col1, col2, col3 = st.columns([1, 2, 1])
                    with col2:
                        if score >= 90:
                            st.success(f"### Quality Score: {score:.0f}/100 (Grade: {grade})")
                        elif score >= 70:
                            st.warning(f"### Quality Score: {score:.0f}/100 (Grade: {grade})")
                        else:
                            st.error(f"### Quality Score: {score:.0f}/100 (Grade: {grade})")
                    
                    # Issues
                    if report['issues']:
                        st.markdown("### ⚠️ Issues Found")
                        for issue in report['issues']:
                            st.warning(f"- {issue}")
                    
                    if report['warnings']:
                        st.markdown("### 📝 Warnings")
                        for warning in report['warnings']:
                            st.info(f"- {warning}")
                    
                    # Checks breakdown
                    with st.expander("View Detailed Checks"):
                        for check_name, check_result in report['checks'].items():
                            st.markdown(f"**{check_name.title()}**")
                            st.json(check_result)
            
            # Anomaly Detection
            st.markdown("---")
            st.markdown("### Anomaly Detection")
            
            if st.button("Detect Anomalies"):
                with st.spinner("Detecting anomalies..."):
                    result = api_call(f'/quality/anomalies/{data_id}')
                
                if result and result.get('success'):
                    anomalies = result['anomalies']
                    perf_issues = result['performance_issues']
                    
                    if anomalies['anomalies']:
                        st.warning(f"Found {len(anomalies['anomalies'])} potential anomalies")
                        
                        anomaly_df = pd.DataFrame(anomalies['anomalies'])
                        st.dataframe(anomaly_df, use_container_width=True)
                    else:
                        st.success("✅ No significant anomalies detected!")
                    
                    if perf_issues['issues']:
                        st.markdown("### 🎯 Performance Issues")
                        for issue in perf_issues['issues']:
                            if issue['type'] == 'budget_drainage':
                                st.error(f"🔴 {issue['recommendation']}")
                            else:
                                st.warning(f"⚠️ {issue['recommendation']}")
        
        with tab3:
            st.markdown("### Data View")
            
            # Get full data
            with st.spinner("Loading data..."):
                result = api_call(f'/data/{data_id}')
            
            if result and result.get('success'):
                df = pd.DataFrame(result['data'])
                
                # Filters
                col1, col2 = st.columns(2)
                with col1:
                    if 'platform' in df.columns:
                        platforms = st.multiselect("Filter by Platform", df['platform'].unique(), default=df['platform'].unique())
                        df = df[df['platform'].isin(platforms)]
                
                with col2:
                    if 'campaign_name' in df.columns:
                        campaigns = st.multiselect("Filter by Campaign", df['campaign_name'].unique()[:10], default=df['campaign_name'].unique()[:10])
                        df = df[df['campaign_name'].isin(campaigns)]
                
                st.markdown(f"**Showing {len(df)} rows**")
                st.dataframe(df, use_container_width=True)
                
                # Download
                csv = df.to_csv(index=False)
                st.download_button("Download CSV", csv, "marketing_data.csv", "text/csv")


# ============== ANALYTICS PAGE ==============
elif page == "📈 Analytics":
    st.markdown("## 📈 Analytics")
    
    if not st.session_state.data_loaded:
        st.warning("⚠️ Please upload data first!")
    else:
        data_id = st.session_state.data_id
        
        tab1, tab2, tab3 = st.tabs(["By Date", "By Campaign", "By Platform"])
        
        with tab1:
            st.markdown("### Date-wise Analytics")
            
            granularity = st.selectbox("Granularity", ['daily', 'weekly', 'monthly'])
            
            with st.spinner("Aggregating data..."):
                result = api_call(f'/aggregate/date/{data_id}?granularity={granularity}')
            
            if result and result.get('success'):
                df = pd.DataFrame(result['data'])
                
                # Line chart
                import plotly.express as px
                import plotly.graph_objects as go
                
                if 'spend' in df.columns:
                    fig = px.line(df, x='period', y='spend', title='Spend Over Time')
                    st.plotly_chart(fig, use_container_width=True)
                
                if 'clicks' in df.columns and 'impressions' in df.columns:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=df['period'], y=df['clicks'], name='Clicks', yaxis='y'))
                    fig.add_trace(go.Scatter(x=df['period'], y=df['impressions']/100, name='Impressions/100', yaxis='y2'))
                    fig.update_layout(title='Clicks and Impressions', yaxis2=dict(overlaying='y', side='right'))
                    st.plotly_chart(fig, use_container_width=True)
                
                st.dataframe(df, use_container_width=True)
        
        with tab2:
            st.markdown("### Campaign Analytics")
            
            with st.spinner("Aggregating by campaign..."):
                result = api_call(f'/aggregate/campaign/{data_id}')
            
            if result and result.get('success'):
                df = pd.DataFrame(result['data'])
                
                # Bar chart
                import plotly.express as px
                
                if 'spend' in df.columns:
                    fig = px.bar(df.sort_values('spend', ascending=False).head(10), 
                                x='campaign_name', y='spend', title='Top 10 Campaigns by Spend')
                    st.plotly_chart(fig, use_container_width=True)
                
                if 'roas' in df.columns:
                    fig = px.bar(df.sort_values('roas', ascending=False).head(10),
                                x='campaign_name', y='roas', title='Top 10 Campaigns by ROAS')
                    st.plotly_chart(fig, use_container_width=True)
                
                st.dataframe(df, use_container_width=True)
        
        with tab3:
            st.markdown("### Platform Comparison")
            
            with st.spinner("Comparing platforms..."):
                result = api_call(f'/compare/platforms/{data_id}')
            
            if result and result.get('success'):
                stats = result['platform_stats']
                
                if stats:
                    df = pd.DataFrame(stats)
                    
                    import plotly.express as px
                    
                    fig = px.pie(df, values='spend', names='platform', title='Spend Distribution by Platform')
                    st.plotly_chart(fig, use_container_width=True)
                    
                    st.dataframe(df, use_container_width=True)
                    
                    # Rankings
                    if result.get('rankings'):
                        st.markdown("### Platform Rankings")
                        for metric, ranking in result['rankings'].items():
                            st.markdown(f"**{metric.upper()}**: {' > '.join(ranking)}")


# ============== AI INSIGHTS PAGE ==============
elif page == "🤖 AI Insights":
    st.markdown("## 🤖 AI Insights")
    
    if not st.session_state.data_loaded:
        st.warning("⚠️ Please upload data first!")
    else:
        data_id = st.session_state.data_id
        
        # Natural Language Query
        st.markdown("### Ask a Question")
        question = st.text_input("Type your question about the data:", 
                                placeholder="e.g., Why did ROAS drop last week?")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            focus_area = st.selectbox("Focus Area (Optional)", [None, 'performance', 'budget', 'optimization'])
        
        if st.button("Generate Insights", type="primary"):
            with st.spinner("Analyzing data..."):
                payload = {}
                if question:
                    payload['question'] = question
                if focus_area:
                    payload['focus_area'] = focus_area
                
                result = api_call(f'/insights/{data_id}', method='POST', data=payload)
            
            if result and result.get('success'):
                insights_data = result['insights']
                st.session_state.insights = insights_data
                
                # Show answer if question was asked
                if question and insights_data.get('answer'):
                    st.markdown("### 💡 Answer")
                    st.info(insights_data['answer'])
                
                # Show insights
                if insights_data.get('ai_insights'):
                    st.markdown("### 📊 Insights")
                    for insight in insights_data['ai_insights']:
                        if insight['type'] == 'positive':
                            st.success(f"✅ {insight['message']}")
                        elif insight['type'] == 'critical':
                            st.error(f"🔴 {insight['message']}")
                        elif insight['type'] == 'warning':
                            st.warning(f"⚠️ {insight['message']}")
                        else:
                            st.info(f"ℹ️ {insight['message']}")
                
                # Show recommendations
                if insights_data.get('recommendations'):
                    st.markdown("### 📋 Recommendations")
                    for rec in insights_data['recommendations']:
                        priority = rec.get('priority', 'medium')
                        if priority == 'high':
                            st.error(f"🔴 **{priority.upper()}**: {rec['action']}")
                        elif priority == 'medium':
                            st.warning(f"⚠️ **{priority.upper()}**: {rec['action']}")
                        else:
                            st.info(f"ℹ️ **{priority.upper()}**: {rec['action']}")
                        st.caption(f"_Expected impact: {rec.get('expected_impact', 'N/A')}_")


# ============== REPORTS PAGE ==============
elif page == "📄 Reports":
    st.markdown("## 📄 Reports")
    
    if not st.session_state.data_loaded:
        st.warning("⚠️ Please upload data first!")
    else:
        data_id = st.session_state.data_id
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Excel Report")
            st.markdown("Generate a comprehensive Excel report with multiple sheets.")
            
            report_name = st.text_input("Report Name", "marketing_report")
            
            if st.button("Generate Excel Report", type="primary"):
                with st.spinner("Generating Excel report..."):
                    payload = {
                        'report_name': report_name,
                        'include_charts': True,
                        'insights': st.session_state.insights
                    }
                    result = api_call(f'/report/excel/{data_id}', method='POST', data=payload)
                
                if result and result.get('success'):
                    st.success("✅ Excel report generated!")
                    st.download_button(
                        "📥 Download Excel Report",
                        data=open(result['filepath'], 'rb').read(),
                        file_name=result['filename'],
                        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    )
        
        with col2:
            st.markdown("### PDF Report")
            st.markdown("Generate a client-ready PDF report with visualizations.")
            
            client_name = st.text_input("Client Name", "Client")
            
            if st.button("Generate PDF Report", type="primary"):
                with st.spinner("Generating PDF report..."):
                    payload = {
                        'report_name': report_name,
                        'client_name': client_name,
                        'insights': st.session_state.insights,
                        'include_charts': True
                    }
                    result = api_call(f'/report/pdf/{data_id}', method='POST', data=payload)
                
                if result and result.get('success'):
                    st.success("✅ Report generated!")
                    
                    # Show HTML link
                    with open(result['html_filepath'], 'r') as f:
                        html_content = f.read()
                    
                    st.download_button(
                        "📥 Download HTML Report (Print to PDF)",
                        data=html_content,
                        file_name=result['filename'].replace('.pdf', '.html'),
                        mime='text/html'
                    )
        
        st.markdown("---")
        
        # Quick Export
        st.markdown("### Quick Export")
        if st.button("Export as CSV"):
            with st.spinner("Exporting..."):
                result = api_call(f'/export/csv/{data_id}')
            
            if result and result.get('success'):
                with open(result['filepath'], 'r') as f:
                    csv_content = f.read()
                
                st.download_button(
                    "📥 Download CSV",
                    data=csv_content,
                    file_name=result['filename'],
                    mime='text/csv'
                )


# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #64748b; font-size: 0.8rem;">
    Marketing Data Engine v1.0 | Built for Belva Mar-Tech Agency
</div>
""", unsafe_allow_html=True)