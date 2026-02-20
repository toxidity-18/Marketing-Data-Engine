"""
Report Generator Service
Generates PDF and Excel reports from marketing data
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from datetime import datetime
import io
import os
import base64
from io import BytesIO


class ReportGeneratorService:
    """
    Service for generating client-ready reports from marketing data.
    """
    def __init__(self, output_dir: str = None):
        if output_dir is None:
            # Use default relative path
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            output_dir = os.path.join(base_dir, 'data', 'reports')
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        # ===== FIX: Initialize report history =====
        self.report_history = []
        
    def generate_excel_report(self, df: pd.DataFrame, 
                              report_name: str = 'marketing_report',
                              include_charts: bool = True,
                              insights: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Generate an Excel report with multiple sheets.
        """
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{report_name}_{timestamp}.xlsx"
            filepath = os.path.join(self.output_dir, filename)
            
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                # Sheet 1: Summary
                self._create_summary_sheet(df, writer, insights)
                
                # Sheet 2: Raw Data
                df.to_excel(writer, sheet_name='Raw Data', index=False)
                
                # Sheet 3: Campaign Summary
                if 'campaign_name' in df.columns:
                    campaign_summary = self._aggregate_by_campaign(df)
                    campaign_summary.to_excel(writer, sheet_name='Campaign Summary', index=False)
                
                # Sheet 4: Platform Comparison
                if 'platform' in df.columns:
                    platform_summary = self._aggregate_by_platform(df)
                    platform_summary.to_excel(writer, sheet_name='Platform Summary', index=False)
                
                # Sheet 5: Date-wise Trend
                if 'date' in df.columns:
                    date_summary = self._aggregate_by_date(df)
                    date_summary.to_excel(writer, sheet_name='Daily Trend', index=False)
                
                # Sheet 6: Insights (if provided)
                if insights:
                    self._create_insights_sheet(writer, insights)
            
            # Record generation
            record = {
                'timestamp': datetime.now().isoformat(),
                'type': 'excel',
                'filename': filename,
                'filepath': filepath,
                'rows': len(df),
                'sheets': ['Summary', 'Raw Data', 'Campaign Summary', 'Platform Summary', 'Daily Trend', 'Insights']
            }
            self.report_history.append(record)
            
            return {
                'success': True,
                'filepath': filepath,
                'filename': filename,
                'download_url': f'/download/{filename}'
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def generate_pdf_report(self, df: pd.DataFrame,
                           report_name: str = 'marketing_report',
                           client_name: str = 'Client',
                           insights: Optional[Dict] = None,
                           include_charts: bool = True) -> Dict[str, Any]:
        """
        Generate a PDF report with visualizations.
        """
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{report_name}_{timestamp}.pdf"
            filepath = os.path.join(self.output_dir, filename)
            
            # Generate HTML content for PDF
            html_content = self._generate_html_report(df, client_name, insights, include_charts)
            
            # Save HTML for reference
            html_filepath = filepath.replace('.pdf', '.html')
            with open(html_filepath, 'w') as f:
                f.write(html_content)
            
            # Note: For actual PDF generation, weasyprint or reportlab would be used
            # For now, we'll return the HTML which can be opened in browser and printed as PDF
            
            record = {
                'timestamp': datetime.now().isoformat(),
                'type': 'pdf',
                'filename': filename,
                'filepath': filepath,
                'html_filepath': html_filepath,
                'rows': len(df)
            }
            self.report_history.append(record)
            
            return {
                'success': True,
                'filepath': filepath,
                'filename': filename,
                'html_filepath': html_filepath,
                'download_url': f'/download/{filename}'
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def generate_csv_export(self, df: pd.DataFrame,
                           export_name: str = 'marketing_data') -> Dict[str, Any]:
        """
        Generate a clean CSV export.
        """
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{export_name}_{timestamp}.csv"
            filepath = os.path.join(self.output_dir, filename)
            
            df.to_csv(filepath, index=False)
            
            return {
                'success': True,
                'filepath': filepath,
                'filename': filename,
                'download_url': f'/download/{filename}'
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _create_summary_sheet(self, df: pd.DataFrame, writer, insights: Optional[Dict]):
        """
        Create a summary sheet with key metrics.
        """
        summary_data = []
        
        # Overall metrics
        metrics = {
            'Metric': [],
            'Value': []
        }
        
        if 'spend' in df.columns:
            metrics['Metric'].append('Total Spend')
            metrics['Value'].append(f"${df['spend'].sum():,.2f}")
        
        if 'impressions' in df.columns:
            metrics['Metric'].append('Total Impressions')
            metrics['Value'].append(f"{df['impressions'].sum():,.0f}")
        
        if 'clicks' in df.columns:
            metrics['Metric'].append('Total Clicks')
            metrics['Value'].append(f"{df['clicks'].sum():,.0f}")
        
        if 'conversions' in df.columns:
            metrics['Metric'].append('Total Conversions')
            metrics['Value'].append(f"{df['conversions'].sum():,.0f}")
        
        if 'conversion_value' in df.columns:
            metrics['Metric'].append('Total Revenue')
            metrics['Value'].append(f"${df['conversion_value'].sum():,.2f}")
        
        # Calculate derived metrics
        total_spend = df['spend'].sum() if 'spend' in df.columns else 0
        total_clicks = df['clicks'].sum() if 'clicks' in df.columns else 0
        total_impressions = df['impressions'].sum() if 'impressions' in df.columns else 0
        total_conversions = df['conversions'].sum() if 'conversions' in df.columns else 0
        total_revenue = df['conversion_value'].sum() if 'conversion_value' in df.columns else 0
        
        if total_clicks > 0 and total_impressions > 0:
            metrics['Metric'].append('Overall CTR')
            metrics['Value'].append(f"{total_clicks / total_impressions * 100:.2f}%")
        
        if total_spend > 0 and total_clicks > 0:
            metrics['Metric'].append('Average CPC')
            metrics['Value'].append(f"${total_spend / total_clicks:.2f}")
        
        if total_spend > 0 and total_conversions > 0:
            metrics['Metric'].append('Average CPA')
            metrics['Value'].append(f"${total_spend / total_conversions:.2f}")
        
        if total_revenue > 0 and total_spend > 0:
            metrics['Metric'].append('Overall ROAS')
            metrics['Value'].append(f"{total_revenue / total_spend:.2f}x")
        
        summary_df = pd.DataFrame(metrics)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
    
    def _create_insights_sheet(self, writer, insights: Dict):
        """
        Create a sheet with AI-generated insights.
        """
        rows = []
        
        # Add insights
        if insights.get('ai_insights'):
            for insight in insights['ai_insights']:
                rows.append({
                    'Type': 'Insight',
                    'Category': insight.get('category', 'General'),
                    'Message': insight.get('message', ''),
                    'Impact': insight.get('impact', 'Unknown')
                })
        
        # Add recommendations
        if insights.get('recommendations'):
            for rec in insights['recommendations']:
                rows.append({
                    'Type': 'Recommendation',
                    'Category': rec.get('priority', 'Medium'),
                    'Message': rec.get('action', ''),
                    'Impact': rec.get('expected_impact', '')
                })
        
        if rows:
            insights_df = pd.DataFrame(rows)
            insights_df.to_excel(writer, sheet_name='Insights', index=False)
    
    def _aggregate_by_campaign(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate metrics by campaign.
        """
        agg_cols = {}
        for col in ['spend', 'impressions', 'clicks', 'conversions', 'conversion_value']:
            if col in df.columns:
                agg_cols[col] = 'sum'
        
        if 'campaign_name' not in df.columns:
            return pd.DataFrame()
        
        campaign_df = df.groupby('campaign_name').agg(agg_cols).reset_index()
        
        # Calculate efficiency metrics
        if 'clicks' in campaign_df.columns and 'impressions' in campaign_df.columns:
            campaign_df['ctr'] = (campaign_df['clicks'] / campaign_df['impressions'] * 100).round(2)
        
        if 'spend' in campaign_df.columns and 'clicks' in campaign_df.columns:
            campaign_df['cpc'] = (campaign_df['spend'] / campaign_df['clicks']).round(2)
        
        if 'spend' in campaign_df.columns and 'conversions' in campaign_df.columns:
            campaign_df['cpa'] = (campaign_df['spend'] / campaign_df['conversions'].replace(0, np.nan)).round(2)
        
        if 'conversion_value' in campaign_df.columns and 'spend' in campaign_df.columns:
            campaign_df['roas'] = (campaign_df['conversion_value'] / campaign_df['spend'].replace(0, np.nan)).round(2)
        
        return campaign_df.sort_values('spend', ascending=False)
    
    def _aggregate_by_platform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate metrics by platform.
        """
        agg_cols = {}
        for col in ['spend', 'impressions', 'clicks', 'conversions', 'conversion_value']:
            if col in df.columns:
                agg_cols[col] = 'sum'
        
        if 'platform' not in df.columns:
            return pd.DataFrame()
        
        platform_df = df.groupby('platform').agg(agg_cols).reset_index()
        
        # Calculate efficiency metrics
        if 'clicks' in platform_df.columns and 'impressions' in platform_df.columns:
            platform_df['ctr'] = (platform_df['clicks'] / platform_df['impressions'] * 100).round(2)
        
        if 'spend' in platform_df.columns and 'clicks' in platform_df.columns:
            platform_df['cpc'] = (platform_df['spend'] / platform_df['clicks']).round(2)
        
        if 'spend' in platform_df.columns and 'conversions' in platform_df.columns:
            platform_df['cpa'] = (platform_df['spend'] / platform_df['conversions'].replace(0, np.nan)).round(2)
        
        if 'conversion_value' in platform_df.columns and 'spend' in platform_df.columns:
            platform_df['roas'] = (platform_df['conversion_value'] / platform_df['spend'].replace(0, np.nan)).round(2)
        
        return platform_df.sort_values('spend', ascending=False)
    
    def _aggregate_by_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate metrics by date.
        """
        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        
        agg_cols = {'date': 'first'}
        for col in ['spend', 'impressions', 'clicks', 'conversions', 'conversion_value']:
            if col in df.columns:
                agg_cols[col] = 'sum'
        
        date_df = df.groupby(df['date'].dt.date).agg(agg_cols).reset_index()
        
        # Calculate efficiency metrics
        if 'clicks' in date_df.columns and 'impressions' in date_df.columns:
            date_df['ctr'] = (date_df['clicks'] / date_df['impressions'] * 100).round(2)
        
        if 'spend' in date_df.columns and 'clicks' in date_df.columns:
            date_df['cpc'] = (date_df['spend'] / date_df['clicks']).round(2)
        
        return date_df.sort_values('date')
    
    def _generate_html_report(self, df: pd.DataFrame, client_name: str, 
                             insights: Optional[Dict], include_charts: bool) -> str:
        """
        Generate HTML content for PDF report.
        """
        # Calculate metrics
        total_spend = df['spend'].sum() if 'spend' in df.columns else 0
        total_impressions = df['impressions'].sum() if 'impressions' in df.columns else 0
        total_clicks = df['clicks'].sum() if 'clicks' in df.columns else 0
        total_conversions = df['conversions'].sum() if 'conversions' in df.columns else 0
        total_revenue = df['conversion_value'].sum() if 'conversion_value' in df.columns else 0
        
        overall_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
        overall_cpc = (total_spend / total_clicks) if total_clicks > 0 else 0
        overall_cpa = (total_spend / total_conversions) if total_conversions > 0 else 0
        overall_roas = (total_revenue / total_spend) if total_spend > 0 else 0
        
        # Platform breakdown
        platform_html = ''
        if 'platform' in df.columns:
            platform_summary = self._aggregate_by_platform(df)
            platform_rows = ''
            for _, row in platform_summary.iterrows():
                platform_rows += f"""
                <tr>
                    <td>{row['platform']}</td>
                    <td>${row.get('spend', 0):,.2f}</td>
                    <td>{row.get('impressions', 0):,.0f}</td>
                    <td>{row.get('clicks', 0):,.0f}</td>
                    <td>{row.get('ctr', 0):.2f}%</td>
                    <td>{row.get('roas', 0):.2f}x</td>
                </tr>
                """
            platform_html = f"""
            <div class="section">
                <h2>Platform Performance</h2>
                <table>
                    <tr>
                        <th>Platform</th>
                        <th>Spend</th>
                        <th>Impressions</th>
                        <th>Clicks</th>
                        <th>CTR</th>
                        <th>ROAS</th>
                    </tr>
                    {platform_rows}
                </table>
            </div>
            """
        
        # Insights section
        insights_html = ''
        if insights and insights.get('ai_insights'):
            insight_items = ''
            for insight in insights['ai_insights']:
                insight_items += f'<li class="{insight.get("type", "info")}">{insight.get("message", "")}</li>'
            
            rec_items = ''
            if insights.get('recommendations'):
                for rec in insights['recommendations']:
                    rec_items += f'<li><strong>{rec.get("priority", "Medium")}:</strong> {rec.get("action", "")}</li>'
            
            insights_html = f"""
            <div class="section">
                <h2>Key Insights</h2>
                <ul class="insights">
                    {insight_items}
                </ul>
                
                <h2>Recommendations</h2>
                <ul class="recommendations">
                    {rec_items}
                </ul>
            </div>
            """
        
        # Generate full HTML
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Marketing Performance Report - {client_name}</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    max-width: 1000px;
                    margin: 0 auto;
                    padding: 20px;
                    color: #333;
                }}
                .header {{
                    text-align: center;
                    padding: 30px 0;
                    border-bottom: 3px solid #2563eb;
                    margin-bottom: 30px;
                }}
                .header h1 {{
                    color: #1e40af;
                    margin: 0;
                }}
                .header p {{
                    color: #666;
                    margin: 10px 0 0;
                }}
                .metrics-grid {{
                    display: grid;
                    grid-template-columns: repeat(4, 1fr);
                    gap: 15px;
                    margin-bottom: 30px;
                }}
                .metric-card {{
                    background: #f8fafc;
                    padding: 20px;
                    border-radius: 8px;
                    text-align: center;
                    border: 1px solid #e2e8f0;
                }}
                .metric-card h3 {{
                    margin: 0;
                    color: #64748b;
                    font-size: 12px;
                    text-transform: uppercase;
                }}
                .metric-card .value {{
                    font-size: 24px;
                    font-weight: bold;
                    color: #1e40af;
                    margin: 10px 0 0;
                }}
                .section {{
                    margin-bottom: 30px;
                }}
                .section h2 {{
                    color: #1e40af;
                    border-bottom: 2px solid #e2e8f0;
                    padding-bottom: 10px;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin-top: 15px;
                }}
                th, td {{
                    padding: 12px;
                    text-align: left;
                    border-bottom: 1px solid #e2e8f0;
                }}
                th {{
                    background: #f1f5f9;
                    font-weight: 600;
                    color: #475569;
                }}
                tr:hover {{
                    background: #f8fafc;
                }}
                .insights li {{
                    padding: 8px 0;
                    border-bottom: 1px solid #e2e8f0;
                }}
                .insights li.positive {{
                    color: #16a34a;
                }}
                .insights li.warning {{
                    color: #ea580c;
                }}
                .insights li.critical {{
                    color: #dc2626;
                    font-weight: bold;
                }}
                .recommendations li {{
                    padding: 8px 0;
                    margin-bottom: 5px;
                }}
                .footer {{
                    text-align: center;
                    padding-top: 30px;
                    border-top: 1px solid #e2e8f0;
                    color: #64748b;
                    font-size: 12px;
                }}
                @media print {{
                    body {{
                        padding: 0;
                    }}
                }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Marketing Performance Report</h1>
                <p>{client_name} | Generated on {datetime.now().strftime('%B %d, %Y')}</p>
            </div>
            
            <div class="metrics-grid">
                <div class="metric-card">
                    <h3>Total Spend</h3>
                    <div class="value">${total_spend:,.2f}</div>
                </div>
                <div class="metric-card">
                    <h3>Total Impressions</h3>
                    <div class="value">{total_impressions:,.0f}</div>
                </div>
                <div class="metric-card">
                    <h3>Total Clicks</h3>
                    <div class="value">{total_clicks:,.0f}</div>
                </div>
                <div class="metric-card">
                    <h3>Conversions</h3>
                    <div class="value">{total_conversions:,.0f}</div>
                </div>
            </div>
            
            <div class="metrics-grid">
                <div class="metric-card">
                    <h3>Overall CTR</h3>
                    <div class="value">{overall_ctr:.2f}%</div>
                </div>
                <div class="metric-card">
                    <h3>Average CPC</h3>
                    <div class="value">${overall_cpc:.2f}</div>
                </div>
                <div class="metric-card">
                    <h3>Average CPA</h3>
                    <div class="value">${overall_cpa:.2f}</div>
                </div>
                <div class="metric-card">
                    <h3>Overall ROAS</h3>
                    <div class="value">{overall_roas:.2f}x</div>
                </div>
            </div>
            
            {platform_html}
            
            {insights_html}
            
            <div class="footer">
                <p>Generated by Marketing Data Engine | Belva Mar-Tech Agency Tools</p>
                <p>Report generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def get_report_history(self) -> List[Dict]:
        """
        Return history of generated reports.
        """
        return self.report_history