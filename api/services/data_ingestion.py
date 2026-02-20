"""
Data Ingestion Service
Handles importing data from various file formats (CSV, Excel, JSON)
Supports marketing platform exports (Google Ads, Meta Ads, TikTok, LinkedIn)
"""

import pandas as pd
import numpy as np
import json
import os
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import io


class DataIngestionService:
    """
    Service for ingesting marketing data from various sources and formats.
    """
    
    # Known platform column mappings
    PLATFORM_MAPPINGS = {
        'google_ads': {
            'identifiers': ['campaign', 'ad group', 'keyword', 'clicks', 'impressions', 'cost', 'ctr'],
            'date_columns': ['day', 'date', 'week', 'month', 'reporting_period'],
            'metric_columns': ['clicks', 'impressions', 'cost', 'conversions', 'conversion_value', 'ctr', 'cpc', 'cpm']
        },
        'meta_ads': {
            'identifiers': ['campaign name', 'ad set name', 'ad name', 'campaign_id', 'reach', 'frequency'],
            'date_columns': ['reporting_starts', 'reporting_ends', 'date_start', 'date_stop'],
            'metric_columns': ['impressions', 'clicks', 'spend', 'reach', 'frequency', 'cpm', 'cpc', 'ctr']
        },
        'tiktok_ads': {
            'identifiers': ['campaign_name', 'adgroup_name', 'ad_name', 'campaign_id'],
            'date_columns': ['stat_days', 'date', 'report_date'],
            'metric_columns': ['spend', 'impressions', 'clicks', 'conversions', 'cost_per_conversion']
        },
        'linkedin_ads': {
            'identifiers': ['campaign name', 'campaign group', 'creative name'],
            'date_columns': ['start date', 'end date', 'date'],
            'metric_columns': ['impressions', 'clicks', 'spend', 'ctr', 'average cpc', 'average cpm']
        }
    }
    
    def __init__(self, upload_dir: str = None):
        if upload_dir is None:
            # Use default relative path
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            upload_dir = os.path.join(base_dir, 'data', 'uploads')
        self.upload_dir = upload_dir
        os.makedirs(upload_dir, exist_ok=True)
        self.ingestion_history = []
    
    def detect_platform(self, df: pd.DataFrame) -> Tuple[str, float]:
        """
        Detect which advertising platform the data came from.
        Returns tuple of (platform_name, confidence_score)
        """
        columns_lower = [col.lower().strip() for col in df.columns]
        column_set = set(columns_lower)
        
        scores = {}
        for platform, mapping in self.PLATFORM_MAPPINGS.items():
            identifiers = [id.lower() for id in mapping['identifiers']]
            matches = sum(1 for id in identifiers if any(id in col for col in column_set))
            scores[platform] = matches / len(identifiers)
        
        best_platform = max(scores, key=scores.get)
        confidence = scores[best_platform]
        
        if confidence < 0.2:
            return 'unknown', 0.0
        
        return best_platform, confidence
    
    def ingest_csv(self, file_path: str, **kwargs) -> Dict[str, Any]:
        """
        Ingest data from a CSV file.
        """
        try:
            # Try different encodings
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, **kwargs)
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                raise ValueError("Could not decode CSV file with any supported encoding")
            
            return self._process_dataframe(df, file_path, 'csv')
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def ingest_excel(self, file_path: str, sheet_name: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """
        Ingest data from an Excel file.
        """
        try:
            if sheet_name:
                df = pd.read_excel(file_path, sheet_name=sheet_name, **kwargs)
            else:
                # Read first sheet by default
                df = pd.read_excel(file_path, **kwargs)
            
            return self._process_dataframe(df, file_path, 'excel')
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def ingest_json(self, file_path: str, **kwargs) -> Dict[str, Any]:
        """
        Ingest data from a JSON file.
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Handle different JSON structures
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                if 'data' in data:
                    df = pd.DataFrame(data['data'])
                else:
                    df = pd.DataFrame([data])
            else:
                raise ValueError("Unsupported JSON structure")
            
            return self._process_dataframe(df, file_path, 'json')
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def ingest_from_buffer(self, buffer: io.BytesIO, filename: str) -> Dict[str, Any]:
        """
        Ingest data from a file buffer (used for Streamlit uploads).
        """
        file_ext = os.path.splitext(filename)[1].lower()
        
        try:
            if file_ext == '.csv':
                df = pd.read_csv(buffer)
            elif file_ext in ['.xlsx', '.xls']:
                df = pd.read_excel(buffer)
            elif file_ext == '.json':
                data = json.load(buffer)
                if isinstance(data, list):
                    df = pd.DataFrame(data)
                else:
                    df = pd.DataFrame([data])
            else:
                return {'success': False, 'error': f'Unsupported file format: {file_ext}'}
            
            return self._process_dataframe(df, filename, file_ext[1:])
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _process_dataframe(self, df: pd.DataFrame, source: str, file_type: str) -> Dict[str, Any]:
        """
        Process a DataFrame and return metadata about the ingested data.
        """
        # Clean column names
        df.columns = [col.strip() for col in df.columns]
        
        # Detect platform
        platform, confidence = self.detect_platform(df)
        
        # Get basic stats
        stats = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'columns': list(df.columns),
            'column_types': {col: str(dtype) for col, dtype in df.dtypes.items()},
            'missing_values': {col: int(df[col].isna().sum()) for col in df.columns},
            'duplicate_rows': int(df.duplicated().sum()),
            'memory_usage': f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB"
        }
        
        # Detect date columns
        date_columns = self._detect_date_columns(df)
        stats['date_columns'] = date_columns
        
        # Detect numeric columns
        numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
        stats['numeric_columns'] = numeric_columns
        
        # Record ingestion
        ingestion_record = {
            'timestamp': datetime.now().isoformat(),
            'source': source,
            'file_type': file_type,
            'platform': platform,
            'confidence': confidence,
            'rows': len(df),
            'columns': len(df.columns)
        }
        self.ingestion_history.append(ingestion_record)
        
        return {
            'success': True,
            'data': df,
            'metadata': {
                'source': source,
                'file_type': file_type,
                'platform': platform,
                'platform_confidence': confidence,
                'stats': stats,
                'ingested_at': datetime.now().isoformat()
            }
        }
    
    def _detect_date_columns(self, df: pd.DataFrame) -> List[str]:
        """
        Detect columns that contain date/datetime values.
        """
        date_columns = []
        
        for col in df.columns:
            # Check if dtype is already datetime
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                date_columns.append(col)
                continue
            
            # Try to parse as date
            if df[col].dtype == 'object':
                try:
                    sample = df[col].dropna().head(100)
                    if len(sample) > 0:
                        pd.to_datetime(sample, errors='raise')
                        date_columns.append(col)
                except:
                    pass
        
        return date_columns
    
    def get_ingestion_history(self) -> List[Dict]:
        """
        Return the history of data ingestions.
        """
        return self.ingestion_history
    
    def save_uploaded_file(self, file_content: bytes, filename: str) -> str:
        """
        Save uploaded file to the upload directory.
        Returns the path to the saved file.
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_filename = f"{timestamp}_{filename}"
        file_path = os.path.join(self.upload_dir, safe_filename)
        
        with open(file_path, 'wb') as f:
            f.write(file_content)
        
        return file_path