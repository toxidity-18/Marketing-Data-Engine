"""
Data Normalization Service
Standardizes marketing data across different platforms
Handles column mapping, date formats, currency conversion, deduplication
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import re


class DataNormalizationService:
    """
    Service for normalizing marketing data to a standard schema.
    """
    
    # Standard column schema for marketing data
    STANDARD_SCHEMA = {
        # Identifiers
        'campaign_name': ['campaign', 'campaign name', 'campaign_name', 'campaignname'],
        'adset_name': ['ad set', 'adset', 'adset name', 'adset_name', 'ad group', 'adgroup', 'adgroup_name'],
        'ad_name': ['ad', 'ad name', 'ad_name', 'creative', 'creative name', 'creative_name'],
        'keyword': ['keyword', 'keywords', 'search term', 'search_term'],
        
        # Dimensions
        'date': ['date', 'day', 'reporting_period', 'report_date', 'stat_days'],
        'platform': ['platform', 'source', 'network'],
        'country': ['country', 'country_code', 'country code'],
        'device': ['device', 'device_type', 'device type'],
        
        # Metrics - Reach & Impressions
        'impressions': ['impressions', 'imps', 'impr', 'impressions'],
        'reach': ['reach', 'unique_users', 'unique users'],
        'frequency': ['frequency', 'freq'],
        
        # Metrics - Engagement
        'clicks': ['clicks', 'click', 'link_clicks', 'link clicks'],
        'ctr': ['ctr', 'click_through_rate', 'click-through rate', 'click through rate'],
        'engagements': ['engagements', 'engagement', 'total_engagements'],
        
        # Metrics - Conversions
        'conversions': ['conversions', 'conv', 'converts', 'purchases', 'completes'],
        'conversion_value': ['conversion_value', 'conv value', 'conversion value', 'revenue', 'purchase_value'],
        'leads': ['leads', 'lead', 'signups', 'sign_ups'],
        
        # Metrics - Cost
        'spend': ['spend', 'cost', 'ad spend', 'ad_spend', 'amount spent', 'amount_spent'],
        'cpc': ['cpc', 'cost_per_click', 'cost per click', 'average cpc', 'avg cpc'],
        'cpm': ['cpm', 'cost_per_mille', 'cost per mille', 'cost per 1000 impressions', 'average cpm'],
        'cpa': ['cpa', 'cost_per_acquisition', 'cost per acquisition', 'cost per conversion'],
        'roas': ['roas', 'return_on_ad_spend', 'return on ad spend'],
        
        # Video Metrics
        'video_views': ['video_views', 'video views', 'video_views_3s', '3-second video views'],
        'video_completions': ['video_completions', 'video completions', '100% video views'],
        'video_view_rate': ['video_view_rate', 'video view rate', 'vtr', 'vcr'],
        
        # Additional
        'quality_score': ['quality_score', 'quality score', 'qs'],
        'search_impression_share': ['search_impression_share', 'search impression share', 'sis']
    }
    
    # Currency conversion rates (approximate, for demonstration)
    CURRENCY_RATES = {
        'USD': 1.0,
        'EUR': 1.08,
        'GBP': 1.27,
        'CAD': 0.74,
        'AUD': 0.65,
        'INR': 0.012,
        'JPY': 0.0067,
        'BRL': 0.20,
        'MXN': 0.058
    }
    
    def __init__(self):
        self.normalization_log = []
    
    def normalize(self, df: pd.DataFrame, platform: str = 'unknown', 
                  currency: str = 'USD', custom_mappings: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Main normalization method that applies all standardization steps.
        """
        original_columns = list(df.columns)
        log_entries = []
        
        # Step 1: Map columns to standard names
        df, mapping_log = self._map_columns(df, custom_mappings)
        log_entries.extend(mapping_log)
        
        # Step 2: Normalize date columns
        df, date_log = self._normalize_dates(df)
        log_entries.extend(date_log)
        
        # Step 3: Normalize numeric columns
        df, numeric_log = self._normalize_numeric(df)
        log_entries.extend(numeric_log)
        
        # Step 4: Normalize currency
        df, currency_log = self._normalize_currency(df, currency)
        log_entries.extend(currency_log)
        
        # Step 5: Remove duplicates
        df, dedup_log = self._deduplicate(df)
        log_entries.extend(dedup_log)
        
        # Step 6: Calculate derived metrics
        df, calc_log = self._calculate_derived_metrics(df)
        log_entries.extend(calc_log)
        
        # Step 7: Handle missing values
        df, missing_log = self._handle_missing_values(df)
        log_entries.extend(missing_log)
        
        # Record normalization
        normalization_record = {
            'timestamp': datetime.now().isoformat(),
            'platform': platform,
            'original_columns': original_columns,
            'normalized_columns': list(df.columns),
            'original_rows': len(df),
            'final_rows': len(df),
            'steps_performed': log_entries
        }
        self.normalization_log.append(normalization_record)
        
        return {
            'success': True,
            'data': df,
            'normalization_report': normalization_record
        }
    
    def _map_columns(self, df: pd.DataFrame, custom_mappings: Optional[Dict] = None) -> Tuple[pd.DataFrame, List[str]]:
        """
        Map source columns to standard column names.
        """
        log = []
        column_mapping = {}
        
        # Combine standard mappings with custom mappings
        all_mappings = self.STANDARD_SCHEMA.copy()
        if custom_mappings:
            for standard_col, variants in custom_mappings.items():
                if standard_col in all_mappings:
                    all_mappings[standard_col].extend(variants)
                else:
                    all_mappings[standard_col] = variants
        
        # Create mapping for current DataFrame
        for col in df.columns:
            col_lower = col.lower().strip()
            
            for standard_name, variants in all_mappings.items():
                if col_lower in [v.lower() for v in variants]:
                    column_mapping[col] = standard_name
                    log.append(f"Mapped '{col}' → '{standard_name}'")
                    break
        
        # Apply mapping
        df = df.rename(columns=column_mapping)
        
        if column_mapping:
            log.insert(0, f"Column mapping: {len(column_mapping)} columns standardized")
        else:
            log.append("No standard column mappings found")
        
        return df, log
    
    def _normalize_dates(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """
        Convert all date columns to a standard datetime format.
        """
        log = []
        date_patterns = [
            '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d',
            '%d-%m-%Y', '%m-%d-%Y', '%Y%m%d', '%d %b %Y',
            '%d %B %Y', '%b %d, %Y', '%B %d, %Y'
        ]
        
        if 'date' in df.columns:
            original_type = str(df['date'].dtype)
            
            if not pd.api.types.is_datetime64_any_dtype(df['date']):
                for pattern in date_patterns:
                    try:
                        df['date'] = pd.to_datetime(df['date'], format=pattern, errors='raise')
                        log.append(f"Date column parsed with format: {pattern}")
                        break
                    except:
                        continue
                
                # Fallback to pandas automatic parsing
                if not pd.api.types.is_datetime64_any_dtype(df['date']):
                    df['date'] = pd.to_datetime(df['date'], errors='coerce')
                    log.append("Date column parsed with automatic detection")
            
            # Remove rows with invalid dates
            invalid_dates = df['date'].isna().sum()
            if invalid_dates > 0:
                log.append(f"Warning: {invalid_dates} rows with invalid dates removed")
                df = df.dropna(subset=['date'])
        
        return df, log
    
    def _normalize_numeric(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """
        Clean and standardize numeric columns.
        """
        log = []
        numeric_cols = ['impressions', 'clicks', 'spend', 'conversions', 
                       'conversion_value', 'reach', 'cpc', 'cpm', 'ctr']
        
        for col in numeric_cols:
            if col in df.columns:
                if df[col].dtype == 'object':
                    # Remove currency symbols, commas, percentage signs
                    df[col] = df[col].astype(str).str.replace(r'[$,]', '', regex=True)
                    df[col] = df[col].str.replace('%', '', regex=False)
                    
                    # Convert to numeric
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    log.append(f"Cleaned numeric column: {col}")
        
        return df, log
    
    def _normalize_currency(self, df: pd.DataFrame, target_currency: str = 'USD') -> Tuple[pd.DataFrame, List[str]]:
        """
        Convert all monetary values to a target currency.
        """
        log = []
        
        if target_currency not in self.CURRENCY_RATES:
            log.append(f"Currency {target_currency} not supported, keeping original values")
            return df, log
        
        # If a currency column exists, use it for conversion
        monetary_cols = ['spend', 'cpc', 'cpm', 'cpa', 'conversion_value']
        
        if 'currency' in df.columns:
            for col in monetary_cols:
                if col in df.columns:
                    # Convert each row based on its currency
                    def convert_row(row):
                        curr = row.get('currency', 'USD')
                        if pd.isna(curr) or curr.upper() not in self.CURRENCY_RATES:
                            return row[col]
                        rate = self.CURRENCY_RATES.get(curr.upper(), 1.0)
                        return row[col] * rate / self.CURRENCY_RATES[target_currency]
                    
                    df[col] = df.apply(convert_row, axis=1)
                    log.append(f"Converted {col} to {target_currency}")
        else:
            log.append(f"No currency column found, assuming all values are in {target_currency}")
        
        return df, log
    
    def _deduplicate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """
        Remove duplicate rows from the data.
        """
        log = []
        original_count = len(df)
        
        # Define key columns for deduplication
        key_cols = ['date', 'campaign_name', 'adset_name', 'ad_name', 'platform']
        existing_key_cols = [col for col in key_cols if col in df.columns]
        
        if existing_key_cols:
            df = df.drop_duplicates(subset=existing_key_cols, keep='last')
            duplicates_removed = original_count - len(df)
            
            if duplicates_removed > 0:
                log.append(f"Removed {duplicates_removed} duplicate rows based on: {existing_key_cols}")
        else:
            duplicates = df.duplicated().sum()
            if duplicates > 0:
                df = df.drop_duplicates()
                log.append(f"Removed {duplicates} exact duplicate rows")
        
        return df, log
    
    def _calculate_derived_metrics(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """
        Calculate derived metrics if source data is available.
        """
        log = []
        
        # Calculate CTR
        if 'ctr' not in df.columns and 'clicks' in df.columns and 'impressions' in df.columns:
            df['ctr'] = (df['clicks'] / df['impressions'] * 100).round(4)
            log.append("Calculated CTR = (clicks / impressions) * 100")
        
        # Calculate CPC
        if 'cpc' not in df.columns and 'spend' in df.columns and 'clicks' in df.columns:
            df['cpc'] = (df['spend'] / df['clicks']).round(4)
            log.append("Calculated CPC = spend / clicks")
        
        # Calculate CPM
        if 'cpm' not in df.columns and 'spend' in df.columns and 'impressions' in df.columns:
            df['cpm'] = (df['spend'] / df['impressions'] * 1000).round(4)
            log.append("Calculated CPM = (spend / impressions) * 1000")
        
        # Calculate CPA
        if 'cpa' not in df.columns and 'spend' in df.columns and 'conversions' in df.columns:
            df['cpa'] = (df['spend'] / df['conversions'].replace(0, np.nan)).round(4)
            log.append("Calculated CPA = spend / conversions")
        
        # Calculate ROAS
        if 'roas' not in df.columns and 'conversion_value' in df.columns and 'spend' in df.columns:
            df['roas'] = (df['conversion_value'] / df['spend'].replace(0, np.nan)).round(4)
            log.append("Calculated ROAS = conversion_value / spend")
        
        return df, log
    
    def _handle_missing_values(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """
        Handle missing values appropriately for each column type.
        """
        log = []
        
        # Fill missing numeric metrics with 0
        numeric_fill_zero = ['impressions', 'clicks', 'spend', 'conversions', 
                            'conversion_value', 'reach', 'video_views']
        for col in numeric_fill_zero:
            if col in df.columns:
                missing = df[col].isna().sum()
                if missing > 0:
                    df[col] = df[col].fillna(0)
                    log.append(f"Filled {missing} missing values in '{col}' with 0")
        
        # Fill missing text with empty string
        text_cols = ['campaign_name', 'adset_name', 'ad_name', 'keyword']
        for col in text_cols:
            if col in df.columns:
                missing = df[col].isna().sum()
                if missing > 0:
                    df[col] = df[col].fillna('')
                    log.append(f"Filled {missing} missing values in '{col}' with empty string")
        
        return df, log
    
    def get_normalization_log(self) -> List[Dict]:
        """
        Return the log of all normalizations performed.
        """
        return self.normalization_log
