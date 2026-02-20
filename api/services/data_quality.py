"""
Data Quality Service
Performs data quality checks, anomaly detection, and validation
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from scipy import stats
import warnings
warnings.filterwarnings('ignore')


class DataQualityService:
    """
    Service for checking data quality and detecting anomalies in marketing data.
    """
    
    # Thresholds for anomaly detection
    ANOMALY_THRESHOLDS = {
        'z_score': 2.5,  # Standard deviations from mean
        'iqr_multiplier': 1.5,  # IQR multiplier for outlier detection
        'min_sample_size': 10,  # Minimum samples for statistical tests
        'ctr_range': (0.0, 50.0),  # Valid CTR percentage range
        'cpc_range': (0.0, 1000.0),  # Valid CPC range in USD
        'cpm_range': (0.0, 500.0),  # Valid CPM range in USD
        'roas_range': (0.0, 100.0),  # Valid ROAS range
    }
    
    def __init__(self):
        self.quality_reports = []
    
    def check_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Perform comprehensive data quality checks.
        """
        report = {
            'timestamp': datetime.now().isoformat(),
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'checks': {},
            'issues': [],
            'warnings': [],
            'score': 100.0  # Start with perfect score, deduct for issues
        }
        
        # Run all checks
        report['checks']['completeness'] = self._check_completeness(df)
        report['checks']['uniqueness'] = self._check_uniqueness(df)
        report['checks']['validity'] = self._check_validity(df)
        report['checks']['consistency'] = self._check_consistency(df)
        report['checks']['timeliness'] = self._check_timeliness(df)
        
        # Aggregate issues
        for check_name, check_result in report['checks'].items():
            if check_result.get('issues'):
                report['issues'].extend(check_result['issues'])
            if check_result.get('warnings'):
                report['warnings'].extend(check_result['warnings'])
            # Update score
            report['score'] -= check_result.get('penalty', 0)
        
        # Ensure score doesn't go below 0
        report['score'] = max(0, report['score'])
        
        # Determine quality grade
        report['grade'] = self._calculate_grade(report['score'])
        
        self.quality_reports.append(report)
        
        return report
    
    def detect_anomalies(self, df: pd.DataFrame, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Detect anomalies in numeric columns using statistical methods.
        """
        anomalies = {
            'timestamp': datetime.now().isoformat(),
            'method': 'z_score_and_iqr',
            'columns_analyzed': [],
            'anomalies': [],
            'summary': {}
        }
        
        # Default columns to check
        if columns is None:
            columns = ['spend', 'clicks', 'impressions', 'conversions', 'ctr', 'cpc', 'cpm', 'roas']
        
        for col in columns:
            if col not in df.columns:
                continue
            
            if df[col].dtype not in [np.float64, np.int64, float, int]:
                continue
            
            col_data = df[col].dropna()
            
            if len(col_data) < self.ANOMALY_THRESHOLDS['min_sample_size']:
                continue
            
            anomalies['columns_analyzed'].append(col)
            
            # Method 1: Z-Score
            mean = col_data.mean()
            std = col_data.std()
            
            if std > 0:
                z_scores = np.abs((col_data - mean) / std)
                z_outliers = col_data[z_scores > self.ANOMALY_THRESHOLDS['z_score']]
                
                for idx in z_outliers.index:
                    value = df.loc[idx, col]
                    z_score = z_scores.loc[idx]
                    anomalies['anomalies'].append({
                        'type': 'z_score_outlier',
                        'column': col,
                        'row_index': int(idx),
                        'value': float(value),
                        'z_score': float(z_score),
                        'expected_range': f"{mean - 2*std:.2f} to {mean + 2*std:.2f}",
                        'severity': 'high' if z_score > 3 else 'medium'
                    })
            
            # Method 2: IQR
            Q1 = col_data.quantile(0.25)
            Q3 = col_data.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - self.ANOMALY_THRESHOLDS['iqr_multiplier'] * IQR
            upper_bound = Q3 + self.ANOMALY_THRESHOLDS['iqr_multiplier'] * IQR
            
            iqr_outliers = col_data[(col_data < lower_bound) | (col_data > upper_bound)]
            
            for idx in iqr_outliers.index:
                # Avoid duplicate entries
                existing = [a for a in anomalies['anomalies'] 
                           if a['row_index'] == idx and a['column'] == col]
                if not existing:
                    value = df.loc[idx, col]
                    anomalies['anomalies'].append({
                        'type': 'iqr_outlier',
                        'column': col,
                        'row_index': int(idx),
                        'value': float(value),
                        'expected_range': f"{lower_bound:.2f} to {upper_bound:.2f}",
                        'severity': 'medium'
                    })
            
            # Summary stats
            anomalies['summary'][col] = {
                'mean': float(mean),
                'std': float(std),
                'min': float(col_data.min()),
                'max': float(col_data.max()),
                'outlier_count': len([a for a in anomalies['anomalies'] if a['column'] == col])
            }
        
        return anomalies
    
    def detect_performance_anomalies(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Detect performance anomalies specific to marketing data.
        """
        performance_issues = {
            'timestamp': datetime.now().isoformat(),
            'issues': [],
            'recommendations': []
        }
        
        # Check for campaigns with very high CPA
        if 'cpa' in df.columns and 'campaign_name' in df.columns:
            cpa_threshold = df['cpa'].quantile(0.9)
            high_cpa = df[df['cpa'] > cpa_threshold][['campaign_name', 'cpa', 'spend', 'conversions']]
            
            for _, row in high_cpa.iterrows():
                performance_issues['issues'].append({
                    'type': 'high_cpa',
                    'campaign': row['campaign_name'],
                    'metric': 'cpa',
                    'value': float(row['cpa']),
                    'threshold': float(cpa_threshold),
                    'recommendation': f"Consider pausing or optimizing campaign '{row['campaign_name']}' - CPA is ${row['cpa']:.2f} (above 90th percentile)"
                })
        
        # Check for campaigns with very low CTR
        if 'ctr' in df.columns and 'campaign_name' in df.columns:
            ctr_threshold = df['ctr'].quantile(0.1)
            low_ctr = df[df['ctr'] < ctr_threshold][['campaign_name', 'ctr', 'impressions']]
            
            for _, row in low_ctr.iterrows():
                if row['impressions'] > 1000:  # Only flag if significant impressions
                    performance_issues['issues'].append({
                        'type': 'low_ctr',
                        'campaign': row['campaign_name'],
                        'metric': 'ctr',
                        'value': float(row['ctr']),
                        'threshold': float(ctr_threshold),
                        'recommendation': f"Review ad creatives for '{row['campaign_name']}' - CTR is {row['ctr']:.2f}% (below 10th percentile)"
                    })
        
        # Check for budget drainage (high spend, low conversions)
        if 'spend' in df.columns and 'conversions' in df.columns and 'campaign_name' in df.columns:
            high_spend = df['spend'] > df['spend'].quantile(0.75)
            low_conv = df['conversions'] == 0
            budget_drain = df[high_spend & low_conv][['campaign_name', 'spend', 'impressions', 'clicks']]
            
            for _, row in budget_drain.iterrows():
                performance_issues['issues'].append({
                    'type': 'budget_drainage',
                    'campaign': row['campaign_name'],
                    'metric': 'spend',
                    'value': float(row['spend']),
                    'recommendation': f"PAUSE RECOMMENDED: '{row['campaign_name']}' spent ${row['spend']:.2f} with 0 conversions"
                })
        
        # Check for ROAS issues
        if 'roas' in df.columns and 'campaign_name' in df.columns:
            low_roas = df[df['roas'] < 1.0][['campaign_name', 'roas', 'spend', 'conversion_value']]
            
            for _, row in low_roas.iterrows():
                performance_issues['issues'].append({
                    'type': 'negative_roas',
                    'campaign': row['campaign_name'],
                    'metric': 'roas',
                    'value': float(row['roas']),
                    'recommendation': f"'{row['campaign_name']}' has ROAS of {row['roas']:.2f}x (losing money on ad spend)"
                })
        
        return performance_issues
    
    def _check_completeness(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Check for missing values across all columns.
        """
        result = {'issues': [], 'warnings': [], 'penalty': 0}
        
        total_cells = df.size
        missing_cells = df.isna().sum().sum()
        completeness_ratio = 1 - (missing_cells / total_cells)
        
        result['completeness_ratio'] = completeness_ratio
        result['missing_by_column'] = {col: int(df[col].isna().sum()) for col in df.columns}
        
        # Check critical columns
        critical_cols = ['campaign_name', 'date', 'spend']
        for col in critical_cols:
            if col in df.columns:
                missing = df[col].isna().sum()
                if missing > 0:
                    result['issues'].append(f"Critical column '{col}' has {missing} missing values ({missing/len(df)*100:.1f}%)")
                    result['penalty'] += min(20, missing / len(df) * 100)
        
        # Overall completeness check
        if completeness_ratio < 0.95:
            result['issues'].append(f"Data completeness is {completeness_ratio*100:.1f}% (below 95% threshold)")
            result['penalty'] += (1 - completeness_ratio) * 100
        
        return result
    
    def _check_uniqueness(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Check for duplicate rows and unique value distribution.
        """
        result = {'issues': [], 'warnings': [], 'penalty': 0}
        
        # Check exact duplicates
        duplicates = df.duplicated().sum()
        result['duplicate_rows'] = int(duplicates)
        result['uniqueness_ratio'] = 1 - (duplicates / len(df))
        
        if duplicates > 0:
            result['warnings'].append(f"Found {duplicates} duplicate rows ({duplicates/len(df)*100:.1f}%)")
            result['penalty'] += min(10, duplicates / len(df) * 50)
        
        # Check for low cardinality in expected high-cardinality columns
        if 'campaign_name' in df.columns:
            unique_campaigns = df['campaign_name'].nunique()
            if unique_campaigns < 2:
                result['warnings'].append(f"Only {unique_campaigns} unique campaign found")
        
        return result
    
    def _check_validity(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Check for invalid values in specific columns.
        """
        result = {'issues': [], 'warnings': [], 'penalty': 0}
        
        # Check CTR range
        if 'ctr' in df.columns:
            invalid_ctr = df[(df['ctr'] < self.ANOMALY_THRESHOLDS['ctr_range'][0]) | 
                           (df['ctr'] > self.ANOMALY_THRESHOLDS['ctr_range'][1])]
            if len(invalid_ctr) > 0:
                result['issues'].append(f"Found {len(invalid_ctr)} rows with invalid CTR values (outside 0-50% range)")
                result['penalty'] += min(10, len(invalid_ctr) / len(df) * 100)
        
        # Check for negative spend
        if 'spend' in df.columns:
            negative_spend = df[df['spend'] < 0]
            if len(negative_spend) > 0:
                result['issues'].append(f"Found {len(negative_spend)} rows with negative spend")
                result['penalty'] += min(15, len(negative_spend) / len(df) * 100)
        
        # Check for negative impressions/clicks
        for col in ['impressions', 'clicks', 'conversions']:
            if col in df.columns:
                negative = df[df[col] < 0]
                if len(negative) > 0:
                    result['issues'].append(f"Found {len(negative)} rows with negative {col}")
                    result['penalty'] += min(5, len(negative) / len(df) * 100)
        
        return result
    
    def _check_consistency(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Check for data consistency issues.
        """
        result = {'issues': [], 'warnings': [], 'penalty': 0}
        
        # Check if clicks > impressions (impossible)
        if 'clicks' in df.columns and 'impressions' in df.columns:
            inconsistent = df[df['clicks'] > df['impressions']]
            if len(inconsistent) > 0:
                result['issues'].append(f"Found {len(inconsistent)} rows where clicks > impressions (impossible)")
                result['penalty'] += min(15, len(inconsistent) / len(df) * 100)
        
        # Check CTR calculation consistency
        if 'ctr' in df.columns and 'clicks' in df.columns and 'impressions' in df.columns:
            calculated_ctr = (df['clicks'] / df['impressions'] * 100).fillna(0)
            ctr_diff = abs(df['ctr'] - calculated_ctr)
            inconsistent_ctr = (ctr_diff > 1).sum()  # More than 1% difference
            if inconsistent_ctr > 0:
                result['warnings'].append(f"Found {inconsistent_ctr} rows with inconsistent CTR values")
        
        return result
    
    def _check_timeliness(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Check date-related issues.
        """
        result = {'issues': [], 'warnings': [], 'penalty': 0}
        
        if 'date' in df.columns:
            try:
                dates = pd.to_datetime(df['date'])
                result['date_range'] = {
                    'start': str(dates.min()),
                    'end': str(dates.max())
                }
                
                # Check for future dates
                future_dates = dates > pd.Timestamp.now()
                if future_dates.sum() > 0:
                    result['warnings'].append(f"Found {future_dates.sum()} rows with future dates")
                
                # Check for very old data
                old_dates = dates < pd.Timestamp.now() - pd.DateOffset(years=2)
                if old_dates.sum() > 0:
                    result['warnings'].append(f"Found {old_dates.sum()} rows with dates older than 2 years")
                
            except Exception as e:
                result['warnings'].append(f"Could not parse dates: {str(e)}")
        
        return result
    
    def _calculate_grade(self, score: float) -> str:
        """
        Calculate a letter grade based on the quality score.
        """
        if score >= 95:
            return 'A+'
        elif score >= 90:
            return 'A'
        elif score >= 85:
            return 'B+'
        elif score >= 80:
            return 'B'
        elif score >= 70:
            return 'C'
        elif score >= 60:
            return 'D'
        else:
            return 'F'
    
    def get_quality_reports(self) -> List[Dict]:
        """
        Return all quality reports.
        """
        return self.quality_reports
