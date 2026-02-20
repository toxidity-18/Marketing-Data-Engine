"""
Multi-Platform Data Merger Service
Combines and aggregates marketing data from multiple advertising platforms
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import json


class DataMergerService:
    """
    Service for merging marketing data from multiple platforms.
    """

    # Common column name variants (case‑insensitive)
    DATE_COLUMN_CANDIDATES = ['date', 'day', 'report_date', 'stat_days', 'start date', 'end date']
    CAMPAIGN_COLUMN_CANDIDATES = ['campaign', 'campaign_name', 'campaign name', 'campaign id']
    PLATFORM_COLUMN_CANDIDATES = ['platform', 'source', 'channel', 'ad_platform']

    def __init__(self):
        self.merge_history = []

    def _find_column(self, df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
        """Return the first existing column name (case‑insensitive) from candidates."""
        df_cols_lower = {col.lower(): col for col in df.columns}
        for cand in candidates:
            if cand.lower() in df_cols_lower:
                return df_cols_lower[cand.lower()]
        return None

    def merge_datasets(self, datasets: List[pd.DataFrame],
                       platform_names: Optional[List[str]] = None,
                       merge_strategy: str = 'append') -> Dict[str, Any]:
        """
        Merge multiple datasets into a unified view.

        Args:
            datasets: List of DataFrames to merge
            platform_names: Optional list of platform names for each dataset
            merge_strategy: 'append' for stacking, 'join' for column-wise merge

        Returns:
            Dictionary with merged data and metadata
        """
        if not datasets:
            return {'success': False, 'error': 'No datasets provided'}

        # Add platform column if names provided
        if platform_names and len(platform_names) == len(datasets):
            for i, (df, platform) in enumerate(zip(datasets, platform_names)):
                if 'platform' not in df.columns:
                    datasets[i] = df.copy()
                    datasets[i]['platform'] = platform

        # Get common columns across all datasets
        common_cols = set(datasets[0].columns)
        for df in datasets[1:]:
            common_cols = common_cols.intersection(set(df.columns))

        all_cols = set()
        for df in datasets:
            all_cols = all_cols.union(set(df.columns))

        if merge_strategy == 'append':
            # Stack datasets (rows from each dataset)
            merged_df = pd.concat(datasets, ignore_index=True, sort=False)

        elif merge_strategy == 'join':
            # Merge on common columns (requires date or ID columns)
            # Try to find a date column or campaign column to join on
            join_col = self._find_column(datasets[0], self.DATE_COLUMN_CANDIDATES + self.CAMPAIGN_COLUMN_CANDIDATES)
            if not join_col or join_col not in common_cols:
                return {'success': False, 'error': 'No common join column (date or campaign) found'}

            merged_df = datasets[0]
            for df in datasets[1:]:
                merged_df = pd.merge(merged_df, df, on=join_col, how='outer', suffixes=('', '_dup'))
        else:
            return {'success': False, 'error': f'Unknown merge strategy: {merge_strategy}'}

        # Record merge
        merge_record = {
            'timestamp': datetime.now().isoformat(),
            'input_datasets': len(datasets),
            'platforms': platform_names,
            'strategy': merge_strategy,
            'common_columns': list(common_cols),
            'all_columns': list(all_cols),
            'output_rows': len(merged_df),
            'output_columns': len(merged_df.columns)
        }
        self.merge_history.append(merge_record)

        return {
            'success': True,
            'data': merged_df,
            'metadata': merge_record
        }

    def aggregate_by_date(self, df: pd.DataFrame,
                          date_granularity: str = 'daily') -> Dict[str, Any]:
        """
        Aggregate data by date with specified granularity.

        Args:
            df: Input DataFrame
            date_granularity: 'daily', 'weekly', 'monthly'

        Returns:
            Aggregated DataFrame with metrics summed/averaged appropriately
        """
        df = df.copy()

        # Find the date column (case‑insensitive)
        date_col = self._find_column(df, self.DATE_COLUMN_CANDIDATES)
        if not date_col:
            return {
                'success': False,
                'error': f"No date column found. Expected one of: {self.DATE_COLUMN_CANDIDATES}"
            }

        # Rename it to a standard name for internal use
        df.rename(columns={date_col: 'date'}, inplace=True)

        # ===== Robust date parsing (handles DD-MM-YYYY) =====
        df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
        df = df.dropna(subset=['date'])

        # Create period column based on granularity
        if date_granularity == 'daily':
            df['period'] = df['date'].dt.date
        elif date_granularity == 'weekly':
            df['period'] = df['date'].dt.to_period('W').astype(str)
        elif date_granularity == 'monthly':
            df['period'] = df['date'].dt.to_period('M').astype(str)
        else:
            return {'success': False, 'error': f'Unknown granularity: {date_granularity}'}

        # Define aggregation rules
        sum_cols = ['impressions', 'clicks', 'spend', 'conversions',
                    'conversion_value', 'reach', 'video_views']
        avg_cols = ['ctr', 'cpc', 'cpm', 'cpa', 'roas']

        # Filter to existing columns
        existing_sum_cols = [col for col in sum_cols if col in df.columns]
        existing_avg_cols = [col for col in avg_cols if col in df.columns]

        # Create aggregation dictionary
        agg_dict = {col: 'sum' for col in existing_sum_cols}
        agg_dict.update({col: 'mean' for col in existing_avg_cols})

        # Group by period and platform (if exists)
        group_cols = ['period']
        # Find platform column (case‑insensitive)
        platform_col = self._find_column(df, self.PLATFORM_COLUMN_CANDIDATES)
        if platform_col:
            # Standardize name
            if platform_col != 'platform':
                df.rename(columns={platform_col: 'platform'}, inplace=True)
            group_cols.append('platform')
        elif 'platform' in df.columns:
            group_cols.append('platform')

        # Perform aggregation
        aggregated = df.groupby(group_cols).agg(agg_dict).reset_index()

        # Recalculate derived metrics
        if 'clicks' in aggregated.columns and 'impressions' in aggregated.columns:
            aggregated['ctr'] = (aggregated['clicks'] / aggregated['impressions'] * 100).round(4)
        if 'spend' in aggregated.columns and 'clicks' in aggregated.columns:
            aggregated['cpc'] = (aggregated['spend'] / aggregated['clicks']).round(4)
        if 'spend' in aggregated.columns and 'impressions' in aggregated.columns:
            aggregated['cpm'] = (aggregated['spend'] / aggregated['impressions'] * 1000).round(4)

        return {
            'success': True,
            'data': aggregated,
            'granularity': date_granularity,
            'periods': len(aggregated['period'].unique())
        }

    def aggregate_by_campaign(self, df: pd.DataFrame,
                              include_platform_breakdown: bool = True) -> Dict[str, Any]:
        """
        Aggregate data by campaign with optional platform breakdown.
        """
        df = df.copy()

        # Find campaign column (case‑insensitive)
        campaign_col = self._find_column(df, self.CAMPAIGN_COLUMN_CANDIDATES)
        if not campaign_col:
            return {
                'success': False,
                'error': f"No campaign column found. Expected one of: {self.CAMPAIGN_COLUMN_CANDIDATES}"
            }
        # Standardize name
        if campaign_col != 'campaign_name':
            df.rename(columns={campaign_col: 'campaign_name'}, inplace=True)

        # Define aggregation rules
        sum_cols = ['impressions', 'clicks', 'spend', 'conversions', 'conversion_value']
        avg_cols = ['ctr', 'cpc', 'cpm', 'roas']

        existing_sum_cols = [col for col in sum_cols if col in df.columns]
        existing_avg_cols = [col for col in avg_cols if col in df.columns]

        agg_dict = {col: 'sum' for col in existing_sum_cols}
        agg_dict.update({col: 'mean' for col in existing_avg_cols})

        # Group by campaign
        if include_platform_breakdown:
            # Find platform column (case‑insensitive)
            platform_col = self._find_column(df, self.PLATFORM_COLUMN_CANDIDATES)
            if platform_col:
                if platform_col != 'platform':
                    df.rename(columns={platform_col: 'platform'}, inplace=True)
                grouped = df.groupby(['campaign_name', 'platform']).agg(agg_dict).reset_index()
            elif 'platform' in df.columns:
                grouped = df.groupby(['campaign_name', 'platform']).agg(agg_dict).reset_index()
            else:
                grouped = df.groupby('campaign_name').agg(agg_dict).reset_index()
        else:
            grouped = df.groupby('campaign_name').agg(agg_dict).reset_index()

        # Recalculate metrics
        if 'clicks' in grouped.columns and 'impressions' in grouped.columns:
            grouped['ctr'] = (grouped['clicks'] / grouped['impressions'] * 100).round(4)
        if 'spend' in grouped.columns and 'conversions' in grouped.columns:
            grouped['cpa'] = (grouped['spend'] / grouped['conversions'].replace(0, np.nan)).round(4)
        if 'conversion_value' in grouped.columns and 'spend' in grouped.columns:
            grouped['roas'] = (grouped['conversion_value'] / grouped['spend'].replace(0, np.nan)).round(4)

        return {
            'success': True,
            'data': grouped,
            'total_campaigns': grouped['campaign_name'].nunique()
        }

    def compare_platforms(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate platform-level comparison statistics.
        """
        df = df.copy()

        # Find platform column (case‑insensitive)
        platform_col = self._find_column(df, self.PLATFORM_COLUMN_CANDIDATES)
        if not platform_col:
            return {
                'success': False,
                'error': f"No platform column found. Expected one of: {self.PLATFORM_COLUMN_CANDIDATES}"
            }
        # Standardize name
        if platform_col != 'platform':
            df.rename(columns={platform_col: 'platform'}, inplace=True)

        metrics = ['spend', 'impressions', 'clicks', 'conversions', 'ctr', 'cpc', 'cpm', 'roas']
        existing_metrics = [col for col in metrics if col in df.columns]

        # Group by platform
        platform_stats = df.groupby('platform').agg({
            'spend': 'sum',
            'impressions': 'sum',
            'clicks': 'sum',
            'conversions': 'sum',
            'conversion_value': 'sum'
        }).reset_index() if all(col in df.columns for col in ['spend', 'impressions', 'clicks', 'conversions', 'conversion_value']) else None

        if platform_stats is not None:
            # Calculate efficiency metrics
            platform_stats['ctr'] = (platform_stats['clicks'] / platform_stats['impressions'] * 100).round(2)
            platform_stats['cpc'] = (platform_stats['spend'] / platform_stats['clicks']).round(2)
            platform_stats['cpm'] = (platform_stats['spend'] / platform_stats['impressions'] * 1000).round(2)
            platform_stats['cpa'] = (platform_stats['spend'] / platform_stats['conversions'].replace(0, np.nan)).round(2)
            platform_stats['roas'] = (platform_stats['conversion_value'] / platform_stats['spend'].replace(0, np.nan)).round(2)

        # Calculate share of spend
        if platform_stats is not None:
            total_spend = platform_stats['spend'].sum()
            platform_stats['spend_share'] = (platform_stats['spend'] / total_spend * 100).round(1)

        # Rank platforms by performance
        rankings = {}
        if platform_stats is not None:
            for metric in ['roas', 'ctr', 'cpa']:
                if metric in platform_stats.columns:
                    sorted_df = platform_stats.sort_values(metric, ascending=(metric == 'cpa'))
                    rankings[metric] = list(sorted_df['platform'].values)

        return {
            'success': True,
            'platform_stats': platform_stats.to_dict('records') if platform_stats is not None else [],
            'rankings': rankings,
            'total_platforms': df['platform'].nunique()
        }

    def create_unified_report(self, datasets: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Create a unified report from multiple platform datasets.

        Args:
            datasets: Dictionary mapping platform names to DataFrames

        Returns:
            Unified report with cross-platform analytics
        """
        # Add platform identifier to each dataset
        all_data = []
        for platform, df in datasets.items():
            df = df.copy()
            df['platform'] = platform
            all_data.append(df)

        # Merge all data
        if not all_data:
            return {'success': False, 'error': 'No data provided'}

        merged_df = pd.concat(all_data, ignore_index=True, sort=False)

        # Calculate overall metrics
        overall_metrics = {}
        for col in ['spend', 'impressions', 'clicks', 'conversions', 'conversion_value']:
            if col in merged_df.columns:
                overall_metrics[col] = float(merged_df[col].sum())

        # Calculate overall efficiency
        if overall_metrics.get('clicks') and overall_metrics.get('impressions'):
            overall_metrics['ctr'] = overall_metrics['clicks'] / overall_metrics['impressions'] * 100
        if overall_metrics.get('spend') and overall_metrics.get('clicks'):
            overall_metrics['cpc'] = overall_metrics['spend'] / overall_metrics['clicks']
        if overall_metrics.get('spend') and overall_metrics.get('conversions'):
            overall_metrics['cpa'] = overall_metrics['spend'] / overall_metrics['conversions']
        if overall_metrics.get('conversion_value') and overall_metrics.get('spend'):
            overall_metrics['roas'] = overall_metrics['conversion_value'] / overall_metrics['spend']

        # Platform comparison
        platform_comparison = self.compare_platforms(merged_df)

        # Date range analysis – find date column
        date_col = self._find_column(merged_df, self.DATE_COLUMN_CANDIDATES)
        if date_col:
            merged_df['date'] = pd.to_datetime(merged_df[date_col], dayfirst=True, errors='coerce')
            valid_dates = merged_df['date'].dropna()
            if not valid_dates.empty:
                date_range = {
                    'start': str(valid_dates.min()),
                    'end': str(valid_dates.max()),
                    'days': (valid_dates.max() - valid_dates.min()).days + 1
                }
            else:
                date_range = None
        else:
            date_range = None

        return {
            'success': True,
            'data': merged_df,
            'overall_metrics': overall_metrics,
            'platform_comparison': platform_comparison,
            'date_range': date_range,
            'summary': {
                'total_platforms': len(datasets),
                'total_campaigns': merged_df['campaign_name'].nunique() if 'campaign_name' in merged_df.columns else 0,
                'total_rows': len(merged_df)
            }
        }

    def get_merge_history(self) -> List[Dict]:
        """
        Return merge operation history.
        """
        return self.merge_history