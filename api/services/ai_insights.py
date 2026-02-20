"""
AI Insights Engine Service
Generates AI-powered insights and recommendations from marketing data
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from datetime import datetime
import json
import requests
import os


class AIInsightsService:
    """
    Service for generating AI-powered insights from marketing data.
    Integrates with z-ai-web-dev-sdk for LLM capabilities.
    """
    
    def __init__(self):
        self.insights_history = []
        # API endpoint for z-ai-web-dev-sdk
        self.api_base = os.environ.get('AI_API_BASE', 'http://localhost:3000/api')
    
    def generate_insights(self, df: pd.DataFrame, 
                          question: Optional[str] = None,
                          focus_area: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate AI-powered insights from marketing data.
        
        Args:
            df: Marketing data DataFrame
            question: Optional specific question to answer
            focus_area: Optional area to focus on ('performance', 'budget', 'optimization')
        
        Returns:
            Dictionary with insights and recommendations
        """
        # Prepare data summary for AI
        data_summary = self._create_data_summary(df)
        
        # Calculate key metrics
        metrics = self._calculate_key_metrics(df)
        
        # Detect trends
        trends = self._detect_trends(df)
        
        # Generate insights using AI
        if question:
            ai_response = self._query_ai(question, data_summary, metrics, trends)
        else:
            ai_response = self._generate_general_insights(data_summary, metrics, trends, focus_area)
        
        # Compile results
        result = {
            'timestamp': datetime.now().isoformat(),
            'data_summary': data_summary,
            'metrics': metrics,
            'trends': trends,
            'ai_insights': ai_response.get('insights', []),
            'recommendations': ai_response.get('recommendations', []),
            'answer': ai_response.get('answer')
        }
        
        self.insights_history.append(result)
        
        return result
    
    def _create_data_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Create a summary of the data for AI context.
        """
        summary = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'columns': list(df.columns),
            'platforms': list(df['platform'].unique()) if 'platform' in df.columns else ['unknown'],
            'campaigns': df['campaign_name'].nunique() if 'campaign_name' in df.columns else 0
        }
        
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            summary['date_range'] = {
                'start': str(df['date'].min().date()),
                'end': str(df['date'].max().date())
            }
        
        return summary
    
    def _calculate_key_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate key performance metrics.
        """
        metrics = {}
        
        # Sum metrics
        for col in ['spend', 'impressions', 'clicks', 'conversions', 'conversion_value']:
            if col in df.columns:
                metrics[f'total_{col}'] = float(df[col].sum())
        
        # Average metrics
        for col in ['ctr', 'cpc', 'cpm', 'cpa', 'roas']:
            if col in df.columns:
                metrics[f'avg_{col}'] = float(df[col].mean())
        
        # Efficiency metrics
        if metrics.get('total_clicks') and metrics.get('total_impressions'):
            metrics['overall_ctr'] = metrics['total_clicks'] / metrics['total_impressions'] * 100
        
        if metrics.get('total_spend') and metrics.get('total_conversions'):
            metrics['overall_cpa'] = metrics['total_spend'] / metrics['total_conversions']
        
        if metrics.get('total_conversion_value') and metrics.get('total_spend'):
            metrics['overall_roas'] = metrics['total_conversion_value'] / metrics['total_spend']
        
        return metrics
    
    def _detect_trends(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Detect trends in the data.
        """
        trends = {}
        
        if 'date' not in df.columns:
            return trends
        
        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        
        # Sort by date
        df = df.sort_values('date')
        
        # Split into two halves
        mid_point = len(df) // 2
        first_half = df.iloc[:mid_point]
        second_half = df.iloc[mid_point:]
        
        # Compare metrics between halves
        for col in ['spend', 'clicks', 'impressions', 'conversions', 'ctr', 'cpc']:
            if col in df.columns:
                first_avg = first_half[col].mean()
                second_avg = second_half[col].mean()
                
                if first_avg > 0:
                    change = (second_avg - first_avg) / first_avg * 100
                    trends[col] = {
                        'change_percent': round(change, 2),
                        'direction': 'up' if change > 5 else ('down' if change < -5 else 'stable')
                    }
        
        return trends
    
    def _generate_general_insights(self, data_summary: Dict, metrics: Dict, 
                                   trends: Dict, focus_area: Optional[str]) -> Dict[str, Any]:
        """
        Generate general insights (fallback when AI is unavailable).
        """
        insights = []
        recommendations = []
        
        # ROAS insights
        if metrics.get('overall_roas'):
            roas = metrics['overall_roas']
            if roas < 1:
                insights.append({
                    'type': 'critical',
                    'category': 'performance',
                    'message': f"Overall ROAS is {roas:.2f}x - campaigns are losing money on ad spend",
                    'impact': 'high'
                })
                recommendations.append({
                    'priority': 'high',
                    'action': 'Immediately review campaigns with ROAS < 1 and consider pausing or optimizing',
                    'expected_impact': 'Prevent further budget waste'
                })
            elif roas > 3:
                insights.append({
                    'type': 'positive',
                    'category': 'performance',
                    'message': f"Strong ROAS of {roas:.2f}x indicates healthy campaign performance",
                    'impact': 'positive'
                })
                recommendations.append({
                    'priority': 'medium',
                    'action': 'Consider scaling campaigns with highest ROAS to maximize returns',
                    'expected_impact': 'Increase overall revenue'
                })
        
        # CTR insights
        if metrics.get('overall_ctr'):
            ctr = metrics['overall_ctr']
            if ctr < 1:
                insights.append({
                    'type': 'warning',
                    'category': 'engagement',
                    'message': f"Low CTR of {ctr:.2f}% suggests ad creatives may need refresh",
                    'impact': 'medium'
                })
                recommendations.append({
                    'priority': 'medium',
                    'action': 'A/B test new ad creatives and review targeting settings',
                    'expected_impact': 'Improve click-through rates by 20-50%'
                })
            elif ctr > 3:
                insights.append({
                    'type': 'positive',
                    'category': 'engagement',
                    'message': f"Excellent CTR of {ctr:.2f}% indicates strong ad relevance",
                    'impact': 'positive'
                })
        
        # Spend efficiency
        if metrics.get('total_spend') and metrics.get('total_conversions'):
            cpa = metrics.get('overall_cpa', 0)
            if cpa > 50:
                insights.append({
                    'type': 'warning',
                    'category': 'cost',
                    'message': f"High CPA of ${cpa:.2f} may indicate targeting or offer issues",
                    'impact': 'high'
                })
                recommendations.append({
                    'priority': 'high',
                    'action': 'Review landing page experience, offer relevance, and audience targeting',
                    'expected_impact': 'Reduce CPA by 20-40%'
                })
        
        # Trend insights
        for metric, trend_data in trends.items():
            direction = trend_data.get('direction')
            change = trend_data.get('change_percent', 0)
            
            if direction == 'down' and metric in ['conversions', 'ctr', 'roas']:
                insights.append({
                    'type': 'warning',
                    'category': 'trend',
                    'message': f"{metric.replace('_', ' ').title()} decreased by {abs(change):.1f}% in recent period",
                    'impact': 'medium'
                })
            elif direction == 'up' and metric in ['spend', 'cpc', 'cpa']:
                insights.append({
                    'type': 'warning',
                    'category': 'trend',
                    'message': f"{metric.replace('_', ' ').title()} increased by {change:.1f}% in recent period",
                    'impact': 'medium'
                })
        
        # Platform insights if multiple platforms
        if len(data_summary.get('platforms', [])) > 1:
            insights.append({
                'type': 'info',
                'category': 'platform',
                'message': f"Data includes {len(data_summary['platforms'])} platforms: {', '.join(data_summary['platforms'])}",
                'impact': 'neutral'
            })
            recommendations.append({
                'priority': 'low',
                'action': 'Compare platform performance to optimize budget allocation',
                'expected_impact': 'Improve overall efficiency by shifting budget to best-performing platforms'
            })
        
        return {
            'insights': insights,
            'recommendations': recommendations,
            'generated_by': 'rule_engine'
        }
    
    def _query_ai(self, question: str, data_summary: Dict, 
                  metrics: Dict, trends: Dict) -> Dict[str, Any]:
        """
        Query the AI service with a specific question.
        Falls back to rule-based insights if AI unavailable.
        """
        # For now, use rule-based insights with the question context
        # In production, this would call the z-ai-web-dev-sdk
        
        base_insights = self._generate_general_insights(data_summary, metrics, trends, None)
        
        # Add question-specific answer
        answer = self._generate_question_answer(question, metrics, trends)
        base_insights['answer'] = answer
        base_insights['generated_by'] = 'rule_engine_with_context'
        
        return base_insights
    
    def _generate_question_answer(self, question: str, metrics: Dict, trends: Dict) -> str:
        """
        Generate an answer to a specific question.
        """
        question_lower = question.lower()
        
        # ROAS questions
        if 'roas' in question_lower:
            roas = metrics.get('overall_roas', 0)
            if roas > 0:
                return f"Your overall ROAS is {roas:.2f}x. " + (
                    "This is excellent - for every $1 spent, you're earning ${:.2f} back. Consider scaling successful campaigns.".format(roas) if roas > 2
                    else "This is below target. Review campaigns with lowest ROAS and consider pausing or optimizing them."
                )
            return "Unable to calculate ROAS - conversion_value data may be missing."
        
        # CPA questions
        if 'cpa' in question_lower or 'cost per' in question_lower:
            cpa = metrics.get('overall_cpa', 0)
            if cpa > 0:
                return f"Your overall CPA is ${cpa:.2f}. " + (
                    "This is relatively high - consider reviewing targeting and landing pages to improve conversion rates."
                    if cpa > 50 else "This is within a reasonable range for most industries."
                )
            return "Unable to calculate CPA - conversions data may be missing."
        
        # CTR questions
        if 'ctr' in question_lower or 'click through' in question_lower:
            ctr = metrics.get('overall_ctr', 0)
            if ctr > 0:
                return f"Your overall CTR is {ctr:.2f}%. " + (
                    "This is excellent! Your ads are highly relevant to your audience."
                    if ctr > 2 else "This could be improved. Consider testing new ad creatives or refining targeting."
                )
            return "Unable to calculate CTR - clicks or impressions data may be missing."
        
        # Spend questions
        if 'spend' in question_lower or 'budget' in question_lower:
            spend = metrics.get('total_spend', 0)
            return f"Total spend in this dataset is ${spend:,.2f}. "
        
        # Performance questions
        if 'performance' in question_lower or 'how are' in question_lower:
            roas = metrics.get('overall_roas', 0)
            ctr = metrics.get('overall_ctr', 0)
            return f"Overall performance summary: ROAS is {roas:.2f}x, CTR is {ctr:.2f}%, total conversions: {metrics.get('total_conversions', 0):,.0f}. " + (
                "Campaigns are performing well!" if roas > 2 and ctr > 1.5
                else "There's room for optimization - review underperforming campaigns."
            )
        
        # Default response
        return f"Based on the data, your campaigns have generated {metrics.get('total_conversions', 0):,.0f} conversions with a total spend of ${metrics.get('total_spend', 0):,.2f}. The overall ROAS is {metrics.get('overall_roas', 0):.2f}x."
    
    def explain_anomaly(self, anomaly_data: Dict, context_df: pd.DataFrame) -> str:
        """
        Generate an explanation for a detected anomaly.
        """
        anomaly_type = anomaly_data.get('type', 'unknown')
        column = anomaly_data.get('column', '')
        value = anomaly_data.get('value', 0)
        z_score = anomaly_data.get('z_score', 0)
        
        explanations = {
            'z_score_outlier': f"The value {value:.2f} in column '{column}' is {z_score:.1f} standard deviations from the mean, indicating it's an unusual data point that may warrant investigation.",
            'iqr_outlier': f"The value {value:.2f} in column '{column}' falls outside the normal range (Q1-1.5*IQR to Q3+1.5*IQR), suggesting it may be an error or exceptional event.",
            'performance_drop': f"A significant drop in '{column}' was detected, which could indicate campaign issues, market changes, or data collection problems."
        }
        
        return explanations.get(anomaly_type, f"Anomaly detected in {column}: {value}")
    
    def get_insights_history(self) -> List[Dict]:
        """
        Return history of generated insights.
        """
        return self.insights_history
