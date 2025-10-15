# include/customer_risk_platform/fraud_analyzers.py
"""
Fraud detection engine with multi-dimensional risk scoring
"""
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime, timedelta
import hashlib
import json
from dataclasses import dataclass
from enum import Enum
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

class RiskLevel(Enum):
    """Risk level classification"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class FraudIndicator:
    """Fraud indicator data structure"""
    indicator_type: str
    severity: str
    confidence: float
    description: str
    contributing_factors: List[str]

class FraudDetectionEngine:
    """Multi-dimensional fraud risk analyzer"""
    
    def __init__(self):
        # Risk scoring weights
        self.risk_weights = {
            'velocity_risk': 0.25,
            'geographic_risk': 0.20,
            'behavioral_risk': 0.20,
            'profile_risk': 0.15,
            'amount_risk': 0.10,
            'temporal_risk': 0.10
        }
        
        # Thresholds for risk classification
        self.risk_thresholds = {
            RiskLevel.LOW: 0.3,
            RiskLevel.MEDIUM: 0.6,
            RiskLevel.HIGH: 0.8,
            RiskLevel.CRITICAL: 0.9
        }
        
        # Industry fraud patterns
        self.fraud_patterns = {
            'high_velocity': {'threshold': 5, 'window_hours': 24},
            'geographic_anomaly': {'distance_km': 1000, 'time_hours': 6},
            'amount_spike': {'multiplier': 3.0, 'baseline_orders': 3},
            'new_customer_high_activity': {'orders_threshold': 10, 'amount_threshold': 2000}
        }

    def enrich_fraud_indicators(self, customer_profiles: List[Dict], **context) -> Dict[str, Any]:
        """
        Main function to enrich customer profiles with fraud indicators
        
        Args:
            customer_profiles: Transformed customer profiles from transformers module
            
        Returns:
            Dict containing enriched profiles with fraud analysis
        """
        try:
            logger.info(f"Starting fraud analysis for {len(customer_profiles)} customer profiles...")
            
            profiles_with_timestamps = self._simulate_transaction_timestamps(customer_profiles)

            risk_analyzed_profiles = []
            fraud_alerts = []
            
            for profile in profiles_with_timestamps:
                # Perform comprehensive fraud analysis
                fraud_analysis = self._perform_fraud_analysis(profile)
                
                # Enrich profile with fraud data
                enriched_profile = {
                    **profile,
                    **fraud_analysis['risk_assessment'],
                    'fraud_indicators': fraud_analysis['indicators'],
                    'ml_features': fraud_analysis['ml_features']
                }
                
                risk_analyzed_profiles.append(enriched_profile)
                
                # Generate alerts for high-risk customers
                if fraud_analysis['risk_assessment']['risk_level'] in ['high', 'critical']:
                    fraud_alerts.append(self._create_fraud_alert(enriched_profile))
            
            # Calculate metrics
            analysis_metrics = self._calculate_fraud_metrics(risk_analyzed_profiles)
            
            logger.info(f"Fraud analysis completed. {len(fraud_alerts)} high-risk customers identified")
            
            return {
                'risk_analyzed_profiles': risk_analyzed_profiles,
                'fraud_alerts': fraud_alerts,
                'fraud_metrics': analysis_metrics,
                'analysis_timestamp': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error in fraud analysis: {str(e)}")
            raise


    def _simulate_transaction_timestamps(self, profiles: List[Dict]) -> List[Dict]:
        """
        Simulate realistic transaction timestamps for fraud analysis
        Since DummyJSON doesn't provide timestamps, we create realistic patterns
        """
        enriched_profiles = []
        
        for profile in profiles:
            customer_id = profile['customer_id']
            total_orders = profile.get('total_orders', 0)
            
            # Generate realistic transaction timeline
            if total_orders > 0:

                base_date = datetime.utcnow() - timedelta(days=90)
                np.random.seed(customer_id)

                transaction_timestamps = []
                for i in range(total_orders):
                    # Distribute transactions over time with some clustering for realism
                    days_offset = np.random.exponential(scale=90/total_orders) * i
                    hours_offset = np.random.uniform(6, 22)  # Business hours bias
                    
                    transaction_time = base_date + timedelta(
                        days=min(89, days_offset), 
                        hours=hours_offset
                    )
                    transaction_timestamps.append(transaction_time)
                
                # Sort chronologically
                transaction_timestamps.sort()
                
                profile['transaction_timestamps'] = [ts.isoformat() for ts in transaction_timestamps]
                profile['first_transaction'] = transaction_timestamps[0].isoformat()
                profile['last_transaction'] = transaction_timestamps[-1].isoformat()
                
                # Calculate velocity metrics
                profile['transaction_velocity'] = self._calculate_velocity_metrics(transaction_timestamps)
            else:
                profile['transaction_timestamps'] = []
                profile['transaction_velocity'] = {'max_daily': 0, 'avg_daily': 0}
            
            enriched_profiles.append(profile)
            
        return enriched_profiles

    def _perform_fraud_analysis(self, profile: Dict) -> Dict[str, Any]:
        """
        Perform comprehensive fraud analysis on a single customer profile
        """
        indicators = []
        risk_scores = {}
        
        # 1. Velocity Risk Analysis
        velocity_risk, velocity_indicators = self._analyze_velocity_risk(profile)
        risk_scores['velocity_risk'] = velocity_risk
        indicators.extend(velocity_indicators)
        
        # 2. Geographic Risk Analysis
        geographic_risk, geo_indicators = self._analyze_geographic_risk(profile)
        risk_scores['geographic_risk'] = geographic_risk
        indicators.extend(geo_indicators)
        
        # 3. Behavioral Risk Analysis
        behavioral_risk, behavioral_indicators = self._analyze_behavioral_risk(profile)
        risk_scores['behavioral_risk'] = behavioral_risk
        indicators.extend(behavioral_indicators)
        
        # 4. Profile Risk Analysis
        profile_risk, profile_indicators = self._analyze_profile_risk(profile)
        risk_scores['profile_risk'] = profile_risk
        indicators.extend(profile_indicators)
        
        # 5. Amount Risk Analysis
        amount_risk, amount_indicators = self._analyze_amount_risk(profile)
        risk_scores['amount_risk'] = amount_risk
        indicators.extend(amount_indicators)
        
        # 6. Temporal Risk Analysis
        temporal_risk, temporal_indicators = self._analyze_temporal_risk(profile)
        risk_scores['temporal_risk'] = temporal_risk
        indicators.extend(temporal_indicators)
        
        # Calculate composite risk score
        composite_score = self._calculate_composite_risk_score(risk_scores)
        risk_level = self._determine_risk_level(composite_score)
        
        # Generate ML-ready features
        ml_features = self._extract_ml_features(profile, risk_scores)
        
        return {
            'risk_assessment': {
                'composite_risk_score': composite_score,
                'risk_level': risk_level.value,
                'individual_risk_scores': risk_scores,
                'risk_classification_confidence': self._calculate_confidence(composite_score)
            },
            'indicators': [indicator.__dict__ for indicator in indicators],
            'ml_features': ml_features
        }

    def _analyze_velocity_risk(self, profile: Dict) -> Tuple[float, List[FraudIndicator]]:
        """Check transaction velocity"""
        indicators = []
        risk_score = 0.0
        
        velocity_data = profile.get('transaction_velocity', {})
        max_daily = velocity_data.get('max_daily', 0)
        total_orders = profile.get('total_orders', 0)
        
        # High velocity pattern detection
        if max_daily >= 5:
            risk_score += 0.6
            indicators.append(FraudIndicator(
                indicator_type="HIGH_VELOCITY",
                severity="high",
                confidence=0.8,
                description=f"Customer made {max_daily} transactions in single day",
                contributing_factors=["unusual_transaction_frequency"]
            ))
        
        # Burst pattern detection (many transactions in short time)
        if total_orders >= 10 and len(profile.get('transaction_timestamps', [])) > 0:
            # Simulate burst detection
            customer_hash = hash(str(profile['customer_id']))
            if customer_hash % 20 == 0:  # 5% chance of burst pattern
                risk_score += 0.4
                indicators.append(FraudIndicator(
                    indicator_type="BURST_PATTERN",
                    severity="medium",
                    confidence=0.7,
                    description="Detected burst of transactions in short timeframe",
                    contributing_factors=["transaction_clustering"]
                ))
        
        return min(1.0, risk_score), indicators

    def _analyze_geographic_risk(self, profile: Dict) -> Tuple[float, List[FraudIndicator]]:
        """Check geographic patterns"""
        indicators = []
        risk_score = 0.0
        
        country = profile.get('country', '').lower()
        
        # International transaction risk
        if country not in ['united states', 'canada', 'united kingdom']:
            risk_score += 0.3
            indicators.append(FraudIndicator(
                indicator_type="INTERNATIONAL_PROFILE",
                severity="low",
                confidence=0.6,
                description=f"Customer located in {country}",
                contributing_factors=["geographic_location"]
            ))
        
        # Simulate impossible travel scenarios
        customer_hash = hash(str(profile['customer_id']))
        if customer_hash % 25 == 0:  # 4% chance of impossible travel
            risk_score += 0.7
            indicators.append(FraudIndicator(
                indicator_type="IMPOSSIBLE_TRAVEL",
                severity="high",
                confidence=0.9,
                description="Detected transactions from impossible geographic locations",
                contributing_factors=["geographic_anomaly", "location_spoofing"]
            ))
        
        return min(1.0, risk_score), indicators

    def _analyze_behavioral_risk(self, profile: Dict) -> Tuple[float, List[FraudIndicator]]:
        """Check behavioral patterns"""
        indicators = []
        risk_score = 0.0
        
        # New customer with high activity
        total_spent = profile.get('total_spent', 0)
        total_orders = profile.get('total_orders', 0)
        customer_segment = profile.get('customer_segment', 'new')
        
        if customer_segment == 'new' and total_spent > 2000:
            risk_score += 0.5
            indicators.append(FraudIndicator(
                indicator_type="NEW_CUSTOMER_HIGH_SPENDING",
                severity="medium",
                confidence=0.7,
                description=f"New customer with high spending: ${total_spent}",
                contributing_factors=["unusual_behavior_pattern"]
            ))
        
        # Unusual product diversity
        diversity_score = profile.get('product_diversity_score', 0)
        if diversity_score > 0.9 and total_orders >= 5:
            risk_score += 0.3
            indicators.append(FraudIndicator(
                indicator_type="UNUSUAL_PRODUCT_DIVERSITY",
                severity="low",
                confidence=0.6,
                description="Unusually diverse product purchase pattern",
                contributing_factors=["behavioral_anomaly"]
            ))
        
        return min(1.0, risk_score), indicators

    def _analyze_profile_risk(self, profile: Dict) -> Tuple[float, List[FraudIndicator]]:
        """Check profile completeness"""
        indicators = []
        risk_score = 0.0
        
        profile_completeness = profile.get('profile_completeness', 1.0)
        
        # Incomplete profile risk
        if profile_completeness < 0.5:
            risk_score += 0.4
            indicators.append(FraudIndicator(
                indicator_type="INCOMPLETE_PROFILE",
                severity="medium",
                confidence=0.8,
                description=f"Profile only {profile_completeness*100:.1f}% complete",
                contributing_factors=["missing_information"]
            ))
        
        # Email domain analysis
        email = profile.get('email', '')
        if email and ('temp' in email or 'disposable' in email):
            risk_score += 0.6
            indicators.append(FraudIndicator(
                indicator_type="SUSPICIOUS_EMAIL",
                severity="high",
                confidence=0.9,
                description="Potentially disposable email address",
                contributing_factors=["email_risk"]
            ))
        
        return min(1.0, risk_score), indicators

    def _analyze_amount_risk(self, profile: Dict) -> Tuple[float, List[FraudIndicator]]:
        """Check transaction amounts"""
        indicators = []
        risk_score = 0.0
        
        avg_order_value = profile.get('avg_order_value', 0)
        total_spent = profile.get('total_spent', 0)
        
        # Unusually high order values
        if avg_order_value > 1000:
            risk_score += 0.4
            indicators.append(FraudIndicator(
                indicator_type="HIGH_AVERAGE_ORDER",
                severity="medium",
                confidence=0.7,
                description=f"High average order value: ${avg_order_value:.2f}",
                contributing_factors=["amount_anomaly"]
            ))
        
        # Round number bias (fraudsters often use round numbers)
        if total_spent > 0 and total_spent % 100 == 0:
            risk_score += 0.2
            indicators.append(FraudIndicator(
                indicator_type="ROUND_NUMBER_BIAS",
                severity="low",
                confidence=0.5,
                description="Transaction amounts show round number pattern",
                contributing_factors=["amount_pattern"]
            ))
        
        return min(1.0, risk_score), indicators

    def _analyze_temporal_risk(self, profile: Dict) -> Tuple[float, List[FraudIndicator]]:
        """Check timing patterns"""
        indicators = []
        risk_score = 0.0
        
        timestamps = profile.get('transaction_timestamps', [])
        
        if timestamps:
            # Simulate off-hours transaction analysis
            customer_hash = hash(str(profile['customer_id']))
            if customer_hash % 15 == 0:  # 6.7% chance of off-hours pattern
                risk_score += 0.3
                indicators.append(FraudIndicator(
                    indicator_type="OFF_HOURS_ACTIVITY",
                    severity="low",
                    confidence=0.6,
                    description="Unusual transaction timing patterns detected",
                    contributing_factors=["temporal_anomaly"]
                ))
        
        return min(1.0, risk_score), indicators

    def _calculate_velocity_metrics(self, timestamps: List[datetime]) -> Dict[str, float]:
        """Calculate transaction velocity metrics"""
        if not timestamps:
            return {'max_daily': 0, 'avg_daily': 0}
        
        # Group by date
        daily_counts = {}
        for ts in timestamps:
            date_key = ts.date()
            daily_counts[date_key] = daily_counts.get(date_key, 0) + 1
        
        counts = list(daily_counts.values())
        return {
            'max_daily': max(counts) if counts else 0,
            'avg_daily': sum(counts) / len(counts) if counts else 0
        }

    def _calculate_composite_risk_score(self, risk_scores: Dict[str, float]) -> float:
        """Calculate weighted composite risk score"""
        composite = 0.0
        for risk_type, score in risk_scores.items():
            weight = self.risk_weights.get(risk_type, 0)
            composite += score * weight
        return min(1.0, composite)

    def _determine_risk_level(self, composite_score: float) -> RiskLevel:
        """Determine risk level based on composite score"""
        if composite_score >= self.risk_thresholds[RiskLevel.CRITICAL]:
            return RiskLevel.CRITICAL
        elif composite_score >= self.risk_thresholds[RiskLevel.HIGH]:
            return RiskLevel.HIGH
        elif composite_score >= self.risk_thresholds[RiskLevel.MEDIUM]:
            return RiskLevel.MEDIUM
        else:
            return RiskLevel.LOW

    def _calculate_confidence(self, risk_score: float) -> float:
        """Calculate confidence in risk assessment"""
        # Higher scores have higher confidence, but not linearly
        return min(0.95, 0.5 + (risk_score * 0.45))

    def _extract_ml_features(self, profile: Dict, risk_scores: Dict[str, float]) -> Dict[str, float]:
        """Extract ML-ready features for future model training"""
        return {
            # Demographic features
            'age': float(profile.get('age', 0)),
            'profile_completeness': profile.get('profile_completeness', 0),
            
            # Transaction features  
            'total_spent': float(profile.get('total_spent', 0)),
            'total_orders': float(profile.get('total_orders', 0)),
            'avg_order_value': float(profile.get('avg_order_value', 0)),
            'product_diversity_score': profile.get('product_diversity_score', 0),
            'customer_activity_score': profile.get('customer_activity_score', 0),
            
            # Risk component scores
            **{f'risk_{k}': v for k, v in risk_scores.items()},
            
            # Velocity features
            'max_daily_transactions': profile.get('transaction_velocity', {}).get('max_daily', 0),
            'avg_daily_transactions': profile.get('transaction_velocity', {}).get('avg_daily', 0),
            
            # Categorical features (encoded)
            'is_international': 1.0 if profile.get('country', '').lower() not in ['united states', 'canada'] else 0.0,
            'is_premium_customer': 1.0 if profile.get('customer_segment') == 'premium' else 0.0,
            'has_risk_indicators': 1.0 if len(profile.get('risk_indicators', [])) > 0 else 0.0
        }

    def _create_fraud_alert(self, profile: Dict) -> Dict[str, Any]:
        """Create fraud alert for high-risk customers"""
        return {
            'alert_id': f"FRAUD_{profile['customer_id']}_{int(datetime.utcnow().timestamp())}",
            'customer_id': profile['customer_id'],
            'customer_name': profile.get('full_name', 'Unknown'),
            'risk_level': profile['risk_level'],
            'risk_score': profile['composite_risk_score'],
            'primary_indicators': [indicator['indicator_type'] for indicator in profile['fraud_indicators'][:3]],
            'alert_timestamp': datetime.utcnow().isoformat(),
            'recommended_action': self._get_recommended_action(profile['risk_level']),
            'investigation_priority': self._get_investigation_priority(profile['composite_risk_score'])
        }

    def _get_recommended_action(self, risk_level: str) -> str:
        """Get recommended action based on risk level"""
        actions = {
            'critical': 'IMMEDIATE_INVESTIGATION_REQUIRED',
            'high': 'PRIORITY_REVIEW',
            'medium': 'ENHANCED_MONITORING',
            'low': 'STANDARD_MONITORING'
        }
        return actions.get(risk_level, 'STANDARD_MONITORING')

    def _get_investigation_priority(self, risk_score: float) -> str:
        """Get investigation priority based on risk score"""
        if risk_score >= 0.9:
            return 'P1_URGENT'
        elif risk_score >= 0.8:
            return 'P2_HIGH'
        elif risk_score >= 0.6:
            return 'P3_MEDIUM'
        else:
            return 'P4_LOW'

    def _calculate_fraud_metrics(self, profiles: List[Dict]) -> Dict[str, Any]:
        """Calculate overall fraud analysis metrics"""
        total_customers = len(profiles)
        risk_distribution = {}
        
        for level in RiskLevel:
            count = sum(1 for p in profiles if p.get('risk_level') == level.value)
            risk_distribution[level.value] = {
                'count': count,
                'percentage': (count / total_customers * 100) if total_customers > 0 else 0
            }
        
        avg_risk_score = sum(p.get('composite_risk_score', 0) for p in profiles) / total_customers if total_customers > 0 else 0
        
        return {
            'total_customers_analyzed': total_customers,
            'risk_distribution': risk_distribution,
            'average_risk_score': avg_risk_score,
            'high_risk_customers': risk_distribution['high']['count'] + risk_distribution['critical']['count'],
            'fraud_detection_rate': (risk_distribution['high']['count'] + risk_distribution['critical']['count']) / total_customers * 100 if total_customers > 0 else 0
        }

def _save_fraud_scores_to_staging(fraud_profiles: List[Dict]) -> None:
    """Save fraud analysis results to staging for dbt consumption"""
    if not fraud_profiles:
        logger.warning("No fraud scores to save")
        return
    
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        truncate_sql = "TRUNCATE TABLE staging.fraud_scores;"
        postgres_hook.run(truncate_sql)
        
        insert_sql = """
        INSERT INTO staging.fraud_scores 
        (customer_id, composite_risk_score, risk_level,
        velocity_risk, geographic_risk, behavioral_risk, 
        profile_risk, amount_risk, temporal_risk,
        fraud_indicators_json, ml_features_json)
        VALUES %s;
        """
        
        values_list = [
            (
                p['customer_id'],
                p['composite_risk_score'],
                p['risk_level'],
                p['individual_risk_scores']['velocity_risk'],
                p['individual_risk_scores']['geographic_risk'],
                p['individual_risk_scores']['behavioral_risk'],
                p['individual_risk_scores']['profile_risk'],
                p['individual_risk_scores']['amount_risk'],
                p['individual_risk_scores']['temporal_risk'],
                json.dumps(p['fraud_indicators']),
                json.dumps(p['ml_features'])
            )
            for p in fraud_profiles
        ]
        
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        
        from psycopg2.extras import execute_values
        execute_values(cursor, insert_sql, values_list, page_size=1000)
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info(f"✓ Saved {len(fraud_profiles)} fraud scores to staging.fraud_scores")
        
    except Exception as e:
        logger.error(f"Failed to save fraud scores: {str(e)}")
        raise

def enrich_fraud_indicators(**context) -> Dict[str, Any]:
    """
    Main function called by Airflow DAG for fraud analysis
    """
    try:
        # Read customer metrics from dbt model
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        query = "SELECT * FROM analytics.stg_customer_metrics"
        customer_profiles = postgres_hook.get_pandas_df(query).to_dict('records')
        
        if not customer_profiles:
            raise ValueError("No customer profiles found in analytics.stg_customer_metrics")
        
        logger.info(f"Processing {len(customer_profiles)} customer profiles for fraud analysis")
        
        # Run fraud detection
        fraud_engine = FraudDetectionEngine()
        fraud_results = fraud_engine.enrich_fraud_indicators(customer_profiles, **context)
        
        # Save fraud scores to staging
        _save_fraud_scores_to_staging(fraud_results['risk_analyzed_profiles'])
        
        # Log success
        fraud_metrics = fraud_results.get('fraud_metrics', {})
        high_risk_customers = fraud_metrics.get('high_risk_customers', 0)
        logger.info(f"✓ Fraud analysis completed: {high_risk_customers} high-risk customers identified")
        
        return fraud_results
        
    except Exception as e:
        logger.error(f"Error in fraud analysis: {str(e)}")
        raise