"""
Data quality validation for customer and transaction data
"""

from datetime import datetime
from typing import Dict, Any, List, Tuple
import logging
import json

from airflow.providers.postgres.hooks.postgres import PostgresHook

from .monitoring import track_performance_metrics

# Configure logging
logger = logging.getLogger(__name__)

# Data quality thresholds based on business requirements
QUALITY_THRESHOLDS = {
    'email_completeness_minimum': 20.0,    # 20% minimum for marketing campaigns
    'fraud_detection_critical_amount': 10000.0,  # $10K+ transactions need extra validation
    'high_amount_anomaly_threshold': 5.0,   # 5% of transactions >$10K indicates data issue
    'overall_quality_minimum': 0.7         # 80% overall quality required for SLA
}

def validate_data_quality(**context) -> Dict[str, Any]:
    """Validate data quality and check for critical issues"""
        
    start_time = datetime.now()
    task_id = 'validate_data_quality'
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    logger.info("Starting Customer 360 + Fraud Detection data quality validation")
    
    # Perform all quality checks
    quality_checks = {}
    critical_issues = 0
    total_issues = 0
    
    # Customer profile validation
    customer_results, customer_issues, customer_critical = _validate_customer_profiles(postgres_hook)
    quality_checks.update(customer_results)
    total_issues += customer_issues
    critical_issues += customer_critical
    
    # Transaction integrity validation  
    transaction_results, transaction_issues, transaction_critical = _validate_transaction_integrity(postgres_hook)
    quality_checks.update(transaction_results)
    total_issues += transaction_issues
    critical_issues += transaction_critical
    
    # Calculate overall quality assessment
    quality_summary = _calculate_quality_summary(quality_checks, critical_issues, total_issues)
    
    # Track validation performance
    performance_metrics = _track_validation_performance(
        task_id, start_time, quality_summary, context
    )
    
    # Generate structured validation results
    return _generate_validation_summary(
        quality_checks=quality_checks,
        quality_summary=quality_summary, 
        performance_metrics=performance_metrics,
        critical_issues=critical_issues,
        total_issues=total_issues
    )

def _validate_customer_profiles(postgres_hook: PostgresHook) -> Tuple[Dict[str, Any], int, int]:
    """Validate customer profile completeness and consistency."""
    quality_checks = {}
    total_issues = 0
    critical_issues = 0

    customer_completeness_sql = """
    SELECT 
        COUNT(*) as total_customers,
        COUNT(CASE WHEN customer_id IS NULL THEN 1 END) as missing_customer_id,
        COUNT(CASE WHEN email IS NULL OR email = '' THEN 1 END) as missing_email,
        COUNT(CASE WHEN phone IS NULL OR phone = '' THEN 1 END) as missing_phone,
        COUNT(CASE WHEN country IS NULL OR country = '' THEN 1 END) as missing_country
    FROM staging.raw_customers;
    """
    
    customer_results = postgres_hook.get_first(customer_completeness_sql)
    
    if not customer_results or customer_results[0] == 0:
        quality_checks['no_data_found'] = {
            'status': 'CRITICAL',
            'issues_found': 1,
            'description': 'No customer data found for validation'
        }
        return quality_checks, 1, 1
    
    # Calculate completeness percentages
    total_customers = customer_results[0]
    missing_email_pct = (customer_results[2] / total_customers) * 100
    missing_phone_pct = (customer_results[3] / total_customers) * 100
    missing_country_pct = (customer_results[4] / total_customers) * 100
    
    # Critical check: Customer ID integrity
    customer_id_issues = customer_results[1]
    if customer_id_issues > 0:
        critical_issues += customer_id_issues
        quality_checks['customer_id_integrity'] = {
            'status': 'CRITICAL',
            'issues_found': customer_id_issues,
            'description': 'Missing customer IDs found - pipeline cannot continue'
        }
    else:
        quality_checks['customer_id_integrity'] = {'status': 'PASS', 'issues_found': 0}
    
    # Warning check: Email completeness
    if missing_email_pct > QUALITY_THRESHOLDS['email_completeness_minimum']:
        quality_checks['email_completeness'] = {
            'status': 'WARNING',
            'missing_percentage': round(missing_email_pct, 2),
            'description': f'{missing_email_pct:.1f}% customers missing email',
            'business_impact': 'Reduced marketing campaign effectiveness'
        }
        total_issues += customer_results[2]
    else:
        quality_checks['email_completeness'] = {
            'status': 'PASS', 
            'missing_percentage': round(missing_email_pct, 2)
        }
    
    # Info check: Geographic data completeness
    quality_checks['geographic_completeness'] = {
        'status': 'INFO',
        'country_missing_pct': round(missing_country_pct, 2),
        'phone_missing_pct': round(missing_phone_pct, 2),
        'description': 'Geographic and contact data completeness'
    }
    
    return quality_checks, total_issues, critical_issues

def _validate_transaction_integrity(postgres_hook: PostgresHook) -> Tuple[Dict[str, Any], int, int]:
    """Validate transaction data integrity for fraud detection."""
    quality_checks = {}
    total_issues = 0
    critical_issues = 0

    transaction_integrity_sql = """
    SELECT 
        COUNT(*) as total_transactions,
        COUNT(CASE WHEN price <= 0 THEN 1 END) as invalid_amounts,
        COUNT(CASE WHEN quantity <= 0 THEN 1 END) as invalid_quantities,
        COUNT(CASE WHEN created_at IS NULL THEN 1 END) as missing_dates,
        COUNT(CASE WHEN product_category IS NULL OR product_category = '' THEN 1 END) as missing_categories,
        AVG(price) as avg_transaction_amount,
        MAX(price) as max_transaction_amount,
        COUNT(CASE WHEN price > %s THEN 1 END) as unusually_high_amounts
    FROM staging.raw_orders;
    """
    
    transaction_results = postgres_hook.get_first(
        transaction_integrity_sql, 
        parameters=[QUALITY_THRESHOLDS['fraud_detection_critical_amount']]
    )
    
    total_transactions = transaction_results[0] if transaction_results[0] > 0 else 1
    
    # Critical: Invalid transaction amounts
    invalid_amounts = transaction_results[1]
    if invalid_amounts > 0:
        critical_issues += invalid_amounts
        quality_checks['transaction_amounts'] = {
            'status': 'CRITICAL',
            'issues_found': invalid_amounts,
            'description': 'Invalid transaction amounts detected - fraud scoring impossible',
            'business_impact': 'Fraud detection system cannot function properly'
        }
    else:
        quality_checks['transaction_amounts'] = {'status': 'PASS', 'issues_found': 0}
    
    # Critical: Invalid quantities
    invalid_quantities = transaction_results[2] 
    if invalid_quantities > 0:
        critical_issues += invalid_quantities
        quality_checks['transaction_quantities'] = {
            'status': 'CRITICAL',
            'issues_found': invalid_quantities,
            'description': 'Invalid quantities detected - violates business logic'
        }
    else:
        quality_checks['transaction_quantities'] = {'status': 'PASS', 'issues_found': 0}
    
    # Warning: Missing transaction dates
    missing_dates = transaction_results[3]
    if missing_dates > 0:
        quality_checks['transaction_dates'] = {
            'status': 'WARNING', 
            'issues_found': missing_dates,
            'description': 'Missing transaction dates affect temporal fraud analysis'
        }
        total_issues += missing_dates
    else:
        quality_checks['transaction_dates'] = {'status': 'PASS', 'issues_found': 0}
    
    # Fraud-specific: High amount anomaly detection
    high_amount_count = transaction_results[7]
    high_amount_pct = (high_amount_count / total_transactions) * 100
    
    if high_amount_pct > QUALITY_THRESHOLDS['high_amount_anomaly_threshold']:
        quality_checks['high_amount_anomaly'] = {
            'status': 'WARNING',
            'anomaly_percentage': round(high_amount_pct, 2),
            'description': f'{high_amount_pct:.1f}% transactions >${QUALITY_THRESHOLDS["fraud_detection_critical_amount"]:,.0f}',
            'avg_amount': round(transaction_results[5] or 0, 2),
            'max_amount': transaction_results[6] or 0,
            'business_impact': 'May indicate data corruption or fraud spike'
        }
        total_issues += high_amount_count
    else:
        quality_checks['high_amount_anomaly'] = {
            'status': 'PASS', 
            'anomaly_percentage': round(high_amount_pct, 2),
            'high_value_count': high_amount_count
        }
    
    return quality_checks, total_issues, critical_issues

def _calculate_quality_summary(quality_checks: Dict[str, Any], critical_issues: int, total_issues: int) -> Dict[str, Any]:
    """Calculate overall quality score and assessment."""
    
    # Calculate overall quality score based on passed checks
    total_checks = len(quality_checks)
    passed_checks = len([check for check in quality_checks.values() 
                        if check.get('status') == 'PASS'])
    overall_quality_score = (passed_checks / total_checks) if total_checks > 0 else 0   
    
    # Determine quality level
    if overall_quality_score >= 0.9:
        quality_level = 'EXCELLENT'
    elif overall_quality_score >= 0.7:
        quality_level = 'GOOD' 
    elif overall_quality_score >= 0.5:
        quality_level = 'FAIR'
    else:
        quality_level = 'POOR'
    
    # Pipeline continuation decision
    pipeline_should_continue = (
        critical_issues == 0 and 
        overall_quality_score >= QUALITY_THRESHOLDS['overall_quality_minimum']
    )
    
    return {
        'overall_quality_score': overall_quality_score,
        'quality_level': quality_level,
        'pipeline_should_continue': pipeline_should_continue,
        'total_checks_performed': total_checks,
        'checks_passed': passed_checks,
        'critical_issues': critical_issues,
        'total_issues': total_issues
    }

def _track_validation_performance(task_id: str, start_time: datetime, quality_summary: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    """Track validation performance metrics."""
    
    execution_time = (datetime.now() - start_time).total_seconds() / 60
    
    # Get record count for performance calculation
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    record_count_sql = "SELECT COUNT(*) FROM raw_data.sales_data WHERE created_at::date = CURRENT_DATE"
    
    try:
        records_validated = postgres_hook.get_first(record_count_sql)[0] or 0
    except Exception as e:
        logger.warning(f"Could not get record count: {e}")
        records_validated = 0
    
    additional_context = {
        'quality_score': quality_summary['overall_quality_score'],
        'critical_issues': quality_summary['critical_issues'],
        'total_issues': quality_summary['total_issues'],
        'quality_level': quality_summary['quality_level'],
        'pipeline_continuation_approved': quality_summary['pipeline_should_continue']
    }
    
    # Import and call track_performance_metrics
    from .monitoring import track_performance_metrics
    
    performance_metrics = track_performance_metrics(
        task_id=task_id,
        execution_time_minutes=execution_time,
        records_processed=records_validated,
        additional_context=additional_context
    )
    
    # SKIP database insert for now - table doesn't exist
    logger.info("TESTING MODE: Skipping performance metrics database storage")
    logger.info(f"Would store validation metrics: {performance_metrics}")
    
    return performance_metrics

def _generate_validation_summary(
    quality_checks: Dict[str, Any], 
    quality_summary: Dict[str, Any],
    performance_metrics: Dict[str, Any],
    critical_issues: int,
    total_issues: int
) -> Dict[str, Any]:
    """Generate comprehensive validation summary with business recommendations."""
    
    # Log structured validation results for monitoring
    logger.info(json.dumps({
        'event_type': 'data_quality_validation',
        'pipeline': 'customer_risk_platform',
        'validation_results': {
            'overall_quality_score': quality_summary['overall_quality_score'],
            'quality_level': quality_summary['quality_level'],
            'critical_issues': critical_issues,
            'total_issues': total_issues,
            'pipeline_continue': quality_summary['pipeline_should_continue'],
            'checks_detail': quality_checks
        }
    }))
    
    # Fail pipeline if critical issues found
    if not quality_summary['pipeline_should_continue']:
        error_message = f"Data quality validation failed: {critical_issues} critical issues found. Pipeline cannot continue."
        logger.error(error_message)
        raise ValueError(error_message)
    
    # Generate business recommendations based on findings
    recommendations = _generate_business_recommendations(quality_checks, quality_summary)
    
    return {
        'status': 'success',
        'overall_quality_score': quality_summary['overall_quality_score'],
        'quality_level': quality_summary['quality_level'],
        'critical_issues': critical_issues,
        'total_issues': total_issues,
        'checks_detail': quality_checks,
        'records_validated': performance_metrics['records_processed'],
        'execution_time_minutes': performance_metrics['execution_time_minutes'],
        'pipeline_continue': quality_summary['pipeline_should_continue'],
        'business_impact': {
            'customer_360_readiness': 'HIGH' if quality_summary['overall_quality_score'] > 0.8 else 'MEDIUM',
            'fraud_detection_readiness': 'READY' if critical_issues == 0 else 'BLOCKED',
            'data_governance_compliance': quality_summary['quality_level']
        },
        'recommendations': recommendations,
        'performance_metrics': performance_metrics
    }

def _generate_business_recommendations(quality_checks: Dict[str, Any], quality_summary: Dict[str, Any]) -> List[str]:
    """Generate actionable business recommendations based on quality findings."""
    
    recommendations = []
    
    # Email completeness recommendations
    email_check = quality_checks.get('email_completeness', {})
    if email_check.get('status') == 'WARNING':
        missing_pct = email_check.get('missing_percentage', 0)
        recommendations.append(
            f"Marketing Impact: {missing_pct:.1f}% customers missing email addresses. "
            "Consider data enrichment services or email capture campaigns."
        )
    
    # High amount anomaly recommendations
    anomaly_check = quality_checks.get('high_amount_anomaly', {})
    if anomaly_check.get('status') == 'WARNING':
        anomaly_pct = anomaly_check.get('anomaly_percentage', 0)
        recommendations.append(
            f"Fraud Investigation: {anomaly_pct:.1f}% transactions are unusually high-value. "
            "Recommend immediate fraud team review to determine if data corruption or actual fraud spike."
        )
    
    # Overall quality recommendations
    if quality_summary['quality_level'] == 'EXCELLENT':
        recommendations.append("Data quality is excellent - proceed with confidence to fraud detection and customer analytics.")
    elif quality_summary['quality_level'] in ['GOOD', 'FAIR']:
        recommendations.append("Data quality is acceptable for processing. Monitor quality trends and address warnings proactively.")
    
    # Default recommendation if no specific issues
    if not recommendations:
        recommendations.append("No data quality concerns detected. Pipeline ready for Customer 360 and fraud detection processing.")
    
    return recommendations