"""
Extract customer and transaction data from DummyJSON API.
Includes retry logic and PII masking.
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from typing import Dict, Any, List
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from include.security import pii_masking
import os

# Configure logging
logger = logging.getLogger(__name__)

# API Configuration
API_CONFIG = {
    'base_url': 'https://dummyjson.com',
    'timeout_seconds': 30,
    'max_retries': 3,
    'backoff_factor': 2,
    'retry_status_codes': [429, 500, 502, 503, 504]
}

def extract_sales_data(**context) -> Dict[str, Any]:
    """Extract customer and cart data from DummyJSON API"""
    
    start_time = datetime.now()
    
    session = requests.Session()
    retry_strategy = Retry(
        total=API_CONFIG['max_retries'],
        backoff_factor=API_CONFIG['backoff_factor'],
        status_forcelist=API_CONFIG['retry_status_codes'],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Extract raw data
    carts_data = _extract_cart_data(session)
    users_data = _extract_user_profiles(session)

    # Apply PII masking if enabled
    if pii_masking.is_masking_enabled():
        logger.info("PII masking enabled - applying data protection")
        for user in users_data:
            user['email_hash'] = pii_masking.hash_email(user.get('email'))
            user['email'] = pii_masking.mask_email(user.get('email'))
            user['phone'] = pii_masking.mask_phone(user.get('phone'))
            user['firstName'] = pii_masking.mask_name(user.get('firstName'))
            user['lastName'] = pii_masking.mask_name(user.get('lastName'))
    
    execution_time = (datetime.now() - start_time).total_seconds()
    
    logger.info(f"Extraction completed: {len(users_data)} users, {len(carts_data)} carts in {execution_time:.2f}s")
    
    # Return simple summary with raw data
    return {
        'status': 'success',
        'users_extracted': len(users_data),
        'carts_extracted': len(carts_data),
        'execution_time_seconds': execution_time,
        'users_data': users_data,
        'carts_data': carts_data
    }

def _extract_cart_data(session: requests.Session) -> List[Dict]:
    """Fetch cart data from API"""
    try:
        headers = {
            'User-Agent': 'CustomerRiskPlatform/1.0',
            'Accept': 'application/json'
        }
        
        response = session.get(
            f"{API_CONFIG['base_url']}/carts?limit=0",  # limit=0 trae TODOS
            headers=headers,
            timeout=API_CONFIG['timeout_seconds']
        )

        response.raise_for_status()
        api_data = response.json()
        
        if 'carts' not in api_data:
            raise ValueError("API response missing 'carts' field")
            
        carts_data = api_data['carts']
        logger.info(f"Successfully fetched {len(carts_data)} carts from DummyJSON API")
        return carts_data
        
    except requests.exceptions.Timeout:
        logger.error("Carts API request timed out after 30 seconds")
        raise
    except requests.exceptions.ConnectionError:
        logger.error("Failed to connect to DummyJSON Carts API")
        raise
    except requests.exceptions.HTTPError as e:
        logger.error(f"Carts API returned HTTP error: {e.response.status_code}")
        raise
    except ValueError as e:
        logger.error(f"Invalid carts API response format: {str(e)}")
        raise
    finally:
        session.close()

def _extract_user_profiles(session: requests.Session) -> List[Dict]:
    """Fetch user profiles from API"""
    try:
        user_session = requests.Session()
        # Reuse retry adapter
        adapter = session.adapters.get('https://')
        user_session.mount("http://", adapter)
        user_session.mount("https://", adapter)
        
        headers = {
            'User-Agent': 'CustomerRiskPlatform/1.0',
            'Accept': 'application/json'
        }

        user_response = user_session.get(
            f"{API_CONFIG['base_url']}/users?limit=0",  # limit=0 trae TODOS
            headers=headers,
            timeout=API_CONFIG['timeout_seconds']
        )

        user_response.raise_for_status()
        
        users_data = user_response.json().get('users', [])
        logger.info(f"Successfully fetched {len(users_data)} user profiles for Customer 360")
        return users_data
        
    except Exception as e:
        logger.warning(f"Failed to fetch user data: {str(e)}. Proceeding with cart data only.")
        return []
    finally:
        user_session.close()

