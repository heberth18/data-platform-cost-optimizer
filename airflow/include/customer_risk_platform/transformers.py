import logging
from typing import Dict, List, Any
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

def _get_region_from_country(country: str) -> str:
    """
    Map country to geographic region for business analytics.
    More realistic than random assignment.
    """
    region_mapping = {
        # North America
        'United States': 'NORTH_AMERICA',
        'Canada': 'NORTH_AMERICA',
        'Mexico': 'NORTH_AMERICA',
        
        # South America
        'Brazil': 'SOUTH_AMERICA',
        'Argentina': 'SOUTH_AMERICA',
        'Chile': 'SOUTH_AMERICA',
        'Colombia': 'SOUTH_AMERICA',
        'Peru': 'SOUTH_AMERICA',
        
        # Europe
        'United Kingdom': 'EUROPE',
        'Germany': 'EUROPE',
        'France': 'EUROPE',
        'Spain': 'EUROPE',
        'Italy': 'EUROPE',
        'Netherlands': 'EUROPE',
        'Poland': 'EUROPE',
        'Sweden': 'EUROPE',
        'Norway': 'EUROPE',
        'Denmark': 'EUROPE',
        'Belgium': 'EUROPE',
        'Austria': 'EUROPE',
        'Switzerland': 'EUROPE',
        'Ireland': 'EUROPE',
        'Portugal': 'EUROPE',
        'Greece': 'EUROPE',
        'Czech Republic': 'EUROPE',
        'Finland': 'EUROPE',
        
        # Asia
        'China': 'ASIA',
        'Japan': 'ASIA',
        'India': 'ASIA',
        'South Korea': 'ASIA',
        'Singapore': 'ASIA',
        'Thailand': 'ASIA',
        'Vietnam': 'ASIA',
        'Indonesia': 'ASIA',
        'Malaysia': 'ASIA',
        'Philippines': 'ASIA',
        'Pakistan': 'ASIA',
        'Bangladesh': 'ASIA',
        'Taiwan': 'ASIA',
        
        # Oceania
        'Australia': 'OCEANIA',
        'New Zealand': 'OCEANIA',
        
        # Middle East
        'United Arab Emirates': 'MIDDLE_EAST',
        'Saudi Arabia': 'MIDDLE_EAST',
        'Israel': 'MIDDLE_EAST',
        'Turkey': 'MIDDLE_EAST',
        
        # Africa
        'South Africa': 'AFRICA',
        'Nigeria': 'AFRICA',
        'Egypt': 'AFRICA',
        'Kenya': 'AFRICA',
        'Morocco': 'AFRICA'
    }
    
    return region_mapping.get(country, 'OTHER')

def _save_raw_customers_to_staging(customers: List[Dict]) -> None:
    """Save raw customer data to staging (NO CLEANING)"""
    if not customers:
        logger.warning("No customers to save")
        return
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        truncate_sql = "TRUNCATE TABLE staging.raw_customers CASCADE;"
        postgres_hook.run(truncate_sql)
        
        insert_sql = """
        INSERT INTO staging.raw_customers 
        (customer_id, first_name, last_name, email, phone, birth_date, age, gender,
        city, state, country, postal_code, full_address,
        company_name, job_title, department, university,
        card_type, card_last_4, iban_country)
        VALUES %s;
        """
        
        values_list = [
            (
                c['customer_id'],
                c.get('first_name', ''),
                c.get('last_name', ''),
                c.get('email', ''),
                c.get('phone', ''),
                c.get('birth_date', ''),
                c.get('age', 0),
                c.get('gender', ''),
                c.get('city', ''),
                c.get('state', ''),
                c.get('country', ''),
                c.get('postal_code', ''),
                c.get('full_address', ''),
                c.get('company_name', ''),
                c.get('job_title', ''),
                c.get('department', ''),
                c.get('university', ''),
                c.get('card_type', ''),
                c.get('card_last_4', ''),
                c.get('iban_country', '')
            )
            for c in customers
        ]
        
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        
        from psycopg2.extras import execute_values
        execute_values(cursor, insert_sql, values_list, page_size=1000)
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info(f"✓ Saved {len(customers)} raw customers to staging.raw_customers")
        
    except Exception as e:
        logger.error(f"Failed to save raw customers: {str(e)}")
        raise


def _save_raw_orders_to_staging(orders: List[Dict]) -> None:
    """Save raw orders to staging (NO CLEANING)"""
    if not orders:
        logger.warning("No orders to save")
        return
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # PASO 1: Obtener lista de customer_ids válidos
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        
        cursor.execute("SELECT customer_id FROM staging.raw_customers")
        valid_customer_ids = {row[0] for row in cursor.fetchall()}
        
        logger.info(f"Found {len(valid_customer_ids)} valid customer IDs")
        
        # PASO 2: Filtrar órdenes huérfanas
        filtered_orders = [
            o for o in orders 
            if o['customer_id'] in valid_customer_ids
        ]
        
        orphaned_count = len(orders) - len(filtered_orders)
        if orphaned_count > 0:
            logger.warning(f"Filtered out {orphaned_count} orders with invalid customer_ids")
        
        if not filtered_orders:
            logger.warning("No valid orders to save after filtering")
            return
        
        # PASO 3: Truncar y guardar órdenes válidas
        truncate_sql = "TRUNCATE TABLE staging.raw_orders;"
        postgres_hook.run(truncate_sql)
        
        insert_sql = """
        INSERT INTO staging.raw_orders 
        (cart_id, customer_id, product_id, product_name, product_category,
        quantity, price, discount_percentage, line_total, region)
        VALUES %s;
        """
        
        values_list = [
            (
                o['cart_id'],
                o['customer_id'],
                o['product_id'],
                o.get('product_name', ''),
                o.get('product_category', ''),
                o['quantity'],
                o['price'],
                o.get('discount_percentage', 0),
                o['line_total'],
                o.get('region', 'OTHER') 
            )
            for o in filtered_orders
        ]
        
        from psycopg2.extras import execute_values
        execute_values(cursor, insert_sql, values_list, page_size=1000)
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info(f"✓ Saved {len(filtered_orders)} raw orders to staging.raw_orders")
        
    except Exception as e:
        logger.error(f"Failed to save raw orders: {str(e)}")
        raise


def _flatten_users_data(raw_users: List[Dict]) -> List[Dict]:
    """Flatten nested JSON structure from DummyJSON API"""
    flattened = []
    
    for user in raw_users:
        try:
            address = user.get('address', {})
            company = user.get('company', {})
            bank = user.get('bank', {})
            
            customer = {
                'customer_id': user.get('id'),
                'first_name': user.get('firstName', ''),
                'last_name': user.get('lastName', ''),
                'email': user.get('email', ''),
                'phone': user.get('phone', ''),
                'birth_date': user.get('birthDate', ''),
                'age': user.get('age', 0),
                'gender': user.get('gender', ''),
                
                'city': address.get('city', ''),
                'state': address.get('state', ''),
                'country': address.get('country', ''),
                'postal_code': address.get('postalCode', ''),
                'full_address': address.get('address', ''),
                
                'company_name': company.get('name', ''),
                'job_title': company.get('title', ''),
                'department': company.get('department', ''),
                'university': user.get('university', ''),
                
                'card_type': bank.get('cardType', ''),
                'card_last_4': bank.get('cardNumber', '')[-4:] if bank.get('cardNumber') else '',
                'iban_country': bank.get('iban', '')[:2] if bank.get('iban') else ''
            }
            
            flattened.append(customer)
            
        except Exception as e:
            logger.warning(f"Failed to flatten user {user.get('id')}: {str(e)}")
            continue
    
    return flattened

def _flatten_carts_data(raw_carts: List[Dict], users_lookup: Dict[int, Dict]) -> List[Dict]:
    """
    Flatten cart data into individual order lines.
    Includes region based on customer country.
    """
    orders = []
    
    for cart in raw_carts:
        try:
            cart_id = cart.get('id')
            customer_id = cart.get('userId')
            
            # Get customer data to infer region
            user = users_lookup.get(customer_id, {})
            country = user.get('address', {}).get('country', '')
            region = _get_region_from_country(country)
            
            for product in cart.get('products', []):
                order = {
                    'cart_id': cart_id,
                    'customer_id': customer_id,
                    'product_id': product.get('id'),
                    'product_name': product.get('title', ''),
                    'product_category': product.get('category', ''),
                    'quantity': product.get('quantity', 1),
                    'price': product.get('price', 0),
                    'discount_percentage': product.get('discountPercentage', 0),
                    'line_total': product.get('price', 0) * product.get('quantity', 1) * (1 - product.get('discountPercentage', 0)/100),
                    'region': region    
                }
                orders.append(order)
                
        except Exception as e:
            logger.warning(f"Failed to flatten cart {cart.get('id')}: {str(e)}")
            continue
    
    return orders

def transform_clean_data(**context) -> Dict[str, Any]:
    """
    Simplified transformation: Only flatten and move data.
    NO cleaning, NO aggregations - those happen in dbt.
    """
    try:
        extraction_results = context['task_instance'].xcom_pull(task_ids='extract_sales_data')
        
        if not extraction_results:
            raise ValueError("No extraction results found in XCom")
        
        raw_users = extraction_results.get('users_data', [])
        raw_carts = extraction_results.get('carts_data', [])

        if not raw_users or not raw_carts:
            raise ValueError(f"Empty data from API: {len(raw_users)} users, {len(raw_carts)} carts")
        
        if len(raw_users) == 0 or len(raw_carts) == 0:
            raise ValueError("API returned zero records")
        
        logger.info(f"✓ Valid data from XCom: {len(raw_users)} users, {len(raw_carts)} carts")
        
        logger.info(f"Starting transformation: {len(raw_users)} users, {len(raw_carts)} carts")
        
        # Step 1: Flatten nested JSON structures
        flattened_customers = _flatten_users_data(raw_users)

        # Create user lookup for region mapping
        users_lookup = {user['id']: user for user in raw_users}

        flattened_orders = _flatten_carts_data(raw_carts, users_lookup)
        
        logger.info(f"Flattened: {len(flattened_customers)} customers, {len(flattened_orders)} orders")
        
        # Step 2: Save to staging (NO cleaning)
        _save_raw_customers_to_staging(flattened_customers)
        _save_raw_orders_to_staging(flattened_orders)
        
        logger.info("✓ Transformation completed successfully")
        
        return {
            'customers_saved': len(flattened_customers),
            'orders_saved': len(flattened_orders),
            'transformation_timestamp': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in transformation: {str(e)}")
        raise