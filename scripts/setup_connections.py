"""
Setup Airflow connections for the data platform
Run this script to configure database connections
"""

from airflow.models import Connection
from airflow.utils.db import provide_session

@provide_session
def create_connections(session=None):
    """Create database connections for the data platform"""
    
    # PostgreSQL connection
    postgres_conn = Connection(
        conn_id='postgres_default',
        conn_type='postgres',
        host='postgres',
        schema='data_platform',
        login='dataeng',
        password='dataeng123',
        port=5432
    )
    
    # Check if connection exists
    existing_conn = session.query(Connection).filter(
        Connection.conn_id == 'postgres_default'
    ).first()
    
    if not existing_conn:
        session.add(postgres_conn)
        session.commit()
        print("PostgreSQL connection created successfully")
    else:
        print("PostgreSQL connection already exists")

if __name__ == "__main__":
    create_connections()