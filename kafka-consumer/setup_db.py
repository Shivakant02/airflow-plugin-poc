#!/usr/bin/env python3
"""
Database setup script for the Kafka consumer
Creates the necessary table and indexes
"""

import psycopg2
import psycopg2.extras

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow'
}

def setup_database():
    """Setup the database table and indexes"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("🔧 Setting up database...")
        
        # Create table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS "airflow-poc" (
            id SERIAL PRIMARY KEY,
            info JSONB NOT NULL,
            type VARCHAR(50) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute(create_table_query)
        print("✅ Table 'airflow-poc' created/verified")
        
        # Create indexes for better performance
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_airflow_poc_type ON "airflow-poc" (type);',
            'CREATE INDEX IF NOT EXISTS idx_airflow_poc_created_at ON "airflow-poc" (created_at);',
            'CREATE INDEX IF NOT EXISTS idx_airflow_poc_info_dag_id ON "airflow-poc" USING btree ((info->>\'dag_id\'));',
            'CREATE INDEX IF NOT EXISTS idx_airflow_poc_info_task_id ON "airflow-poc" USING btree ((info->>\'task_id\'));'
        ]
        
        for index_query in indexes:
            cursor.execute(index_query)
        
        print("✅ Indexes created/verified")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("🎉 Database setup completed successfully!")
        
    except Exception as e:
        print(f"❌ Error setting up database: {e}")
        raise

def test_connection():
    """Test the database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Test basic connection
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"✅ Database connection successful!")
        print(f"   PostgreSQL version: {version}")
        
        # Test table existence
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'airflow-poc'
            );
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            cursor.execute('SELECT COUNT(*) FROM "airflow-poc"')
            count = cursor.fetchone()[0]
            print(f"✅ Table 'airflow-poc' exists with {count} records")
        else:
            print("⚠️  Table 'airflow-poc' does not exist")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_connection()
    else:
        setup_database()
        test_connection()
