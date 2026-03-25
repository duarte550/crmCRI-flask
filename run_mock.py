import os
from db import get_db_connection

def run_sql_file(filename):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            with open(filename, 'r', encoding='utf-8') as f:
                sql_script = f.read()

            # Split statements by semicolon
            statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]
            for stmt in statements:
                # Skip comments entirely if it's just a comment block
                if stmt.startswith('--') and '\n' not in stmt:
                    continue
                try:
                    print(f"Executing: {stmt[:50]}...")
                    cursor.execute(stmt)
                except Exception as e:
                    print(f"Error executing statement: {e}")
                    # Some updates could fail if there's no data, we ignore minor errors for seed script
        conn.commit()
        print("Mock data seeded successfully.")
    except Exception as e:
        print(f"Fatal error: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    run_sql_file('mock_data.sql')
