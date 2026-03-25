import sys
from db import get_db_connection

def run_script(filename):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            with open(filename, 'r', encoding='utf-8') as f:
                sql_script = f.read()

            statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]
            for stmt in statements:
                if stmt.startswith('--') and '\n' not in stmt:
                    continue
                try:
                    print(f"Executing: {stmt[:50]}...")
                    cursor.execute(stmt)
                except Exception as e:
                    print(f"Error executing statement: {e}")
        conn.commit()
    finally:
        conn.close()

if __name__ == '__main__':
    run_script(sys.argv[1])
