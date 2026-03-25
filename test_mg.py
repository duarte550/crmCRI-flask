import os
from db import get_db_connection
from master_groups import fetch_full_master_group
import traceback

def test():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            mg = fetch_full_master_group(cursor, 2)
            print("Success:", mg)
    except Exception as e:
        print("Error encountered:")
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == '__main__':
    test()
