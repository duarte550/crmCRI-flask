from db import get_db_connection

def test():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            try:
                cursor.execute("SELECT * FROM cri_cra_dev.crm.task_exceptions LIMIT 1")
                print("task_exceptions EXISTS")
            except Exception as e:
                print("task_exceptions ERROR:", e)

            try:
                cursor.execute("SELECT * FROM cri_cra_dev.crm.structuring_operation_stages LIMIT 1")
                print("structuring_operation_stages EXISTS")
            except Exception as e:
                print("structuring_operation_stages ERROR:", e)
    finally:
        conn.close()

if __name__ == "__main__":
    test()
