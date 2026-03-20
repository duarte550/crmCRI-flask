import os
from db import get_db_connection

def update_schema():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            print("Checking if priority column exists in task_rules...")
            cursor.execute("DESCRIBE cri_cra_dev.crm.task_rules")
            columns = [row.col_name for row in cursor.fetchall()]
            
            if 'priority' not in columns:
                print("Adding priority column...")
                cursor.execute("ALTER TABLE cri_cra_dev.crm.task_rules ADD COLUMN priority STRING COMMENT 'Prioridade da tarefa (Alta, Média, Baixa).'")
                print("Column added successfully.")
            else:
                print("Column priority already exists.")

            print("Ensuring task_rules dates are nullable...")
            # In Databricks SQL, we use ALTER TABLE ALTER COLUMN
            cursor.execute("ALTER TABLE cri_cra_dev.crm.task_rules ALTER COLUMN start_date DROP NOT NULL")
            cursor.execute("ALTER TABLE cri_cra_dev.crm.task_rules ALTER COLUMN end_date DROP NOT NULL")
            print("Dates are now nullable.")

            print("Ensuring operation_review_notes table exists...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cri_cra_dev.crm.operation_review_notes (
                    operation_id BIGINT PRIMARY KEY,
                    notes STRING,
                    updated_at TIMESTAMP NOT NULL,
                    updated_by STRING NOT NULL,
                    FOREIGN KEY (operation_id) REFERENCES cri_cra_dev.crm.operations(id)
                ) COMMENT 'Armazena notas e observações editáveis para cada operação.'
            """)
            print("Table operation_review_notes checked/created.")
    except Exception as e:
        print(f"Error updating schema: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    update_schema()
