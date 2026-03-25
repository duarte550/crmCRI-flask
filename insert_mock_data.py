import os
import sys
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db import get_db_connection

def run_mock_data():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            print("Connected to Databricks SQL Warehouse.")
            
            # --- MASTER GROUP ---
            print("Inserting Master Group...")
            cursor.execute("INSERT INTO cri_cra_dev.crm.master_groups (name) VALUES (?)",
                           ("Grupo Alpha Mock",))
            cursor.execute("SELECT MAX(id) as id FROM cri_cra_dev.crm.master_groups")
            row = cursor.fetchone()
            mg_id = row.id if hasattr(row, 'id') else row[0]

            # --- STRUCTURING OPERATION ---
            print(f"Inserting Structuring Operation for MG {mg_id}...")
            cursor.execute(
                "INSERT INTO cri_cra_dev.crm.structuring_operations (master_group_id, name, stage, liquidation_date, risk, temperature) VALUES (?, ?, ?, ?, ?, ?)",
                (mg_id, "Emissão Alpha R$ 50M", "Aprovação", (date.today() + timedelta(days=15)).isoformat(), "High Yield", "Quente")
            )
            cursor.execute("SELECT MAX(id) as id FROM cri_cra_dev.crm.structuring_operations")
            row = cursor.fetchone()
            so_id = row.id if hasattr(row, 'id') else row[0]

            default_stages = ['Conversa Inicial', 'Term Sheet', 'Due Diligence', 'Aprovação', 'Liquidação']
            for idx, sn in enumerate(default_stages):
                is_comp = True if idx <= 3 else False
                cursor.execute(
                    "INSERT INTO cri_cra_dev.crm.structuring_operation_stages (structuring_operation_id, name, order_index, is_completed) VALUES (?, ?, ?, ?)", 
                    (so_id, sn, idx, is_comp)
                )

            cursor.execute(
                "INSERT INTO cri_cra_dev.crm.structuring_operation_series (structuring_operation_id, name, rate, indexer, volume, fund) VALUES (?, ?, ?, ?, ?, ?)",
                (so_id, "Série Única", "+ 3.5%", "IPCA", 50000000.0, "FII HGLG11")
            )

            # --- ACTIVE OPERATION ---
            print("Inserting Active Operation...")
            cursor.execute("""
                INSERT INTO cri_cra_dev.crm.operations 
                (name, area, operation_type, maturity_date, responsible_analyst, review_frequency, call_frequency, df_frequency, segmento, rating_operation, rating_group, watchlist, status, description, master_group_id) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, ("Alpha Towers CRI (Mock)", "Imobiliário", "CRI", (date.today() + timedelta(days=365)).isoformat(), "João Silva", "Semestral", "Mensal", "Trimestral", "Residencial", "Ba2", "Ba2", "Amarelo", "Ativa", "Operação de teste", mg_id))
            
            cursor.execute("SELECT MAX(id) as id FROM cri_cra_dev.crm.operations")
            row = cursor.fetchone()
            op_id = row.id if hasattr(row, 'id') else row[0]

            # --- PROJECTS & GUARANTEES ---
            print(f"Inserting Projects & Guarantees for Op {op_id}...")
            cursor.execute("INSERT INTO cri_cra_dev.crm.projects (name, status, state, city) VALUES (?, ?, ?, ?)", ("Residencial Alpha", "Em Construção", "SP", "São Paulo"))
            cursor.execute("SELECT MAX(id) as id FROM cri_cra_dev.crm.projects")
            p_row = cursor.fetchone()
            proj_id = p_row.id if hasattr(p_row, 'id') else p_row[0]
            cursor.execute("INSERT INTO cri_cra_dev.crm.operation_projects (operation_id, project_id) VALUES (?, ?)", (op_id, proj_id))

            cursor.execute("INSERT INTO cri_cra_dev.crm.guarantees (name, type, value, description) VALUES (?, ?, ?, ?)", ("Alienação Fiduciária Terreno", "Imóvel", 80000000.0, "Terreno em SP avaliado em 80M"))
            cursor.execute("SELECT MAX(id) as id FROM cri_cra_dev.crm.guarantees")
            g_row = cursor.fetchone()
            guar_id = g_row.id if hasattr(g_row, 'id') else g_row[0]
            cursor.execute("INSERT INTO cri_cra_dev.crm.operation_guarantees (operation_id, guarantee_id) VALUES (?, ?)", (op_id, guar_id))

            # --- TASK RULES ---
            print("Inserting Task Rules...")
            cursor.execute("""
                INSERT INTO cri_cra_dev.crm.task_rules 
                (operation_id, name, frequency, start_date, end_date, description, priority)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (op_id, "Monitoramento Mensal (Mock)", "Mensal", (date.today() - timedelta(days=5)).isoformat(), (date.today() + timedelta(days=365)).isoformat(), "Tarefa gerada via Mock.", "Alta"))

            # --- EVENTS ---
            print("Inserting Events...")
            cursor.execute("""
                INSERT INTO cri_cra_dev.crm.events 
                (operation_id, date, type, title, description, registered_by, is_origination)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (op_id, (datetime.now() - timedelta(days=2)).isoformat(), "Reunião", "Apresentação de Resultados", "Reunião com a Diretoria Financeira.", "João Silva", False))

            print("Mock data successfully generated!")
            print("Done.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    load_dotenv()
    run_mock_data()
