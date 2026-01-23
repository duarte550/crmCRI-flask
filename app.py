
import re
import os
from flask import Flask, jsonify, request
from flask_cors import CORS
from db import get_db_connection
from task_engine import generate_tasks_for_operation
from datetime import datetime, date, timedelta

app = Flask(__name__)

# Production-ready CORS configuration
# In production, you would set the ALLOWED_ORIGINS environment variable to your frontend's domain,
# e.g., "https://www.my-crm-app.com"
# In development (like Codespaces), it falls back to the flexible regex.
allowed_origins_env = os.getenv("ALLOWED_ORIGINS")
if allowed_origins_env:
    origins = allowed_origins_env.split(',')
else:
    origins = re.compile(r"https?://.*\.app\.github\.dev")

CORS(app, origins=origins, supports_credentials=True)


def format_row(row, cursor):
    """ Converts a database row into a dictionary with column names as keys. """
    return {desc[0]: value for desc, value in zip(cursor.description, row)}

def fetch_full_operation(cursor, operation_id):
    """ Fetches a single operation and all its related data, then enriches it with dynamically generated tasks. """
    cursor.execute("SELECT * FROM cri.crm.operations WHERE id = ?", (operation_id,))
    op_row = cursor.fetchone()
    if not op_row:
        return None
    
    operation_db = format_row(op_row, cursor)

    # Convert snake_case from DB to camelCase for frontend
    operation = {
        'id': operation_db['id'], 'name': operation_db['name'], 'operationType': operation_db['operation_type'],
        'maturityDate': operation_db['maturity_date'].isoformat() if operation_db.get('maturity_date') else None,
        'responsibleAnalyst': operation_db['responsible_analyst'], 'reviewFrequency': operation_db['review_frequency'],
        'callFrequency': operation_db['call_frequency'], 'dfFrequency': operation_db['df_frequency'],
        'segmento': operation_db['segmento'], 'ratingOperation': operation_db['rating_operation'],
        'ratingGroup': operation_db['rating_group'], 'watchlist': operation_db['watchlist'],
        'covenants': {'ltv': operation_db['ltv'], 'dscr': operation_db['dscr']},
        'defaultMonitoring': {
            'news': operation_db['monitoring_news'], 'fiiReport': operation_db['monitoring_fii_report'],
            'operationalInfo': operation_db['monitoring_operational_info'],
            'receivablesPortfolio': operation_db['monitoring_receivables_portfolio'],
            'monthlyConstructionReport': operation_db['monitoring_construction_report'],
            'monthlyCommercialInfo': operation_db['monitoring_commercial_info'],
            'speDfs': operation_db['monitoring_spe_dfs']
        }
    }

    # Fetch related data
    cursor.execute("SELECT p.id, p.name FROM cri.crm.projects p JOIN cri.crm.operation_projects op ON p.id = op.project_id WHERE op.operation_id = ?", (operation_id,))
    operation['projects'] = [format_row(row, cursor) for row in cursor.fetchall()]

    cursor.execute("SELECT g.id, g.name FROM cri.crm.guarantees g JOIN cri.crm.operation_guarantees og ON g.id = og.guarantee_id WHERE og.operation_id = ?", (operation_id,))
    operation['guarantees'] = [format_row(row, cursor) for row in cursor.fetchall()]
    
    cursor.execute("SELECT * FROM cri.crm.events WHERE operation_id = ? ORDER BY date DESC", (operation_id,))
    operation['events'] = [{**format_row(r, cursor), 'date': r.date.isoformat() if r.date else None} for r in cursor.fetchall()]
    
    cursor.execute("SELECT * FROM cri.crm.task_rules WHERE operation_id = ?", (operation_id,))
    operation['taskRules'] = [{**format_row(r, cursor), 'startDate': r.start_date.isoformat() if r.start_date else None, 'endDate': r.end_date.isoformat() if r.end_date else None} for r in cursor.fetchall()]
    
    cursor.execute("SELECT * FROM cri.crm.rating_history WHERE operation_id = ? ORDER BY date DESC", (operation_id,))
    operation['ratingHistory'] = [{**format_row(r, cursor), 'date': r.date.isoformat() if r.date else None} for r in cursor.fetchall()]

    # SERVER-SIDE BUSINESS LOGIC: Generate tasks and overdue count
    tasks = generate_tasks_for_operation(operation)
    operation['tasks'] = tasks
    operation['overdueCount'] = sum(1 for task in tasks if task['status'] == 'Atrasada')

    return operation

@app.route('/api/operations', methods=['GET', 'POST'])
def manage_operations_collection():
    """
    Handles fetching all operations (GET) and adding a new one (POST).
    """
    conn = get_db_connection()

    if request.method == 'GET':
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM cri.crm.operations ORDER BY name")
                operation_ids = [row.id for row in cursor.fetchall()]
                all_operations = [fetch_full_operation(cursor, op_id) for op_id in operation_ids]
            return jsonify(all_operations)
        finally:
            conn.close()

    elif request.method == 'POST':
        try:
            data = request.json
            with conn.cursor() as cursor:
                dm = data.get('defaultMonitoring', {})
                cursor.execute(
                    """
                    INSERT INTO cri.crm.operations (name, operation_type, maturity_date, responsible_analyst, review_frequency, call_frequency, df_frequency, segmento, rating_operation, rating_group, watchlist, ltv, dscr, monitoring_news, monitoring_fii_report, monitoring_operational_info, monitoring_receivables_portfolio, monitoring_construction_report, monitoring_commercial_info, monitoring_spe_dfs)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        data['name'], data['operationType'], data['maturityDate'], data['responsibleAnalyst'], data['reviewFrequency'],
                        data['callFrequency'], data['dfFrequency'], data['segmento'], data['ratingOperation'], data['ratingGroup'],
                        data['watchlist'], data.get('covenants', {}).get('ltv'), data.get('covenants', {}).get('dscr'), dm.get('news'), dm.get('fiiReport'),
                        dm.get('operationalInfo'), dm.get('receivablesPortfolio'), dm.get('monthlyConstructionReport'),
                        dm.get('monthlyCommercialInfo'), dm.get('speDfs')
                    )
                )
                cursor.execute("SELECT id FROM cri.crm.operations WHERE name = ? ORDER BY id DESC LIMIT 1", (data['name'],))
                new_op_id = cursor.fetchone().id

                # Handle Projects with application-level uniqueness ("get-or-create")
                for proj in data.get('projects', []):
                    cursor.execute("SELECT id FROM cri.crm.projects WHERE name = ?", (proj['name'],))
                    existing_proj = cursor.fetchone()
                    if existing_proj:
                        proj_id = existing_proj.id
                    else:
                        cursor.execute("INSERT INTO cri.crm.projects (name) VALUES (?)", (proj['name'],))
                        cursor.execute("SELECT id FROM cri.crm.projects WHERE name = ?", (proj['name'],))
                        proj_id = cursor.fetchone().id
                    cursor.execute("INSERT INTO cri.crm.operation_projects (operation_id, project_id) VALUES (?, ?)", (new_op_id, proj_id))

                # Handle Guarantees with application-level uniqueness ("get-or-create")
                for guar in data.get('guarantees', []):
                    cursor.execute("SELECT id FROM cri.crm.guarantees WHERE name = ?", (guar['name'],))
                    existing_guar = cursor.fetchone()
                    if existing_guar:
                        guar_id = existing_guar.id
                    else:
                        cursor.execute("INSERT INTO cri.crm.guarantees (name) VALUES (?)", (guar['name'],))
                        cursor.execute("SELECT id FROM cri.crm.guarantees WHERE name = ?", (guar['name'],))
                        guar_id = cursor.fetchone().id
                    cursor.execute("INSERT INTO cri.crm.operation_guarantees (operation_id, guarantee_id) VALUES (?, ?)", (new_op_id, guar_id))


                # SERVER-SIDE BUSINESS LOGIC: Auto-generate task rules
                today = datetime.now()
                start_date_iso = today.isoformat()
                end_date_iso = data['maturityDate']
                rules_to_add = [
                    {'name': 'Revisão Periódica', 'frequency': data['reviewFrequency'], 'desc': 'Revisão periódica da operação, conforme frequência definida.'},
                    {'name': 'Call de Acompanhamento', 'frequency': data['callFrequency'], 'desc': 'Call de acompanhamento com o cliente/gestor.'},
                    {'name': 'Análise de DFs & Dívida', 'frequency': data['dfFrequency'], 'desc': 'Análise dos demonstrativos financeiros e endividamento.'},
                ]
                if dm.get('news'):
                    day_of_week = today.weekday()
                    days_until_friday = (4 - day_of_week + 7) % 7
                    next_friday = today + timedelta(days=days_until_friday if days_until_friday > 0 else 7)
                    rules_to_add.append({'name': 'Monitorar Notícias', 'frequency': 'Semanal', 'start': next_friday.isoformat(), 'desc': 'Acompanhar notícias relacionadas à operação e ao mercado.'})
                if dm.get('fiiReport'): rules_to_add.append({'name': 'Verificar Relatório FII', 'frequency': 'Mensal', 'desc': 'Analisar o relatório mensal do FII, se aplicável.'})
                if dm.get('operationalInfo'): rules_to_add.append({'name': 'Coletar Info Operacional', 'frequency': 'Mensal', 'desc': 'Coletar e analisar informações operacionais da SPE/Projeto.'})
                if dm.get('receivablesPortfolio'): rules_to_add.append({'name': 'Analisar Carteira de Recebíveis', 'frequency': 'Mensal', 'desc': 'Análise da performance da carteira de recebíveis (FIDC/CRI).'})
                if dm.get('monthlyConstructionReport'): rules_to_add.append({'name': 'Analisar Relatório Mensal de Obra', 'frequency': 'Mensal', 'desc': 'Verificar o andamento físico e financeiro da obra.'})
                if dm.get('monthlyCommercialInfo'): rules_to_add.append({'name': 'Analisar Info Comercial Mensal', 'frequency': 'Mensal', 'desc': 'Analisar performance de vendas/locação do projeto.'})
                if dm.get('speDfs'): rules_to_add.append({'name': 'Analisar DFs da SPE', 'frequency': 'Mensal', 'desc': 'Analisar os demonstrativos financeiros da Sociedade de Propósito Específico.'})

                for rule in rules_to_add:
                    cursor.execute(
                        "INSERT INTO cri.crm.task_rules (operation_id, name, frequency, start_date, end_date, description) VALUES (?, ?, ?, ?, ?, ?)",
                        (new_op_id, rule['name'], rule['frequency'], rule.get('start', start_date_iso), end_date_iso, rule['desc'])
                    )
                
                # SERVER-SIDE BUSINESS LOGIC: Create initial rating history
                cursor.execute(
                    "INSERT INTO cri.crm.rating_history (operation_id, date, rating_operation, rating_group, sentiment) VALUES (?, ?, ?, ?, ?)",
                    (new_op_id, start_date_iso, data['ratingOperation'], data['ratingGroup'], 'Neutro')
                )
                
                new_operation_full = fetch_full_operation(cursor, new_op_id)
            
            conn.commit()
            return jsonify(new_operation_full), 201
        finally:
            conn.close()

@app.route('/api/operations/<int:op_id>', methods=['PUT', 'DELETE'])
def manage_operation(op_id):
    """
    Handles updating (PUT) and deleting (DELETE) a specific operation.
    """
    conn = get_db_connection()

    if request.method == 'PUT':
        try:
            data = request.json
            with conn.cursor() as cursor:
                cov = data.get('covenants', {})
                cursor.execute(
                    """
                    UPDATE cri.crm.operations SET
                    name = ?, rating_operation = ?, rating_group = ?, watchlist = ?, ltv = ?, dscr = ?
                    WHERE id = ?
                    """,
                    (data['name'], data['ratingOperation'], data['ratingGroup'], data['watchlist'], cov.get('ltv'), cov.get('dscr'), op_id)
                )
                
                # Add new events
                cursor.execute("SELECT id FROM cri.crm.events WHERE operation_id = ?", (op_id,))
                db_event_ids = {row.id for row in cursor.fetchall()}
                newly_inserted_event_ids_map = {} # Maps client ID to new DB ID

                for event in data.get('events', []):
                    client_id = event.get('id')
                    if client_id not in db_event_ids:
                        cursor.execute(
                            "INSERT INTO cri.crm.events (operation_id, date, type, title, description, registered_by, next_steps, completed_task_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (op_id, event['date'], event['type'], event['title'], event['description'], event['registeredBy'], event['nextSteps'], event.get('completedTaskId'))
                        )
                        # Fetch the new ID to handle FK relationships for rating_history
                        cursor.execute("SELECT id FROM cri.crm.events WHERE operation_id = ? ORDER BY id DESC LIMIT 1", (op_id,))
                        new_db_id = cursor.fetchone().id
                        newly_inserted_event_ids_map[client_id] = new_db_id
                
                # SERVER-SIDE BUSINESS LOGIC: Add new rating history entries
                cursor.execute("SELECT id FROM cri.crm.rating_history WHERE operation_id = ?", (op_id,))
                db_rh_ids = {row.id for row in cursor.fetchall()}
                
                for rh_entry in data.get('ratingHistory', []):
                    if rh_entry.get('id') not in db_rh_ids:
                        # Resolve the event ID: if it was a client-side ID for a new event, use the new DB ID
                        client_event_id = rh_entry.get('eventId')
                        db_event_id = newly_inserted_event_ids_map.get(client_event_id, client_event_id)
                        
                        cursor.execute(
                            "INSERT INTO cri.crm.rating_history (operation_id, event_id, date, rating_operation, rating_group, sentiment) VALUES (?, ?, ?, ?, ?, ?)",
                            (op_id, db_event_id, rh_entry['date'], rh_entry['ratingOperation'], rh_entry['ratingGroup'], rh_entry['sentiment'])
                        )

                # Add new task rules
                cursor.execute("SELECT id FROM cri.crm.task_rules WHERE operation_id = ?", (op_id,))
                db_rule_ids = {row.id for row in cursor.fetchall()}
                for rule in data.get('taskRules', []):
                    if rule.get('id') not in db_rule_ids:
                         cursor.execute(
                            "INSERT INTO cri.crm.task_rules (operation_id, name, frequency, start_date, end_date, description) VALUES (?, ?, ?, ?, ?, ?)",
                            (op_id, rule['name'], rule['frequency'], rule['startDate'], rule['endDate'], rule['description'])
                        )

                updated_operation_full = fetch_full_operation(cursor, op_id)

            conn.commit()
            return jsonify(updated_operation_full)
        finally:
            conn.close()

    elif request.method == 'DELETE':
        try:
            with conn.cursor() as cursor:
                # The order is important to respect logical dependencies.
                cursor.execute("DELETE FROM cri.crm.operation_projects WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri.crm.operation_guarantees WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri.crm.rating_history WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri.crm.events WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri.crm.task_rules WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri.crm.operations WHERE id = ?", (op_id,))
            
            conn.commit()
            return '', 204
        finally:
            conn.close()

if __name__ == '__main__':
    app.run(debug=True)
