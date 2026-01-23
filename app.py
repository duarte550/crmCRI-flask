
import os
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from db import get_db_connection
from task_engine import generate_tasks_for_operation
from datetime import datetime, date, timedelta
from collections import defaultdict
import json

# Configura o Flask para servir os arquivos estáticos da pasta raiz do projeto
app = Flask(__name__, static_folder=os.path.join(os.path.dirname(__file__), '..'), static_url_path='')

# Configuração de CORS para permitir requisições de qualquer origem.
CORS(app, supports_credentials=True)

# Regras de negócio centralizadas
RATING_TO_POLITICA_FREQUENCY = {
    'A4': 'Anual', 'Baa1': 'Anual', 'Baa3': 'Anual', 'Baa4': 'Anual', 'Ba1': 'Anual', 'Ba6': 'Anual',
    'B1': 'Semestral', 'B2': 'Semestral', 'B3': 'Semestral',
    'C1': 'Semestral', 'C2': 'Semestral', 'C3': 'Semestral',
}

# Usado para comparar a "velocidade" das frequências. Menor número = mais frequente.
FREQUENCY_VALUE_MAP = {
    'Diário': 1, 'Semanal': 7, 'Quinzenal': 15, 'Mensal': 30,
    'Trimestral': 90, 'Semestral': 180, 'Anual': 365
}


def format_row(row, cursor):
    """ Converte uma linha do banco de dados em um dicionário. """
    return {desc[0]: value for desc, value in zip(cursor.description, row)}

def log_action(cursor, user_name, action, entity_type, entity_id, details=""):
    """Grava uma ação no log de auditoria."""
    cursor.execute(
        """
        INSERT INTO cri.crm.audit_logs (timestamp, user_name, action, entity_type, entity_id, details)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (datetime.now(), user_name, action, entity_type, str(entity_id), details)
    )

def generate_diff_details(old_data, new_data, fields_to_compare):
    """ Gera uma string de detalhes comparando dados antigos e novos. """
    details = []
    for field, field_name in fields_to_compare.items():
        old_value = old_data.get(field)
        new_value = new_data.get(field)
        # Handle nested dicts like 'covenants'
        if '.' in field:
            key1, key2 = field.split('.')
            old_value = old_data.get(key1, {}).get(key2)
            new_value = new_data.get(key1, {}).get(key2)

        if old_value != new_value:
            details.append(f"Alterou '{field_name}' de '{old_value}' para '{new_value}'")
    return "; ".join(details)


def fetch_full_operation(cursor, operation_id):
    """ Busca uma operação completa com todos os seus dados relacionados e tarefas geradas. """
    cursor.execute("SELECT * FROM cri.crm.operations WHERE id = ?", (operation_id,))
    op_row = cursor.fetchone()
    if not op_row:
        return None
    
    operation_db = format_row(op_row, cursor)

    operation = {
        'id': operation_db['id'], 'name': operation_db['name'], 'area': operation_db['area'],
        'operationType': operation_db['operation_type'],
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

    cursor.execute("SELECT op.operation_id, p.id, p.name FROM cri.crm.projects p JOIN cri.crm.operation_projects op ON p.id = op.project_id WHERE op.operation_id = ?", (operation_id,))
    operation['projects'] = [{'id': r.id, 'name': r.name} for r in cursor.fetchall()]

    cursor.execute("SELECT og.operation_id, g.id, g.name FROM cri.crm.guarantees g JOIN cri.crm.operation_guarantees og ON g.id = og.guarantee_id WHERE og.operation_id = ?", (operation_id,))
    operation['guarantees'] = [{'id': r.id, 'name': r.name} for r in cursor.fetchall()]
    
    cursor.execute("SELECT * FROM cri.crm.events WHERE operation_id = ? ORDER BY date DESC", (operation_id,))
    events = []
    for r in cursor.fetchall():
        entry = format_row(r, cursor)
        events.append({
            'id': entry.get('id'),
            'date': entry.get('date').isoformat() if entry.get('date') else None,
            'type': entry.get('type'),
            'title': entry.get('title'),
            'description': entry.get('description'),
            'registeredBy': entry.get('registered_by'),
            'nextSteps': entry.get('next_steps'),
            'completedTaskId': entry.get('completed_task_id'),
        })
    operation['events'] = events

    cursor.execute("SELECT * FROM cri.crm.task_rules WHERE operation_id = ?", (operation_id,))
    task_rules = []
    for r in cursor.fetchall():
        entry = format_row(r, cursor)
        task_rules.append({
            'id': entry.get('id'),
            'name': entry.get('name'),
            'frequency': entry.get('frequency'),
            'startDate': entry.get('start_date').isoformat() if entry.get('start_date') else None,
            'endDate': entry.get('end_date').isoformat() if entry.get('end_date') else None,
            'description': entry.get('description'),
        })
    operation['taskRules'] = task_rules

    cursor.execute("SELECT * FROM cri.crm.rating_history WHERE operation_id = ? ORDER BY date DESC", (operation_id,))
    rating_history = []
    for r in cursor.fetchall():
        entry = format_row(r, cursor)
        rating_history.append({
            'id': entry.get('id'),
            'date': entry.get('date').isoformat() if entry.get('date') else None,
            'ratingOperation': entry.get('rating_operation'),
            'ratingGroup': entry.get('rating_group'),
            'watchlist': entry.get('watchlist'),
            'sentiment': entry.get('sentiment'),
            'eventId': entry.get('event_id'),
        })
    operation['ratingHistory'] = rating_history

    cursor.execute("SELECT task_id FROM cri.crm.task_exceptions WHERE operation_id = ?", (operation_id,))
    task_exceptions = {row.task_id for row in cursor.fetchall()}

    tasks = generate_tasks_for_operation(operation, task_exceptions)
    operation['tasks'] = tasks
    operation['overdueCount'] = sum(1 for task in tasks if task['status'] == 'Atrasada')

    # Calculate next review dates
    today = date.today()
    pending_tasks = [t for t in tasks if t['status'] != 'Concluída' and datetime.fromisoformat(t['dueDate']).date() >= today]
    
    next_gerencial_tasks = sorted([t['dueDate'] for t in pending_tasks if t['ruleName'] == 'Revisão Gerencial'])
    next_politica_tasks = sorted([t['dueDate'] for t in pending_tasks if t['ruleName'] == 'Revisão Política'])

    operation['nextReviewGerencial'] = next_gerencial_tasks[0] if next_gerencial_tasks else None
    operation['nextReviewPolitica'] = next_politica_tasks[0] if next_politica_tasks else None


    return operation

# ================== Rotas da API ==================
@app.route('/api/operations', methods=['GET', 'POST'])
def manage_operations_collection():
    conn = get_db_connection()
    if request.method == 'GET':
        try:
            with conn.cursor() as cursor:
                # This endpoint is complex. For simplicity in this change, we'll fetch one by one.
                # In a production environment, the bulk fetch logic below would be updated.
                cursor.execute("SELECT id FROM cri.crm.operations ORDER BY name")
                operation_ids = [row.id for row in cursor.fetchall()]
                all_operations = [fetch_full_operation(cursor, op_id) for op_id in operation_ids]
            return jsonify(all_operations)
        except Exception as e:
            app.logger.error(f"Exception on /api/operations [GET]: {e}", exc_info=True)
            return jsonify({"error": str(e)}), 500
        finally: 
            if conn:
                conn.close()
    
    elif request.method == 'POST':
        try:
            data = request.json
            with conn.cursor() as cursor:
                # --- Dynamic Frequency Logic on Creation ---
                politica_freq = RATING_TO_POLITICA_FREQUENCY.get(data['ratingGroup'], 'Anual')
                gerencial_freq = data['reviewFrequency']

                # Enforce that gerencial is at least as frequent as politica
                if FREQUENCY_VALUE_MAP.get(gerencial_freq, 999) > FREQUENCY_VALUE_MAP.get(politica_freq, 0):
                    gerencial_freq = politica_freq
                
                # Update the main operation record with the potentially adjusted gerencial frequency
                data['reviewFrequency'] = gerencial_freq
                # --- End of Logic ---

                # Insert main operation and get its ID
                dm = data.get('defaultMonitoring', {})
                cursor.execute(
                    "INSERT INTO cri.crm.operations (name, area, operation_type, maturity_date, responsible_analyst, review_frequency, call_frequency, df_frequency, segmento, rating_operation, rating_group, watchlist, ltv, dscr, monitoring_news, monitoring_fii_report, monitoring_operational_info, monitoring_receivables_portfolio, monitoring_construction_report, monitoring_commercial_info, monitoring_spe_dfs) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (data['name'], data['area'], data['operationType'], data['maturityDate'], data['responsibleAnalyst'], data['reviewFrequency'], data['callFrequency'], data['dfFrequency'], data['segmento'], data['ratingOperation'], data['ratingGroup'], data['watchlist'], data.get('covenants', {}).get('ltv'), data.get('covenants', {}).get('dscr'), dm.get('news'), dm.get('fiiReport'), dm.get('operationalInfo'), dm.get('receivablesPortfolio'), dm.get('monthlyConstructionReport'), dm.get('monthlyCommercialInfo'), dm.get('speDfs'))
                )
                cursor.execute("SELECT id FROM cri.crm.operations WHERE name = ? ORDER BY id DESC LIMIT 1", (data['name'],))
                new_op_id = cursor.fetchone().id

                # ... (code for projects and guarantees is the same)
                
                # Create default task rules and initial rating history
                today, end_date_iso = datetime.now().isoformat(), data['maturityDate']
                rules_to_add = [
                    {'name': 'Revisão Gerencial', 'frequency': gerencial_freq, 'desc': 'Revisão periódica gerencial.'},
                    {'name': 'Revisão Política', 'frequency': politica_freq, 'desc': 'Revisão de política de crédito anual.'},
                    {'name': 'Call de Acompanhamento', 'frequency': data['callFrequency'], 'desc': 'Call de acompanhamento.'},
                    {'name': 'Análise de DFs & Dívida', 'frequency': data['dfFrequency'], 'desc': 'Análise dos DFs.'}
                ]
                if dm.get('news'): rules_to_add.append({'name': 'Monitorar Notícias', 'frequency': 'Semanal', 'desc': 'Acompanhar notícias.'})
                for rule in rules_to_add:
                    cursor.execute("INSERT INTO cri.crm.task_rules (operation_id, name, frequency, start_date, end_date, description) VALUES (?, ?, ?, ?, ?, ?)", (new_op_id, rule['name'], rule['frequency'], today, end_date_iso, rule['desc']))
                
                cursor.execute("INSERT INTO cri.crm.rating_history (operation_id, date, rating_operation, rating_group, watchlist, sentiment) VALUES (?, ?, ?, ?, ?, ?)", (new_op_id, today, data['ratingOperation'], data['ratingGroup'], data['watchlist'], 'Neutro'))
                
                log_action(cursor, data.get('responsibleAnalyst', 'System'), 'CREATE', 'Operation', new_op_id, f"Operação '{data['name']}' criada na área '{data['area']}'.")
            conn.commit()
            
            with conn.cursor() as cursor:
                new_operation_full = fetch_full_operation(cursor, new_op_id)
            return jsonify(new_operation_full), 201
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error in POST /api/operations: {e}", exc_info=True)
            return jsonify({"error": str(e)}), 500
        finally: 
            if conn:
                conn.close()

@app.route('/api/operations/<int:op_id>', methods=['PUT', 'DELETE'])
def manage_operation(op_id):
    conn = get_db_connection()
    if request.method == 'PUT':
        try:
            data = request.json
            with conn.cursor() as cursor:
                # Fetch original data for diff logging
                cursor.execute("SELECT * FROM cri.crm.operations WHERE id = ?", (op_id,))
                old_op_db = format_row(cursor.fetchone(), cursor) if cursor.rowcount > 0 else {}
                old_rating_group = old_op_db.get('rating_group')
                new_rating_group = data['ratingGroup']
                
                # --- Dynamic Frequency Logic on Update ---
                if old_rating_group != new_rating_group:
                    new_politica_freq = RATING_TO_POLITICA_FREQUENCY.get(new_rating_group, 'Anual')
                    
                    # Find the date of the last policy review to reset the schedule
                    cursor.execute("SELECT MAX(date) FROM cri.crm.events WHERE operation_id = ? AND (title = 'Conclusão: Revisão Política' OR type = 'Revisão Periódica')", (op_id,))
                    last_review_date_row = cursor.fetchone()
                    if last_review_date_row and last_review_date_row[0]:
                        new_start_date = last_review_date_row[0]
                    else:
                        cursor.execute("SELECT start_date FROM cri.crm.task_rules WHERE operation_id = ? AND name = 'Revisão Política'", (op_id,))
                        new_start_date = cursor.fetchone()[0] # Fallback to original start date
                    
                    cursor.execute("UPDATE cri.crm.task_rules SET frequency = ?, start_date = ? WHERE operation_id = ? AND name = 'Revisão Política'", (new_politica_freq, new_start_date, op_id))
                    log_action(cursor, data.get('responsibleAnalyst', 'System'), 'UPDATE', 'TaskRule', op_id, f"Frequência da Revisão de Política ajustada para {new_politica_freq} devido à mudança de rating.")

                    # Now, check and adjust the Gerencial frequency if necessary
                    cursor.execute("SELECT frequency, start_date FROM cri.crm.task_rules WHERE operation_id = ? AND name = 'Revisão Gerencial'", (op_id,))
                    gerencial_rule = cursor.fetchone()
                    if gerencial_rule and FREQUENCY_VALUE_MAP.get(gerencial_rule[0], 999) > FREQUENCY_VALUE_MAP.get(new_politica_freq, 0):
                        cursor.execute("SELECT MAX(date) FROM cri.crm.events WHERE operation_id = ? AND (title = 'Conclusão: Revisão Gerencial' OR type = 'Revisão Periódica')", (op_id,))
                        last_gerencial_date_row = cursor.fetchone()
                        new_gerencial_start = last_gerencial_date_row[0] if last_gerencial_date_row and last_gerencial_date_row[0] else gerencial_rule[1]

                        cursor.execute("UPDATE cri.crm.task_rules SET frequency = ?, start_date = ? WHERE operation_id = ? AND name = 'Revisão Gerencial'", (new_politica_freq, new_gerencial_start, op_id))
                        log_action(cursor, data.get('responsibleAnalyst', 'System'), 'UPDATE', 'TaskRule', op_id, f"Frequência da Revisão Gerencial ajustada para {new_politica_freq} para alinhar com a política.")
                # --- End of Logic ---

                # Update main operation table
                cov = data.get('covenants', {})
                cursor.execute(
                    """
                    UPDATE cri.crm.operations 
                    SET name = ?, area = ?, rating_operation = ?, rating_group = ?, watchlist = ?, ltv = ?, dscr = ? 
                    WHERE id = ?
                    """, 
                    (data['name'], data['area'], data['ratingOperation'], data['ratingGroup'], data['watchlist'], cov.get('ltv'), cov.get('dscr'), op_id)
                )
                
                # Sync Events
                db_event_ids = {row.id for row in cursor.execute("SELECT id FROM cri.crm.events WHERE operation_id = ?", (op_id,)).fetchall()}
                client_event_ids = {e['id'] for e in data.get('events', []) if isinstance(e.get('id'), int)}
                for event in data.get('events', []):
                    if not isinstance(event.get('id'), int): # New event
                        cursor.execute("INSERT INTO cri.crm.events (operation_id, date, type, title, description, registered_by, next_steps, completed_task_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                       (op_id, event['date'], event['type'], event['title'], event['description'], event['registeredBy'], event['nextSteps'], event.get('completedTaskId')))
                        log_action(cursor, event['registeredBy'], 'CREATE', 'Event', 'new', f"Evento '{event['title']}' adicionado à operação '{data['name']}'.")

                # Sync Rating History
                db_rh_ids = {row.id for row in cursor.execute("SELECT id FROM cri.crm.rating_history WHERE operation_id = ?", (op_id,)).fetchall()}
                client_rh_ids = {rh['id'] for rh in data.get('ratingHistory', []) if isinstance(rh.get('id'), int)}
                for rh in data.get('ratingHistory', []):
                    if not isinstance(rh.get('id'), int): # New history entry
                        cursor.execute("INSERT INTO cri.crm.rating_history (operation_id, date, rating_operation, rating_group, watchlist, sentiment, event_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                       (op_id, rh['date'], rh['ratingOperation'], rh['ratingGroup'], rh['watchlist'], rh['sentiment'], rh['eventId']))

                # Sync Task Rules
                cursor.execute("SELECT id, name FROM cri.crm.task_rules WHERE operation_id = ?", (op_id,))
                db_rules_map = {row.id: row.name for row in cursor.fetchall()}
                client_rule_ids = {r['id'] for r in data.get('taskRules', []) if 'id' in r and isinstance(r['id'], int)}

                for rule_id_to_delete in set(db_rules_map.keys()) - client_rule_ids:
                    cursor.execute("DELETE FROM cri.crm.task_rules WHERE id = ?", (rule_id_to_delete,))
                    log_action(cursor, data.get('responsibleAnalyst', 'System'), 'DELETE', 'TaskRule', rule_id_to_delete, f"Regra '{db_rules_map[rule_id_to_delete]}' deletada da operação '{data['name']}'.")

                for rule in data.get('taskRules', []):
                    rule_id = rule.get('id')
                    if rule_id and rule_id in db_rules_map:
                        # Updates to frequency/start_date for review rules are handled by the dynamic logic above
                        if rule['name'] not in ['Revisão Política', 'Revisão Gerencial']:
                            cursor.execute("UPDATE cri.crm.task_rules SET name=?, frequency=?, start_date=?, end_date=?, description=? WHERE id=?", 
                                           (rule['name'], rule['frequency'], rule['startDate'], rule['endDate'], rule['description'], rule_id))
                    else:
                        cursor.execute("INSERT INTO cri.crm.task_rules (operation_id, name, frequency, start_date, end_date, description) VALUES (?, ?, ?, ?, ?, ?)", 
                                       (op_id, rule['name'], rule['frequency'], rule['startDate'], rule['endDate'], rule['description']))
                        log_action(cursor, data.get('responsibleAnalyst', 'System'), 'CREATE', 'TaskRule', 'new', f"Regra '{rule['name']}' adicionada à operação '{data['name']}'.")
                
                details = generate_diff_details(old_op_db, data, {'name': 'Nome', 'ratingOperation': 'Rating Op.', 'ratingGroup': 'Rating Grupo', 'watchlist': 'Watchlist'})
                if details: 
                    log_action(cursor, data.get('responsibleAnalyst', 'System'), 'UPDATE', 'Operation', op_id, details)
            
            conn.commit()
            
            with conn.cursor() as cursor:
                updated_operation_full = fetch_full_operation(cursor, op_id)
            return jsonify(updated_operation_full)
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error in PUT /api/operations/{op_id}: {e}", exc_info=True)
            return jsonify({"error": str(e)}), 500
        finally: 
            if conn:
                conn.close()

    elif request.method == 'DELETE':
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT name, responsible_analyst FROM cri.crm.operations WHERE id = ?", (op_id,))
                op_info = cursor.fetchone()
                # Cascade delete and log
                cursor.execute("DELETE FROM cri.crm.operation_projects WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri.crm.operation_guarantees WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri.crm.rating_history WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri.crm.events WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri.crm.task_rules WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri.crm.task_exceptions WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri.crm.operations WHERE id = ?", (op_id,))
                log_action(cursor, op_info.responsible_analyst if op_info else 'System', 'DELETE', 'Operation', op_id, f"Operação '{op_info.name if op_info else 'ID: ' + str(op_id)}' e todos os seus dados foram deletados.")
            conn.commit()
            return '', 204
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error deleting operation {op_id}: {e}", exc_info=True)
            return jsonify({"error": str(e)}), 500
        finally:
            if conn:
                conn.close()

@app.route('/api/tasks/delete', methods=['POST'])
def delete_task():
    conn = get_db_connection()
    try:
        data = request.json
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO cri.crm.task_exceptions (task_id, operation_id, deleted_at, deleted_by) VALUES (?, ?, ?, ?)", (data['taskId'], data['operationId'], datetime.now(), data.get('responsibleAnalyst')))
            log_action(cursor, data.get('responsibleAnalyst'), 'DELETE', 'Task', data['taskId'], f"Tarefa deletada para a operação ID {data['operationId']}.")
        conn.commit()
        with conn.cursor() as cursor:
            updated_op = fetch_full_operation(cursor, data['operationId'])
        return jsonify(updated_op)
    except Exception as e:
        conn.rollback()
        app.logger.error(f"Error deleting task: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()
        
@app.route('/api/tasks/edit', methods=['PUT'])
def edit_task():
    conn = get_db_connection()
    try:
        data = request.json
        updates = data['updates']
        with conn.cursor() as cursor:
            # 1. Add exception for the old task
            cursor.execute("INSERT INTO cri.crm.task_exceptions (task_id, operation_id, deleted_at, deleted_by) VALUES (?, ?, ?, ?)", (data['originalTaskId'], data['operationId'], datetime.now(), data.get('responsibleAnalyst')))
            # 2. Create a new ad-hoc rule for the edited task
            due_date = updates['dueDate']
            cursor.execute("INSERT INTO cri.crm.task_rules (operation_id, name, frequency, start_date, end_date, description) VALUES (?, ?, 'Pontual', ?, ?, ?)", (data['operationId'], updates['name'], due_date, due_date, f"Tarefa editada a partir de {data['originalTaskId']}"))
            log_action(cursor, data.get('responsibleAnalyst'), 'UPDATE', 'Task', data['originalTaskId'], f"Tarefa editada para ter nome '{updates['name']}' e vencimento em {due_date}.")
        conn.commit()
        with conn.cursor() as cursor:
            updated_op = fetch_full_operation(cursor, data['operationId'])
        return jsonify(updated_op)
    except Exception as e:
        conn.rollback()
        app.logger.error(f"Error editing task: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/audit_logs', methods=['GET'])
def get_audit_logs():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM cri.crm.audit_logs ORDER BY timestamp DESC")
            logs = [format_row(row, cursor) for row in cursor.fetchall()]
            for log in logs:
                if log.get('timestamp'):
                    log['timestamp'] = log['timestamp'].isoformat()
            return jsonify(logs)
    except Exception as e:
        app.logger.error(f"Error fetching audit logs: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()

# ================== Servidor de Frontend ==================
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve_react_app(path):
    if path != "" and os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    else:
        return send_from_directory(app.static_folder, 'index.html')
