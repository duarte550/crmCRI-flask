import os
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from db import get_db_connection
from task_engine import generate_tasks_for_operation
from datetime import datetime, date, timedelta
from collections import defaultdict
import json
import logging

# Configurações básicas de logging
app = Flask(__name__, static_folder=os.path.join(os.path.dirname(__file__), '..'), static_url_path='')
logging.basicConfig(level=logging.INFO)

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
        # Handle snake_case for old_data from DB and camelCase for new_data from client
        old_field_key = field.replace('ratingGroup', 'rating_group').replace('ratingOperation', 'rating_operation')
        old_value = old_data.get(old_field_key)
        new_value = new_data.get(field)
        
        if old_value != new_value:
            details.append(f"Alterou '{field_name}' de '{old_value}' para '{new_value}'")
    return "; ".join(details)


def fetch_full_operation(cursor, operation_id):
    """ 
    Busca uma operação completa com todos os seus dados.
    Esta versão utiliza múltiplas queries simples e direcionadas para evitar timeouts
    em queries complexas de JOIN no Databricks.
    """
    cursor.execute("SELECT * FROM cri.crm.operations WHERE id = ?", (operation_id,))
    op_row = cursor.fetchone()
    if not op_row:
        return None
    
    # Monta o dicionário base da operação
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

    # Busca dados relacionados em queries separadas
    cursor.execute("SELECT p.id, p.name FROM cri.crm.projects p JOIN cri.crm.operation_projects op ON p.id = op.project_id WHERE op.operation_id = ?", (operation_id,))
    operation['projects'] = [{'id': r.id, 'name': r.name} for r in cursor.fetchall()]

    cursor.execute("SELECT g.id, g.name FROM cri.crm.guarantees g JOIN cri.crm.operation_guarantees og ON g.id = og.guarantee_id WHERE og.operation_id = ?", (operation_id,))
    operation['guarantees'] = [{'id': r.id, 'name': r.name} for r in cursor.fetchall()]
    
    cursor.execute("SELECT * FROM cri.crm.events WHERE operation_id = ? ORDER BY date DESC", (operation_id,))
    operation['events'] = [format_row(r, cursor) for r in cursor.fetchall()]
    for event in operation['events']:
        if event.get('date'): event['date'] = event['date'].isoformat()

    cursor.execute("SELECT * FROM cri.crm.task_rules WHERE operation_id = ?", (operation_id,))
    operation['taskRules'] = [format_row(r, cursor) for r in cursor.fetchall()]
    for rule in operation['taskRules']:
        if rule.get('startDate'): rule['startDate'] = rule['startDate'].isoformat()
        if rule.get('endDate'): rule['endDate'] = rule['endDate'].isoformat()

    cursor.execute("SELECT * FROM cri.crm.rating_history WHERE operation_id = ? ORDER BY date DESC", (operation_id,))
    operation['ratingHistory'] = [format_row(r, cursor) for r in cursor.fetchall()]
    for rh in operation['ratingHistory']:
        if rh.get('date'): rh['date'] = rh['date'].isoformat()

    cursor.execute("SELECT task_id FROM cri.crm.task_exceptions WHERE operation_id = ?", (operation_id,))
    task_exceptions = {row.task_id for row in cursor.fetchall()}

    # Gerar tarefas com base nos dados montados
    tasks = generate_tasks_for_operation(operation, task_exceptions)
    operation['tasks'] = tasks
    operation['overdueCount'] = sum(1 for task in tasks if task['status'] == 'Atrasada')

    # Calcular próximas revisões
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
                # --- Otimização "1+N": Busca todas as operações e depois os dados relacionados em lotes ---
                
                # 1. Busca todas as operações base
                cursor.execute("SELECT * FROM cri.crm.operations ORDER BY name")
                db_operations = [format_row(row, cursor) for row in cursor.fetchall()]
                
                if not db_operations:
                    return jsonify([])

                # FIX: Mapeia snake_case do DB para camelCase do Frontend
                operations_map = {}
                for op_db in db_operations:
                    op_id = op_db['id']
                    operations_map[op_id] = {
                        'id': op_id, 'name': op_db['name'], 'area': op_db['area'],
                        'operationType': op_db['operation_type'],
                        'maturityDate': op_db['maturity_date'].isoformat() if op_db.get('maturity_date') else None,
                        'responsibleAnalyst': op_db['responsible_analyst'],
                        'reviewFrequency': op_db['review_frequency'],
                        'callFrequency': op_db['call_frequency'],
                        'dfFrequency': op_db['df_frequency'],
                        'segmento': op_db['segmento'],
                        'ratingOperation': op_db['rating_operation'],
                        'ratingGroup': op_db['rating_group'],
                        'watchlist': op_db['watchlist'],
                        'covenants': {'ltv': op_db['ltv'], 'dscr': op_db['dscr']},
                        'defaultMonitoring': {
                            'news': op_db['monitoring_news'], 'fiiReport': op_db['monitoring_fii_report'],
                            'operationalInfo': op_db['monitoring_operational_info'],
                            'receivablesPortfolio': op_db['monitoring_receivables_portfolio'],
                            'monthlyConstructionReport': op_db['monitoring_construction_report'],
                            'monthlyCommercialInfo': op_db['monitoring_commercial_info'],
                            'speDfs': op_db['monitoring_spe_dfs']
                        },
                        'projects': [], 'guarantees': [], 'events': [],
                        'taskRules': [], 'ratingHistory': [], 'tasks': []
                    }

                op_ids = list(operations_map.keys())
                placeholders = ', '.join(['?'] * len(op_ids))
                
                # 2. Busca todos os dados relacionados de uma vez usando "WHERE IN"
                cursor.execute(f"SELECT op.operation_id, p.id, p.name FROM cri.crm.projects p JOIN cri.crm.operation_projects op ON p.id = op.project_id WHERE op.operation_id IN ({placeholders})", op_ids)
                for row in cursor.fetchall(): operations_map[row.operation_id]['projects'].append({'id': row.id, 'name': row.name})

                cursor.execute(f"SELECT og.operation_id, g.id, g.name FROM cri.crm.guarantees g JOIN cri.crm.operation_guarantees og ON g.id = og.guarantee_id WHERE og.operation_id IN ({placeholders})", op_ids)
                for row in cursor.fetchall(): operations_map[row.operation_id]['guarantees'].append({'id': row.id, 'name': row.name})

                cursor.execute(f"SELECT * FROM cri.crm.events WHERE operation_id IN ({placeholders}) ORDER BY date DESC", op_ids)
                for row in cursor.fetchall(): 
                    event = format_row(row, cursor)
                    if event.get('date'): event['date'] = event['date'].isoformat()
                    operations_map[row.operation_id]['events'].append(event)

                cursor.execute(f"SELECT * FROM cri.crm.task_rules WHERE operation_id IN ({placeholders})", op_ids)
                for row in cursor.fetchall(): 
                    rule = format_row(row, cursor)
                    if rule.get('startDate'): rule['startDate'] = rule['startDate'].isoformat()
                    if rule.get('endDate'): rule['endDate'] = rule['endDate'].isoformat()
                    operations_map[row.operation_id]['taskRules'].append(rule)

                cursor.execute(f"SELECT * FROM cri.crm.rating_history WHERE operation_id IN ({placeholders}) ORDER BY date DESC", op_ids)
                for row in cursor.fetchall(): 
                    rh = format_row(row, cursor)
                    if rh.get('date'): rh['date'] = rh['date'].isoformat()
                    operations_map[row.operation_id]['ratingHistory'].append(rh)

                cursor.execute(f"SELECT operation_id, task_id FROM cri.crm.task_exceptions WHERE operation_id IN ({placeholders})", op_ids)
                exceptions_by_op = defaultdict(set)
                for row in cursor.fetchall(): exceptions_by_op[row.operation_id].add(row.task_id)

                # 3. Processa as tarefas e datas de revisão em memória para cada operação
                for op_id, op in operations_map.items():
                    task_exceptions = exceptions_by_op.get(op_id, set())
                    tasks = generate_tasks_for_operation(op, task_exceptions)
                    op['tasks'] = tasks
                    op['overdueCount'] = sum(1 for task in tasks if task['status'] == 'Atrasada')

                    today = date.today()
                    pending_tasks = [t for t in tasks if t['status'] != 'Concluída' and datetime.fromisoformat(t['dueDate']).date() >= today]
                    next_gerencial_tasks = sorted([t['dueDate'] for t in pending_tasks if t['ruleName'] == 'Revisão Gerencial'])
                    next_politica_tasks = sorted([t['dueDate'] for t in pending_tasks if t['ruleName'] == 'Revisão Política'])
                    op['nextReviewGerencial'] = next_gerencial_tasks[0] if next_gerencial_tasks else None
                    op['nextReviewPolitica'] = next_politica_tasks[0] if next_politica_tasks else None

            return jsonify(list(operations_map.values()))
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
                politica_freq = RATING_TO_POLITICA_FREQUENCY.get(data['ratingGroup'], 'Anual')
                gerencial_freq = data['reviewFrequency']

                if FREQUENCY_VALUE_MAP.get(gerencial_freq, 999) > FREQUENCY_VALUE_MAP.get(politica_freq, 0):
                    gerencial_freq = politica_freq
                data['reviewFrequency'] = gerencial_freq

                dm = data.get('defaultMonitoring', {})
                cursor.execute(
                    "INSERT INTO cri.crm.operations (name, area, operation_type, maturity_date, responsible_analyst, review_frequency, call_frequency, df_frequency, segmento, rating_operation, rating_group, watchlist, ltv, dscr, monitoring_news, monitoring_fii_report, monitoring_operational_info, monitoring_receivables_portfolio, monitoring_construction_report, monitoring_commercial_info, monitoring_spe_dfs) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (data['name'], data['area'], data['operationType'], data['maturityDate'], data['responsibleAnalyst'], data['reviewFrequency'], data['callFrequency'], data['dfFrequency'], data['segmento'], data['ratingOperation'], data['ratingGroup'], data['watchlist'], data.get('covenants', {}).get('ltv'), data.get('covenants', {}).get('dscr'), dm.get('news'), dm.get('fiiReport'), dm.get('operationalInfo'), dm.get('receivablesPortfolio'), dm.get('monthlyConstructionReport'), dm.get('monthlyCommercialInfo'), dm.get('speDfs'))
                )
                cursor.execute("SELECT id FROM cri.crm.operations WHERE name = ? ORDER BY id DESC LIMIT 1", (data['name'],))
                new_op_id = cursor.fetchone().id
                
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
            app.logger.error(f"Error in POST /api/operations: {e}", exc_info=True)
            return jsonify({"error": str(e)}), 500
        finally: 
            if conn:
                conn.close()

@app.route('/api/operations/<int:op_id>', methods=['PUT', 'DELETE'])
def manage_operation(op_id):
    conn = get_db_connection()
    if request.method == 'PUT':
        data = request.json
        try:
            with conn.cursor() as cursor:
                # FIX: Busca dados antigos para comparação e como fallback para chaves ausentes
                cursor.execute("SELECT * FROM cri.crm.operations WHERE id = ?", (op_id,))
                old_op_row = cursor.fetchone()
                if not old_op_row:
                    return jsonify({"error": f"Operação com id {op_id} não encontrada."}), 404
                old_op_db = format_row(old_op_row, cursor)
                
                old_rating_group = old_op_db.get('rating_group')
                # FIX: Usa .get() para acessar a chave de forma segura, com fallback para o valor antigo
                new_rating_group = data.get('ratingGroup', old_rating_group)
                
                if old_rating_group != new_rating_group:
                    cursor.execute("SELECT name, frequency, start_date FROM cri.crm.task_rules WHERE operation_id = ?", (op_id,))
                    all_rules = {row.name: {'frequency': row.frequency, 'start_date': row.start_date} for row in cursor.fetchall()}
                    
                    cursor.execute("SELECT type, MAX(date) as max_date FROM cri.crm.events WHERE operation_id = ? AND type = 'Revisão Periódica' GROUP BY type", (op_id,))
                    last_review_date_row = cursor.fetchone()
                    last_review_date = last_review_date_row.max_date if last_review_date_row else None

                    new_politica_freq = RATING_TO_POLITICA_FREQUENCY.get(new_rating_group, 'Anual')
                    politica_rule = all_rules.get('Revisão Política')
                    
                    if politica_rule:
                        new_politica_start_date = last_review_date or politica_rule['start_date'] or datetime.now()
                        cursor.execute("UPDATE cri.crm.task_rules SET frequency = ?, start_date = ? WHERE operation_id = ? AND name = 'Revisão Política'", (new_politica_freq, new_politica_start_date, op_id))
                        log_action(cursor, data.get('responsibleAnalyst', 'System'), 'UPDATE', 'TaskRule', op_id, f"Frequência da Revisão de Política ajustada para {new_politica_freq}.")

                    gerencial_rule = all_rules.get('Revisão Gerencial')
                    if gerencial_rule and FREQUENCY_VALUE_MAP.get(gerencial_rule['frequency'], 999) > FREQUENCY_VALUE_MAP.get(new_politica_freq, 0):
                        new_gerencial_start_date = last_review_date or gerencial_rule['start_date'] or datetime.now()
                        adjusted_gerencial_freq = new_politica_freq
                        cursor.execute("UPDATE cri.crm.task_rules SET frequency = ?, start_date = ? WHERE operation_id = ? AND name = 'Revisão Gerencial'", (adjusted_gerencial_freq, new_gerencial_start_date, op_id))
                        log_action(cursor, data.get('responsibleAnalyst', 'System'), 'UPDATE', 'TaskRule', op_id, f"Frequência da Revisão Gerencial ajustada para {adjusted_gerencial_freq}.")
                
                # FIX: Prepara os dados para o UPDATE com fallbacks para evitar KeyErrors
                cov = data.get('covenants', {})
                name_to_set = data.get('name', old_op_db.get('name'))
                area_to_set = data.get('area', old_op_db.get('area'))
                rating_op_to_set = data.get('ratingOperation', old_op_db.get('rating_operation'))
                watchlist_to_set = data.get('watchlist', old_op_db.get('watchlist'))
                ltv_to_set = cov.get('ltv', old_op_db.get('ltv'))
                dscr_to_set = cov.get('dscr', old_op_db.get('dscr'))

                cursor.execute(
                    "UPDATE cri.crm.operations SET name = ?, area = ?, rating_operation = ?, rating_group = ?, watchlist = ?, ltv = ?, dscr = ? WHERE id = ?", 
                    (name_to_set, area_to_set, rating_op_to_set, new_rating_group, watchlist_to_set, ltv_to_set, dscr_to_set, op_id)
                )
                
                for event in data.get('events', []):
                    if not isinstance(event.get('id'), int):
                        cursor.execute("INSERT INTO cri.crm.events (operation_id, date, type, title, description, registered_by, next_steps, completed_task_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                       (op_id, event['date'], event['type'], event['title'], event['description'], event['registeredBy'], event['nextSteps'], event.get('completedTaskId')))
                        log_action(cursor, event['registeredBy'], 'CREATE', 'Event', 'new', f"Evento '{event['title']}' adicionado.")

                for rh in data.get('ratingHistory', []):
                    if not isinstance(rh.get('id'), int):
                        cursor.execute("INSERT INTO cri.crm.rating_history (operation_id, date, rating_operation, rating_group, watchlist, sentiment, event_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                       (op_id, rh['date'], rh['ratingOperation'], rh['ratingGroup'], rh['watchlist'], rh['sentiment'], rh['eventId']))

                cursor.execute("SELECT id, name FROM cri.crm.task_rules WHERE operation_id = ?", (op_id,))
                db_rules_map = {row.id: row.name for row in cursor.fetchall()}
                client_rule_ids = {r['id'] for r in data.get('taskRules', []) if 'id' in r and isinstance(r['id'], int)}

                for rule_id_to_delete in set(db_rules_map.keys()) - client_rule_ids:
                    cursor.execute("DELETE FROM cri.crm.task_rules WHERE id = ?", (rule_id_to_delete,))
                    log_action(cursor, data.get('responsibleAnalyst', 'System'), 'DELETE', 'TaskRule', rule_id_to_delete, f"Regra '{db_rules_map[rule_id_to_delete]}' deletada.")

                for rule in data.get('taskRules', []):
                    rule_id = rule.get('id')
                    if rule_id and rule_id in db_rules_map:
                        if rule['name'] not in ['Revisão Política', 'Revisão Gerencial']:
                            cursor.execute("UPDATE cri.crm.task_rules SET name=?, frequency=?, start_date=?, end_date=?, description=? WHERE id=?", 
                                           (rule['name'], rule['frequency'], rule['startDate'], rule['endDate'], rule['description'], rule_id))
                    else:
                        cursor.execute("INSERT INTO cri.crm.task_rules (operation_id, name, frequency, start_date, end_date, description) VALUES (?, ?, ?, ?, ?, ?)", 
                                       (op_id, rule['name'], rule['frequency'], rule['startDate'], rule['endDate'], rule['description']))
                        log_action(cursor, data.get('responsibleAnalyst', 'System'), 'CREATE', 'TaskRule', 'new', f"Regra '{rule['name']}' adicionada.")
                
                details = generate_diff_details(old_op_db, data, {'name': 'Nome', 'ratingOperation': 'Rating Op.', 'ratingGroup': 'Rating Grupo', 'watchlist': 'Watchlist'})
                if details: 
                    log_action(cursor, data.get('responsibleAnalyst', 'System'), 'UPDATE', 'Operation', op_id, details)
            
            conn.commit()
            
            with conn.cursor() as cursor:
                updated_operation_full = fetch_full_operation(cursor, op_id)
            return jsonify(updated_operation_full)
        except Exception as e:
            # FIX: Adiciona log aprimorado para depuração
            app.logger.error(f"Erro ao manipular operação {op_id}: {e}. Dados recebidos: {json.dumps(data)}", exc_info=True)
            return jsonify({"error": str(e)}), 500
        finally: 
            if conn:
                conn.close()

    elif request.method == 'DELETE':
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT name, responsible_analyst FROM cri.crm.operations WHERE id = ?", (op_id,))
                op_info = cursor.fetchone()
                cursor.execute("DELETE FROM cri.crm.operation_projects WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri.crm.operation_guarantees WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri.crm.rating_history WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri.crm.events WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri.crm.task_rules WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri.crm.task_exceptions WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri.crm.operations WHERE id = ?", (op_id,))
                log_action(cursor, op_info.responsible_analyst if op_info else 'System', 'DELETE', 'Operation', op_id, f"Operação '{op_info.name if op_info else 'ID: ' + str(op_id)}' deletada.")
            conn.commit()
            return '', 204
        except Exception as e:
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
            cursor.execute("INSERT INTO cri.crm.task_exceptions (task_id, operation_id, deleted_at, deleted_by) VALUES (?, ?, ?, ?)", (data['originalTaskId'], data['operationId'], datetime.now(), data.get('responsibleAnalyst')))
            due_date = updates['dueDate']
            cursor.execute("INSERT INTO cri.crm.task_rules (operation_id, name, frequency, start_date, end_date, description) VALUES (?, ?, 'Pontual', ?, ?, ?)", (data['operationId'], updates['name'], due_date, due_date, f"Tarefa editada a partir de {data['originalTaskId']}"))
            log_action(cursor, data.get('responsibleAnalyst'), 'UPDATE', 'Task', data['originalTaskId'], f"Tarefa editada para ter nome '{updates['name']}' e vencimento em {due_date}.")
        conn.commit()
        with conn.cursor() as cursor:
            updated_op = fetch_full_operation(cursor, data['operationId'])
        return jsonify(updated_op)
    except Exception as e:
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
