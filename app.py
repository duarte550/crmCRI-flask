
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
    """ 
    Busca uma operação completa com todos os seus dados relacionados e tarefas geradas.
    Esta função foi otimizada para usar uma única query com LEFT JOINs, evitando o problema N+1.
    """
    
    OPTIMIZED_FETCH_QUERY = """
    SELECT
        op.*,
        p.id as project_id, p.name as project_name,
        g.id as guarantee_id, g.name as guarantee_name,
        ev.id as event_id, ev.date as event_date, ev.type as event_type, ev.title as event_title, ev.description as event_description, ev.registered_by as event_registered_by, ev.next_steps as event_next_steps, ev.completed_task_id as event_completed_task_id,
        tr.id as rule_id, tr.name as rule_name, tr.frequency as rule_frequency, tr.start_date as rule_start_date, tr.end_date as rule_end_date, tr.description as rule_description,
        rh.id as history_id, rh.date as history_date, rh.rating_operation as history_rating_operation, rh.rating_group as history_rating_group, rh.watchlist as history_watchlist, rh.sentiment as history_sentiment, rh.event_id as history_event_id
    FROM cri.crm.operations op
    LEFT JOIN cri.crm.operation_projects op_p ON op.id = op_p.operation_id
    LEFT JOIN cri.crm.projects p ON op_p.project_id = p.id
    LEFT JOIN cri.crm.operation_guarantees op_g ON op.id = op_g.operation_id
    LEFT JOIN cri.crm.guarantees g ON op_g.guarantee_id = g.id
    LEFT JOIN cri.crm.events ev ON op.id = ev.operation_id
    LEFT JOIN cri.crm.task_rules tr ON op.id = tr.operation_id
    LEFT JOIN cri.crm.rating_history rh ON op.id = rh.operation_id
    WHERE op.id = ?
    """
    cursor.execute(OPTIMIZED_FETCH_QUERY, (operation_id,))
    
    rows = cursor.fetchall()
    if not rows:
        return None

    # Processar o resultado da query para montar o objeto aninhado
    op_data = None
    projects = set()
    guarantees = set()
    events = {}
    task_rules = {}
    rating_history = {}

    for row in rows:
        if not op_data:
            op_data = {
                'id': row.id, 'name': row.name, 'area': row.area, 'operationType': row.operation_type,
                'maturityDate': row.maturity_date.isoformat() if row.maturity_date else None,
                'responsibleAnalyst': row.responsible_analyst, 'reviewFrequency': row.review_frequency,
                'callFrequency': row.call_frequency, 'dfFrequency': row.df_frequency, 'segmento': row.segmento,
                'ratingOperation': row.rating_operation, 'ratingGroup': row.rating_group, 'watchlist': row.watchlist,
                'covenants': {'ltv': row.ltv, 'dscr': row.dscr},
                'defaultMonitoring': { 'news': row.monitoring_news, 'fiiReport': row.monitoring_fii_report, 'operationalInfo': row.monitoring_operational_info, 'receivablesPortfolio': row.monitoring_receivables_portfolio, 'monthlyConstructionReport': row.monitoring_construction_report, 'monthlyCommercialInfo': row.monitoring_commercial_info, 'speDfs': row.monitoring_spe_dfs },
            }
        
        if row.project_id: projects.add((row.project_id, row.project_name))
        if row.guarantee_id: guarantees.add((row.guarantee_id, row.guarantee_name))
        if row.event_id and row.event_id not in events: events[row.event_id] = { 'id': row.event_id, 'date': row.event_date.isoformat() if row.event_date else None, 'type': row.event_type, 'title': row.event_title, 'description': row.event_description, 'registeredBy': row.event_registered_by, 'nextSteps': row.event_next_steps, 'completedTaskId': row.event_completed_task_id }
        if row.rule_id and row.rule_id not in task_rules: task_rules[row.rule_id] = { 'id': row.rule_id, 'name': row.rule_name, 'frequency': row.rule_frequency, 'startDate': row.rule_start_date.isoformat() if row.rule_start_date else None, 'endDate': row.rule_end_date.isoformat() if row.rule_end_date else None, 'description': row.rule_description }
        if row.history_id and row.history_id not in rating_history: rating_history[row.history_id] = { 'id': row.history_id, 'date': row.history_date.isoformat() if row.history_date else None, 'ratingOperation': row.history_rating_operation, 'ratingGroup': row.history_rating_group, 'watchlist': row.history_watchlist, 'sentiment': row.history_sentiment, 'eventId': row.history_event_id }

    # Finalizar a montagem do objeto
    op_data['projects'] = sorted([{'id': pid, 'name': pname} for pid, pname in projects], key=lambda x: x['name'])
    op_data['guarantees'] = sorted([{'id': gid, 'name': gname} for gid, gname in guarantees], key=lambda x: x['name'])
    op_data['events'] = sorted(list(events.values()), key=lambda x: x['date'], reverse=True)
    op_data['taskRules'] = sorted(list(task_rules.values()), key=lambda x: x['id'])
    op_data['ratingHistory'] = sorted(list(rating_history.values()), key=lambda x: x['date'], reverse=True)

    # Buscar exceções de tarefas (query rápida e separada)
    cursor.execute("SELECT task_id FROM cri.crm.task_exceptions WHERE operation_id = ?", (operation_id,))
    task_exceptions = {row.task_id for row in cursor.fetchall()}

    # Gerar tarefas com base nos dados montados
    tasks = generate_tasks_for_operation(op_data, task_exceptions)
    op_data['tasks'] = tasks
    op_data['overdueCount'] = sum(1 for task in tasks if task['status'] == 'Atrasada')

    # Calcular próximas revisões
    today = date.today()
    pending_tasks = [t for t in tasks if t['status'] != 'Concluída' and datetime.fromisoformat(t['dueDate']).date() >= today]
    next_gerencial_tasks = sorted([t['dueDate'] for t in pending_tasks if t['ruleName'] == 'Revisão Gerencial'])
    next_politica_tasks = sorted([t['dueDate'] for t in pending_tasks if t['ruleName'] == 'Revisão Política'])
    op_data['nextReviewGerencial'] = next_gerencial_tasks[0] if next_gerencial_tasks else None
    op_data['nextReviewPolitica'] = next_politica_tasks[0] if next_politica_tasks else None

    return op_data

# ================== Rotas da API ==================
@app.route('/api/operations', methods=['GET', 'POST'])
def manage_operations_collection():
    conn = get_db_connection()
    if request.method == 'GET':
        try:
            with conn.cursor() as cursor:
                # --- A lógica de busca em massa já era eficiente, mantida como está ---
                BULK_FETCH_QUERY = """
                SELECT
                    op.*,
                    p.id as project_id, p.name as project_name,
                    g.id as guarantee_id, g.name as guarantee_name,
                    ev.id as event_id, ev.date as event_date, ev.type as event_type, ev.title as event_title, ev.description as event_description, ev.registered_by as event_registered_by, ev.next_steps as event_next_steps, ev.completed_task_id as event_completed_task_id,
                    tr.id as rule_id, tr.name as rule_name, tr.frequency as rule_frequency, tr.start_date as rule_start_date, tr.end_date as rule_end_date, tr.description as rule_description,
                    rh.id as history_id, rh.date as history_date, rh.rating_operation as history_rating_operation, rh.rating_group as history_rating_group, rh.watchlist as history_watchlist, rh.sentiment as history_sentiment, rh.event_id as history_event_id
                FROM cri.crm.operations op
                LEFT JOIN cri.crm.operation_projects op_p ON op.id = op_p.operation_id
                LEFT JOIN cri.crm.projects p ON op_p.project_id = p.id
                LEFT JOIN cri.crm.operation_guarantees op_g ON op.id = op_g.operation_id
                LEFT JOIN cri.crm.guarantees g ON op_g.guarantee_id = g.id
                LEFT JOIN cri.crm.events ev ON op.id = ev.operation_id
                LEFT JOIN cri.crm.task_rules tr ON op.id = tr.operation_id
                LEFT JOIN cri.crm.rating_history rh ON op.id = rh.operation_id
                ORDER BY op.name, op.id;
                """
                cursor.execute(BULK_FETCH_QUERY)
                
                operations_map = {}
                op_order = []

                # Processar os resultados
                for row in cursor.fetchall():
                    op_id = row.id
                    if op_id not in operations_map:
                        op_order.append(op_id)
                        operations_map[op_id] = {
                            'id': row.id, 'name': row.name, 'area': row.area, 'operationType': row.operation_type,
                            'maturityDate': row.maturity_date.isoformat() if row.maturity_date else None,
                            'responsibleAnalyst': row.responsible_analyst, 'reviewFrequency': row.review_frequency,
                            'callFrequency': row.call_frequency, 'dfFrequency': row.df_frequency, 'segmento': row.segmento,
                            'ratingOperation': row.rating_operation, 'ratingGroup': row.rating_group, 'watchlist': row.watchlist,
                            'covenants': {'ltv': row.ltv, 'dscr': row.dscr},
                            'defaultMonitoring': { 'news': row.monitoring_news, 'fiiReport': row.monitoring_fii_report, 'operationalInfo': row.monitoring_operational_info, 'receivablesPortfolio': row.monitoring_receivables_portfolio, 'monthlyConstructionReport': row.monitoring_construction_report, 'monthlyCommercialInfo': row.monitoring_commercial_info, 'speDfs': row.monitoring_spe_dfs },
                            'projects': set(), 'guarantees': set(), 'events': {}, 'taskRules': {}, 'ratingHistory': {}
                        }
                    
                    if row.project_id: operations_map[op_id]['projects'].add((row.project_id, row.project_name))
                    if row.guarantee_id: operations_map[op_id]['guarantees'].add((row.guarantee_id, row.guarantee_name))
                    if row.event_id: operations_map[op_id]['events'][row.event_id] = { 'id': row.event_id, 'date': row.event_date.isoformat() if row.event_date else None, 'type': row.event_type, 'title': row.event_title, 'description': row.event_description, 'registeredBy': row.event_registered_by, 'nextSteps': row.event_next_steps, 'completedTaskId': row.event_completed_task_id }
                    if row.rule_id: operations_map[op_id]['taskRules'][row.rule_id] = { 'id': row.rule_id, 'name': row.rule_name, 'frequency': row.rule_frequency, 'startDate': row.rule_start_date.isoformat() if row.rule_start_date else None, 'endDate': row.rule_end_date.isoformat() if row.rule_end_date else None, 'description': row.rule_description }
                    if row.history_id: operations_map[op_id]['ratingHistory'][row.history_id] = { 'id': row.history_id, 'date': row.history_date.isoformat() if row.history_date else None, 'ratingOperation': row.history_rating_operation, 'ratingGroup': row.history_rating_group, 'watchlist': row.history_watchlist, 'sentiment': row.history_sentiment, 'eventId': row.history_event_id }

                final_operations = []
                cursor.execute("SELECT operation_id, task_id FROM cri.crm.task_exceptions")
                exceptions_by_op = defaultdict(set)
                for row in cursor.fetchall():
                    exceptions_by_op[row.operation_id].add(row.task_id)

                for op_id in op_order:
                    op = operations_map[op_id]
                    op['projects'] = sorted([{'id': pid, 'name': pname} for pid, pname in op['projects']], key=lambda x: x['name'])
                    op['guarantees'] = sorted([{'id': gid, 'name': gname} for gid, gname in op['guarantees']], key=lambda x: x['name'])
                    op['events'] = sorted(list(op['events'].values()), key=lambda x: x['date'], reverse=True)
                    op['taskRules'] = sorted(list(op['taskRules'].values()), key=lambda x: x['id'])
                    op['ratingHistory'] = sorted(list(op['ratingHistory'].values()), key=lambda x: x['date'], reverse=True)

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

                    final_operations.append(op)

            return jsonify(final_operations)
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
                # Usa a função otimizada para retornar a nova operação
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
        try:
            data = request.json
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM cri.crm.operations WHERE id = ?", (op_id,))
                old_op_db = format_row(cursor.fetchone(), cursor) if cursor.rowcount > 0 else {}
                old_rating_group = old_op_db.get('rating_group')
                new_rating_group = data['ratingGroup']
                
                if old_rating_group != new_rating_group:
                    cursor.execute("SELECT name, frequency, start_date FROM cri.crm.task_rules WHERE operation_id = ?", (op_id,))
                    all_rules = {row.name: {'frequency': row.frequency, 'start_date': row.start_date} for row in cursor.fetchall()}
                    
                    cursor.execute("SELECT type, MAX(date) as max_date FROM cri.crm.events WHERE operation_id = ? AND type = 'Revisão Periódica' GROUP BY type", (op_id,))
                    last_review_date = cursor.fetchone()
                    last_review_date = last_review_date.max_date if last_review_date else None

                    new_politica_freq = RATING_TO_POLITICA_FREQUENCY.get(new_rating_group, 'Anual')
                    politica_rule = all_rules.get('Revisão Política')
                    
                    if politica_rule:
                        new_politica_start_date = last_review_date or politica_rule['start_date'] or datetime.now()
                        cursor.execute("UPDATE cri.crm.task_rules SET frequency = ?, start_date = ? WHERE operation_id = ? AND name = 'Revisão Política'", (new_politica_freq, new_politica_start_date, op_id))
                        log_action(cursor, data.get('responsibleAnalyst', 'System'), 'UPDATE', 'TaskRule', op_id, f"Frequência da Revisão de Política ajustada para {new_politica_freq} devido à mudança de rating.")

                    gerencial_rule = all_rules.get('Revisão Gerencial')
                    if gerencial_rule and FREQUENCY_VALUE_MAP.get(gerencial_rule['frequency'], 999) > FREQUENCY_VALUE_MAP.get(new_politica_freq, 0):
                        new_gerencial_start_date = last_review_date or gerencial_rule['start_date'] or datetime.now()
                        adjusted_gerencial_freq = new_politica_freq
                        cursor.execute("UPDATE cri.crm.task_rules SET frequency = ?, start_date = ? WHERE operation_id = ? AND name = 'Revisão Gerencial'", (adjusted_gerencial_freq, new_gerencial_start_date, op_id))
                        log_action(cursor, data.get('responsibleAnalyst', 'System'), 'UPDATE', 'TaskRule', op_id, f"Frequência da Revisão Gerencial ajustada para {adjusted_gerencial_freq} para alinhar com a política.")
                
                cov = data.get('covenants', {})
                cursor.execute(
                    "UPDATE cri.crm.operations SET name = ?, area = ?, rating_operation = ?, rating_group = ?, watchlist = ?, ltv = ?, dscr = ? WHERE id = ?", 
                    (data['name'], data['area'], data['ratingOperation'], data['ratingGroup'], data['watchlist'], cov.get('ltv'), cov.get('dscr'), op_id)
                )
                
                for event in data.get('events', []):
                    if not isinstance(event.get('id'), int):
                        cursor.execute("INSERT INTO cri.crm.events (operation_id, date, type, title, description, registered_by, next_steps, completed_task_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                       (op_id, event['date'], event['type'], event['title'], event['description'], event['registeredBy'], event['nextSteps'], event.get('completedTaskId')))
                        log_action(cursor, event['registeredBy'], 'CREATE', 'Event', 'new', f"Evento '{event['title']}' adicionado à operação '{data['name']}'.")

                for rh in data.get('ratingHistory', []):
                    if not isinstance(rh.get('id'), int):
                        cursor.execute("INSERT INTO cri.crm.rating_history (operation_id, date, rating_operation, rating_group, watchlist, sentiment, event_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                       (op_id, rh['date'], rh['ratingOperation'], rh['ratingGroup'], rh['watchlist'], rh['sentiment'], rh['eventId']))

                cursor.execute("SELECT id, name FROM cri.crm.task_rules WHERE operation_id = ?", (op_id,))
                db_rules_map = {row.id: row.name for row in cursor.fetchall()}
                client_rule_ids = {r['id'] for r in data.get('taskRules', []) if 'id' in r and isinstance(r['id'], int)}

                for rule_id_to_delete in set(db_rules_map.keys()) - client_rule_ids:
                    cursor.execute("DELETE FROM cri.crm.task_rules WHERE id = ?", (rule_id_to_delete,))
                    log_action(cursor, data.get('responsibleAnalyst', 'System'), 'DELETE', 'TaskRule', rule_id_to_delete, f"Regra '{db_rules_map[rule_id_to_delete]}' deletada da operação '{data['name']}'.")

                for rule in data.get('taskRules', []):
                    rule_id = rule.get('id')
                    if rule_id and rule_id in db_rules_map:
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
                # Usa a função otimizada para retornar o estado atualizado
                updated_operation_full = fetch_full_operation(cursor, op_id)
            return jsonify(updated_operation_full)
        except Exception as e:
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
