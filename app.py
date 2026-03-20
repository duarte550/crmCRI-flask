
import os
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from db import get_db_connection
from task_engine import generate_tasks_for_operation
import update_db
from datetime import datetime, date, timedelta
from collections import defaultdict
import json
import logging

# Configurações básicas de logging
# Serve static files from 'dist' folder in production
app = Flask(__name__, static_folder='../dist', static_url_path='')
logging.basicConfig(level=logging.INFO)

# Run schema updates on startup
update_db.update_schema()

# Configuração de CORS para permitir requisições de qualquer origem.
CORS(app, supports_credentials=True)

# Regras de negócio centralizadas
RATING_TO_POLITICA_FREQUENCY = {
    # Anual (Melhor que B1)
    'A4': 'Anual', 'Baa1': 'Anual', 'Baa3': 'Anual', 'Baa4': 'Anual',
    'Ba1': 'Anual', 'Ba4': 'Anual', 'Ba5': 'Anual', 'Ba6': 'Anual',
    # Semestral (B1 ou pior)
    'B1': 'Semestral', 'B2': 'Semestral', 'B3': 'Semestral', 'B4': 'Semestral',
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
        INSERT INTO cri_cra_dev.crm.audit_logs (timestamp, user_name, action, entity_type, entity_id, details)
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
    Busca uma operação completa com todos os seus dados, garantindo que todas as chaves
    sejam convertidas para camelCase para o frontend.
    """
    cursor.execute("SELECT * FROM cri_cra_dev.crm.operations WHERE id = ?", (operation_id,))
    op_row = cursor.fetchone()
    if not op_row:
        return None
    
    # Monta o dicionário base da operação
    operation_db = format_row(op_row, cursor)
    operation = {
        'id': operation_db['id'], 'name': operation_db['name'], 'area': operation_db['area'],
        'operationType': operation_db['operation_type'],
        'maturityDate': operation_db['maturity_date'].isoformat() if operation_db.get('maturity_date') else None,
        'estimatedDate': operation_db.get('estimated_date').isoformat() if operation_db.get('estimated_date') else None,
        'responsibleAnalyst': operation_db['responsible_analyst'], 'reviewFrequency': operation_db['review_frequency'],
        'callFrequency': operation_db['call_frequency'], 'dfFrequency': operation_db['df_frequency'],
        'segmento': operation_db['segmento'], 'ratingOperation': operation_db['rating_operation'],
        'ratingGroup': operation_db['rating_group'], 'watchlist': operation_db['watchlist'],
        'covenants': {'ltv': operation_db['ltv'], 'dscr': operation_db['dscr']},
        'defaultMonitoring': {
            'news': operation_db.get('monitoring_news') or False,
            'fiiReport': operation_db.get('monitoring_fii_report') or False,
            'operationalInfo': operation_db.get('monitoring_operational_info') or False,
            'receivablesPortfolio': operation_db.get('monitoring_receivables_portfolio') or False,
            'monthlyConstructionReport': operation_db.get('monitoring_construction_report') or False,
            'monthlyCommercialInfo': operation_db.get('monitoring_commercial_info') or False,
            'speDfs': operation_db.get('monitoring_spe_dfs') or False
        },
        'notes': None # Initialize notes as None
    }

    # Tenta buscar as notas em uma query separada para evitar erro se a tabela não existir
    try:
        cursor.execute("SELECT notes FROM cri_cra_dev.crm.operation_review_notes WHERE operation_id = ?", (operation_id,))
        notes_row = cursor.fetchone()
        if notes_row:
            operation['notes'] = notes_row.notes
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            app.logger.warning("Tabela 'operation_review_notes' não encontrada. Pulando campo de notas para a operação ID %s.", operation_id)
        else:
            raise e # Re-lança outros erros de DB inesperados

    # Busca dados relacionados em queries separadas
    cursor.execute("SELECT p.id, p.name FROM cri_cra_dev.crm.projects p JOIN cri_cra_dev.crm.operation_projects op ON p.id = op.project_id WHERE op.operation_id = ?", (operation_id,))
    operation['projects'] = [{'id': r.id, 'name': r.name} for r in cursor.fetchall()]

    cursor.execute("SELECT g.id, g.name FROM cri_cra_dev.crm.guarantees g JOIN cri_cra_dev.crm.operation_guarantees og ON g.id = og.guarantee_id WHERE og.operation_id = ?", (operation_id,))
    operation['guarantees'] = [{'id': r.id, 'name': r.name} for r in cursor.fetchall()]
    
    cursor.execute("SELECT * FROM cri_cra_dev.crm.events WHERE operation_id = ? ORDER BY date DESC", (operation_id,))
    db_events = [format_row(r, cursor) for r in cursor.fetchall()]
    operation['events'] = [{
        'id': e.get('id'), 'date': e.get('date').isoformat() if e.get('date') else None,
        'type': e.get('type'), 'title': e.get('title'), 'description': e.get('description'),
        'registeredBy': e.get('registered_by'), 'nextSteps': e.get('next_steps'),
        'completedTaskId': e.get('completed_task_id')
    } for e in db_events]

    cursor.execute("SELECT * FROM cri_cra_dev.crm.task_rules WHERE operation_id = ?", (operation_id,))
    db_rules = [format_row(r, cursor) for r in cursor.fetchall()]
    operation['taskRules'] = [{
        'id': r.get('id'), 'name': r.get('name'), 'frequency': r.get('frequency'),
        'startDate': r.get('start_date').isoformat() if r.get('start_date') else None,
        'endDate': r.get('end_date').isoformat() if r.get('end_date') else None,
        'description': r.get('description'),
        'priority': r.get('priority')
    } for r in db_rules]

    cursor.execute("SELECT * FROM cri_cra_dev.crm.rating_history WHERE operation_id = ? ORDER BY date DESC", (operation_id,))
    db_rh = [format_row(r, cursor) for r in cursor.fetchall()]
    operation['ratingHistory'] = [{
        'id': rh.get('id'), 'date': rh.get('date').isoformat() if rh.get('date') else None,
        'ratingOperation': rh.get('rating_operation'), 'ratingGroup': rh.get('rating_group'),
        'watchlist': rh.get('watchlist'), 'sentiment': rh.get('sentiment'), 'eventId': rh.get('event_id')
    } for rh in db_rh]

    # FIX: Ensure review rules always extend to the maturity date before generating tasks.
    maturity_date_iso = operation.get('maturityDate')
    if maturity_date_iso:
        for rule in operation.get('taskRules', []):
            if rule.get('name') in ['Revisão Gerencial', 'Revisão Política']:
                rule['endDate'] = maturity_date_iso

    cursor.execute("SELECT task_id FROM cri_cra_dev.crm.task_exceptions WHERE operation_id = ?", (operation_id,))
    task_exceptions = {row.task_id for row in cursor.fetchall()}

    tasks = generate_tasks_for_operation(operation, task_exceptions)
    operation['tasks'] = tasks
    operation['overdueCount'] = sum(1 for task in tasks if task['status'] == 'Atrasada')

    # FIX: Check if operation has matured. If so, no more reviews are due.
    maturity_date_obj = datetime.fromisoformat(maturity_date_iso).date() if maturity_date_iso else None
    if maturity_date_obj and maturity_date_obj < date.today():
        operation['nextReviewGerencialTask'] = None
        operation['nextReviewPoliticaTask'] = None
        operation['nextReviewGerencial'] = None
        operation['nextReviewPolitica'] = None
    else:
        pending_and_overdue_tasks = [t for t in tasks if t['status'] != 'Concluída']
        gerencial_tasks = sorted([t for t in pending_and_overdue_tasks if t['ruleName'] == 'Revisão Gerencial'], key=lambda t: t['dueDate'])
        politica_tasks = sorted([t for t in pending_and_overdue_tasks if t['ruleName'] == 'Revisão Política'], key=lambda t: t['dueDate'])
        
        operation['nextReviewGerencialTask'] = gerencial_tasks[0] if gerencial_tasks else None
        operation['nextReviewPoliticaTask'] = politica_tasks[0] if politica_tasks else None
        operation['nextReviewGerencial'] = gerencial_tasks[0]['dueDate'] if gerencial_tasks else None
        operation['nextReviewPolitica'] = politica_tasks[0]['dueDate'] if politica_tasks else None

    return operation

# ================== Rotas da API ==================
@app.route('/api/operations', methods=['GET', 'POST'])
def manage_operations_collection():
    conn = get_db_connection()
    if request.method == 'GET':
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM cri_cra_dev.crm.operations ORDER BY name")
                db_operations = [format_row(row, cursor) for row in cursor.fetchall()]
                if not db_operations: return jsonify([])

                notes_map = {}
                try:
                    cursor.execute("SELECT operation_id, notes FROM cri_cra_dev.crm.operation_review_notes")
                    for row in cursor.fetchall():
                        notes_map[row.operation_id] = row.notes
                except Exception as e:
                    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                        app.logger.warning("Tabela 'operation_review_notes' não encontrada. Pulando campo de notas para todas as operações.")
                    else:
                        raise e

                operations_map = {}
                for op_db in db_operations:
                    op_id = op_db['id']
                    operations_map[op_id] = {
                        'id': op_id, 'name': op_db['name'], 'area': op_db['area'],
                        'operationType': op_db['operation_type'],
                        'maturityDate': op_db['maturity_date'].isoformat() if op_db.get('maturity_date') else None,
                        'estimatedDate': op_db.get('estimated_date').isoformat() if op_db.get('estimated_date') else None,
                        'responsibleAnalyst': op_db['responsible_analyst'], 'reviewFrequency': op_db['review_frequency'],
                        'callFrequency': op_db['call_frequency'], 'dfFrequency': op_db['df_frequency'],
                        'segmento': op_db['segmento'], 'ratingOperation': op_db['rating_operation'],
                        'ratingGroup': op_db['rating_group'], 'watchlist': op_db['watchlist'],
                        'covenants': {'ltv': op_db['ltv'], 'dscr': op_db['dscr']},
                        'defaultMonitoring': {
                            'news': op_db.get('monitoring_news') or False,
                            'fiiReport': op_db.get('monitoring_fii_report') or False,
                            'operationalInfo': op_db.get('monitoring_operational_info') or False,
                            'receivablesPortfolio': op_db.get('monitoring_receivables_portfolio') or False,
                            'monthlyConstructionReport': op_db.get('monitoring_construction_report') or False,
                            'monthlyCommercialInfo': op_db.get('monitoring_commercial_info') or False,
                            'speDfs': op_db.get('monitoring_spe_dfs') or False
                        },
                        'projects': [], 'guarantees': [], 'events': [], 'taskRules': [], 'ratingHistory': [], 'tasks': [],
                        'notes': notes_map.get(op_id)
                    }

                op_ids = list(operations_map.keys())
                placeholders = ', '.join(['?'] * len(op_ids))
                
                cursor.execute(f"SELECT op.operation_id, p.id, p.name FROM cri_cra_dev.crm.projects p JOIN cri_cra_dev.crm.operation_projects op ON p.id = op.project_id WHERE op.operation_id IN ({placeholders})", op_ids)
                for row in cursor.fetchall(): operations_map[row.operation_id]['projects'].append({'id': row.id, 'name': row.name})

                cursor.execute(f"SELECT og.operation_id, g.id, g.name FROM cri_cra_dev.crm.guarantees g JOIN cri_cra_dev.crm.operation_guarantees og ON g.id = og.guarantee_id WHERE og.operation_id IN ({placeholders})", op_ids)
                for row in cursor.fetchall(): operations_map[row.operation_id]['guarantees'].append({'id': row.id, 'name': row.name})

                cursor.execute(f"SELECT * FROM cri_cra_dev.crm.events WHERE operation_id IN ({placeholders}) ORDER BY date DESC", op_ids)
                for row in cursor.fetchall():
                    event_db = format_row(row, cursor)
                    operations_map[row.operation_id]['events'].append({ 'id': event_db.get('id'), 'date': event_db.get('date').isoformat() if event_db.get('date') else None, 'type': event_db.get('type'), 'title': event_db.get('title'), 'description': event_db.get('description'), 'registeredBy': event_db.get('registered_by'), 'nextSteps': event_db.get('next_steps'), 'completedTaskId': event_db.get('completed_task_id') })

                cursor.execute(f"SELECT * FROM cri_cra_dev.crm.task_rules WHERE operation_id IN ({placeholders})", op_ids)
                for row in cursor.fetchall():
                    rule_db = format_row(row, cursor)
                    operations_map[row.operation_id]['taskRules'].append({ 
                        'id': rule_db.get('id'), 
                        'name': rule_db.get('name'), 
                        'frequency': rule_db.get('frequency'), 
                        'startDate': rule_db.get('start_date').isoformat() if rule_db.get('start_date') else None, 
                        'endDate': rule_db.get('end_date').isoformat() if rule_db.get('end_date') else None, 
                        'description': rule_db.get('description'),
                        'priority': rule_db.get('priority')
                    })

                cursor.execute(f"SELECT * FROM cri_cra_dev.crm.rating_history WHERE operation_id IN ({placeholders}) ORDER BY date DESC", op_ids)
                for row in cursor.fetchall():
                    rh_db = format_row(row, cursor)
                    operations_map[row.operation_id]['ratingHistory'].append({ 'id': rh_db.get('id'), 'date': rh_db.get('date').isoformat() if rh_db.get('date') else None, 'ratingOperation': rh_db.get('rating_operation'), 'ratingGroup': rh_db.get('rating_group'), 'watchlist': rh_db.get('watchlist'), 'sentiment': rh_db.get('sentiment'), 'eventId': rh_db.get('event_id') })

                cursor.execute(f"SELECT operation_id, task_id FROM cri_cra_dev.crm.task_exceptions WHERE operation_id IN ({placeholders})", op_ids)
                exceptions_by_op = defaultdict(set)
                for row in cursor.fetchall(): exceptions_by_op[row.operation_id].add(row.task_id)

                for op_id, op in operations_map.items():
                    maturity_date_iso = op.get('maturityDate')
                    if maturity_date_iso:
                        for rule in op.get('taskRules', []):
                            if rule.get('name') in ['Revisão Gerencial', 'Revisão Política']:
                                rule['endDate'] = maturity_date_iso

                    tasks = generate_tasks_for_operation(op, exceptions_by_op.get(op_id, set()))
                    op['tasks'] = tasks
                    op['overdueCount'] = sum(1 for task in tasks if task['status'] == 'Atrasada')

                    maturity_date_obj = datetime.fromisoformat(maturity_date_iso).date() if maturity_date_iso else None
                    if maturity_date_obj and maturity_date_obj < date.today():
                        op['nextReviewGerencialTask'] = None
                        op['nextReviewPoliticaTask'] = None
                        op['nextReviewGerencial'] = None
                        op['nextReviewPolitica'] = None
                    else:
                        pending_and_overdue_tasks = [t for t in tasks if t['status'] != 'Concluída']
                        gerencial_tasks = sorted([t for t in pending_and_overdue_tasks if t['ruleName'] == 'Revisão Gerencial'], key=lambda t: t['dueDate'])
                        politica_tasks = sorted([t for t in pending_and_overdue_tasks if t['ruleName'] == 'Revisão Política'], key=lambda t: t['dueDate'])
                        
                        op['nextReviewGerencialTask'] = gerencial_tasks[0] if gerencial_tasks else None
                        op['nextReviewPoliticaTask'] = politica_tasks[0] if politica_tasks else None
                        op['nextReviewGerencial'] = gerencial_tasks[0]['dueDate'] if gerencial_tasks else None
                        op['nextReviewPolitica'] = politica_tasks[0]['dueDate'] if politica_tasks else None

            return jsonify(list(operations_map.values()))
        except Exception as e:
            app.logger.error(f"Exception on /api/operations [GET]: {e}", exc_info=True)
            return jsonify({"error": str(e)}), 500
        finally: 
            if conn: conn.close()
    
    elif request.method == 'POST':
        try:
            data = request.json
            with conn.cursor() as cursor:
                politica_freq = RATING_TO_POLITICA_FREQUENCY.get(data['ratingGroup'], 'Anual')
                gerencial_freq = data['reviewFrequency']

                if FREQUENCY_VALUE_MAP.get(gerencial_freq, 999) > FREQUENCY_VALUE_MAP.get(politica_freq, 0):
                    gerencial_freq = politica_freq
                
                # A frequência de revisão da operação (gerencial) é salva no banco.
                data['reviewFrequency'] = gerencial_freq

                dm = data.get('defaultMonitoring', {})
                est_date = data.get('estimatedDate')
                if est_date == "": est_date = None
                
                cursor.execute( "INSERT INTO cri_cra_dev.crm.operations (name, area, operation_type, maturity_date, responsible_analyst, review_frequency, call_frequency, df_frequency, segmento, rating_operation, rating_group, watchlist, ltv, dscr, monitoring_news, monitoring_fii_report, monitoring_operational_info, monitoring_receivables_portfolio, monitoring_construction_report, monitoring_commercial_info, monitoring_spe_dfs, estimated_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (data['name'], data['area'], data['operationType'], data['maturityDate'], data['responsibleAnalyst'], data['reviewFrequency'], data['callFrequency'], data['dfFrequency'], data['segmento'], data['ratingOperation'], data['ratingGroup'], data['watchlist'], data.get('covenants', {}).get('ltv'), data.get('covenants', {}).get('dscr'), dm.get('news'), dm.get('fiiReport'), dm.get('operationalInfo'), dm.get('receivablesPortfolio'), dm.get('monthlyConstructionReport'), dm.get('monthlyCommercialInfo'), dm.get('speDfs'), est_date) )
                cursor.execute("SELECT id FROM cri_cra_dev.crm.operations WHERE name = ? ORDER BY id DESC LIMIT 1", (data['name'],))
                new_op_id = cursor.fetchone().id
                
                # FIX: Handle saving projects and guarantees for new operations
                for project in data.get('projects', []):
                    project_name = project.get('name')
                    if project_name:
                        cursor.execute("SELECT id FROM cri_cra_dev.crm.projects WHERE name = ?", (project_name,))
                        proj_row = cursor.fetchone()
                        if proj_row:
                            project_id = proj_row.id
                        else:
                            cursor.execute("INSERT INTO cri_cra_dev.crm.projects (name) VALUES (?)", (project_name,))
                            cursor.execute("SELECT id FROM cri_cra_dev.crm.projects WHERE name = ? ORDER BY id DESC LIMIT 1", (project_name,))
                            project_id = cursor.fetchone().id
                        cursor.execute("INSERT INTO cri_cra_dev.crm.operation_projects (operation_id, project_id) VALUES (?, ?)", (new_op_id, project_id))
                
                for guarantee in data.get('guarantees', []):
                    guarantee_name = guarantee.get('name')
                    if guarantee_name:
                        cursor.execute("SELECT id FROM cri_cra_dev.crm.guarantees WHERE name = ?", (guarantee_name,))
                        guar_row = cursor.fetchone()
                        if guar_row:
                            guarantee_id = guar_row.id
                        else:
                            cursor.execute("INSERT INTO cri_cra_dev.crm.guarantees (name) VALUES (?)", (guarantee_name,))
                            cursor.execute("SELECT id FROM cri_cra_dev.crm.guarantees WHERE name = ? ORDER BY id DESC LIMIT 1", (guarantee_name,))
                            guarantee_id = cursor.fetchone().id
                        cursor.execute("INSERT INTO cri_cra_dev.crm.operation_guarantees (operation_id, guarantee_id) VALUES (?, ?)", (new_op_id, guarantee_id))

                today, end_date_iso = datetime.now().isoformat(), data['maturityDate']
                rules_to_add = [ {'name': 'Revisão Gerencial', 'frequency': gerencial_freq, 'desc': 'Revisão periódica gerencial.', 'priority': 'Alta'}, {'name': 'Revisão Política', 'frequency': politica_freq, 'desc': 'Revisão de política de crédito anual.', 'priority': 'Alta'}, {'name': 'Call de Acompanhamento', 'frequency': data['callFrequency'], 'desc': 'Call de acompanhamento.', 'priority': 'Média'}, {'name': 'Análise de DFs & Dívida', 'frequency': data['dfFrequency'], 'desc': 'Análise dos DFs.', 'priority': 'Média'} ]
                if dm.get('news'): rules_to_add.append({'name': 'Monitorar Notícias', 'frequency': 'Semanal', 'desc': 'Acompanhar notícias.', 'priority': 'Baixa'})
                for rule in rules_to_add:
                    cursor.execute("INSERT INTO cri_cra_dev.crm.task_rules (operation_id, name, frequency, start_date, end_date, description, priority) VALUES (?, ?, ?, ?, ?, ?, ?)", (new_op_id, rule['name'], rule['frequency'], today, end_date_iso, rule['desc'], rule.get('priority') or 'Média'))
                
                cursor.execute("INSERT INTO cri_cra_dev.crm.rating_history (operation_id, date, rating_operation, rating_group, watchlist, sentiment) VALUES (?, ?, ?, ?, ?, ?)", (new_op_id, today, data['ratingOperation'], data['ratingGroup'], data['watchlist'], 'Neutro'))
                
                # Save notes if provided
                if data.get('notes'):
                    cursor.execute(
                        "INSERT INTO cri_cra_dev.crm.operation_review_notes (operation_id, notes, updated_at, updated_by) VALUES (?, ?, ?, ?)",
                        (new_op_id, data['notes'], datetime.now(), data.get('responsibleAnalyst', 'System'))
                    )

                log_action(cursor, data.get('responsibleAnalyst', 'System'), 'CREATE', 'Operation', new_op_id, f"Operação '{data['name']}' criada na área '{data['area']}'.")
            conn.commit()
            
            with conn.cursor() as cursor:
                new_operation_full = fetch_full_operation(cursor, new_op_id)
            return jsonify(new_operation_full), 201
        except Exception as e:
            app.logger.error(f"Error in POST /api/operations: {e}", exc_info=True)
            return jsonify({"error": str(e)}), 500
        finally: 
            if conn: conn.close()

@app.route('/api/operations/<int:op_id>', methods=['GET', 'PUT', 'DELETE'])
def manage_operation(op_id):
    conn = get_db_connection()
    if request.method == 'GET':
        try:
            with conn.cursor() as cursor:
                operation = fetch_full_operation(cursor, op_id)
                if not operation:
                    return jsonify({"error": "Operation not found"}), 404
                return jsonify(operation)
        except Exception as e:
            app.logger.error(f"Error fetching operation {op_id}: {e}", exc_info=True)
            return jsonify({"error": str(e)}), 500
        finally:
            if conn: conn.close()

    elif request.method == 'PUT':
        data = request.json
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM cri_cra_dev.crm.operations WHERE id = ?", (op_id,))
                old_op_row = cursor.fetchone()
                if not old_op_row: return jsonify({"error": f"Operação com id {op_id} não encontrada."}), 404
                old_op_db = format_row(old_op_row, cursor)
                
                cursor.execute("SELECT id FROM cri_cra_dev.crm.events WHERE operation_id = ?", (op_id,))
                db_event_ids = {row.id for row in cursor.fetchall()}
                cursor.execute("SELECT id FROM cri_cra_dev.crm.rating_history WHERE operation_id = ?", (op_id,))
                db_rh_ids = {row.id for row in cursor.fetchall()}

                old_rating_group, new_rating_group = old_op_db.get('rating_group'), data.get('ratingGroup', old_op_db.get('rating_group'))
                
                if old_rating_group != new_rating_group:
                    cursor.execute("SELECT name, frequency, start_date FROM cri_cra_dev.crm.task_rules WHERE operation_id = ?", (op_id,))
                    all_rules = {row.name: {'frequency': row.frequency, 'start_date': row.start_date} for row in cursor.fetchall()}
                    cursor.execute("SELECT type, MAX(date) as max_date FROM cri_cra_dev.crm.events WHERE operation_id = ? AND type = 'Revisão Periódica' GROUP BY type", (op_id,))
                    last_review_date_row = cursor.fetchone()
                    last_review_date = last_review_date_row.max_date if last_review_date_row else None
                    
                    new_politica_freq = RATING_TO_POLITICA_FREQUENCY.get(new_rating_group, 'Anual')
                    if 'Revisão Política' in all_rules:
                        start = last_review_date or all_rules['Revisão Política'].get('start_date') or datetime.now()
                        cursor.execute("UPDATE cri_cra_dev.crm.task_rules SET frequency = ?, start_date = ? WHERE operation_id = ? AND name = 'Revisão Política'", (new_politica_freq, start, op_id))
                        log_action(cursor, data.get('responsibleAnalyst', 'System'), 'UPDATE', 'TaskRule', op_id, f"Frequência da Revisão de Política ajustada para {new_politica_freq}.")

                    gerencial_rule = all_rules.get('Revisão Gerencial')
                    if gerencial_rule and FREQUENCY_VALUE_MAP.get(gerencial_rule.get('frequency'), 999) > FREQUENCY_VALUE_MAP.get(new_politica_freq, 0):
                        start = last_review_date or gerencial_rule.get('start_date') or datetime.now()
                        cursor.execute("UPDATE cri_cra_dev.crm.task_rules SET frequency = ?, start_date = ? WHERE operation_id = ? AND name = 'Revisão Gerencial'", (new_politica_freq, start, op_id))
                        log_action(cursor, data.get('responsibleAnalyst', 'System'), 'UPDATE', 'TaskRule', op_id, f"Frequência da Revisão Gerencial ajustada para {new_politica_freq}.")

                cov = data.get('covenants', {})
                
                est_date_val = data.get('estimatedDate')
                if est_date_val == "": est_date_val = None
                
                # If estimatedDate is not in data, use old value. If it is in data (even if None), use it.
                final_est_date = est_date_val if 'estimatedDate' in data else old_op_db.get('estimated_date')

                cursor.execute( "UPDATE cri_cra_dev.crm.operations SET name = ?, area = ?, rating_operation = ?, rating_group = ?, watchlist = ?, ltv = ?, dscr = ?, estimated_date = ?, maturity_date = ?, responsible_analyst = ?, segmento = ? WHERE id = ?", (data.get('name', old_op_db.get('name')), data.get('area', old_op_db.get('area')), data.get('ratingOperation', old_op_db.get('rating_operation')), new_rating_group, data.get('watchlist', old_op_db.get('watchlist')), cov.get('ltv', old_op_db.get('ltv')), cov.get('dscr', old_op_db.get('dscr')), final_est_date, data.get('maturityDate', old_op_db.get('maturity_date')), data.get('responsibleAnalyst', old_op_db.get('responsible_analyst')), data.get('segmento', old_op_db.get('segmento')), op_id) )
                
                # Update notes if provided
                if 'notes' in data:
                    cursor.execute("SELECT 1 FROM cri_cra_dev.crm.operation_review_notes WHERE operation_id = ?", (op_id,))
                    if cursor.fetchone():
                        cursor.execute(
                            "UPDATE cri_cra_dev.crm.operation_review_notes SET notes = ?, updated_at = ?, updated_by = ? WHERE operation_id = ?",
                            (data['notes'], datetime.now(), data.get('responsibleAnalyst', 'System'), op_id)
                        )
                    else:
                        cursor.execute(
                            "INSERT INTO cri_cra_dev.crm.operation_review_notes (operation_id, notes, updated_at, updated_by) VALUES (?, ?, ?, ?)",
                            (op_id, data['notes'], datetime.now(), data.get('responsibleAnalyst', 'System'))
                        )

                cursor.execute("DELETE FROM cri_cra_dev.crm.operation_projects WHERE operation_id = ?", (op_id,))
                for project in data.get('projects', []):
                    project_name = project.get('name')
                    if project_name:
                        cursor.execute("SELECT id FROM cri_cra_dev.crm.projects WHERE name = ?", (project_name,))
                        proj_row = cursor.fetchone()
                        if proj_row:
                            project_id = proj_row.id
                        else:
                            cursor.execute("INSERT INTO cri_cra_dev.crm.projects (name) VALUES (?)", (project_name,))
                            cursor.execute("SELECT id FROM cri_cra_dev.crm.projects WHERE name = ? ORDER BY id DESC LIMIT 1", (project_name,))
                            project_id = cursor.fetchone().id
                        cursor.execute("INSERT INTO cri_cra_dev.crm.operation_projects (operation_id, project_id) VALUES (?, ?)", (op_id, project_id))

                cursor.execute("DELETE FROM cri_cra_dev.crm.operation_guarantees WHERE operation_id = ?", (op_id,))
                for guarantee in data.get('guarantees', []):
                    guarantee_name = guarantee.get('name')
                    if guarantee_name:
                        cursor.execute("SELECT id FROM cri_cra_dev.crm.guarantees WHERE name = ?", (guarantee_name,))
                        guar_row = cursor.fetchone()
                        if guar_row:
                            guarantee_id = guar_row.id
                        else:
                            cursor.execute("INSERT INTO cri_cra_dev.crm.guarantees (name) VALUES (?)", (guarantee_name,))
                            cursor.execute("SELECT id FROM cri_cra_dev.crm.guarantees WHERE name = ? ORDER BY id DESC LIMIT 1", (guarantee_name,))
                            guarantee_id = cursor.fetchone().id
                        cursor.execute("INSERT INTO cri_cra_dev.crm.operation_guarantees (operation_id, guarantee_id) VALUES (?, ?)", (op_id, guarantee_id))

                # FIX: Correctly link new rating_history entries to newly created events
                client_event_id_to_db_id_map = {}
                for event in data.get('events', []):
                    if event.get('id') not in db_event_ids:
                        cursor.execute("INSERT INTO cri_cra_dev.crm.events (operation_id, date, type, title, description, registered_by, next_steps, completed_task_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (op_id, event.get('date'), event.get('type'), event.get('title'), event.get('description'), event.get('registeredBy'), event.get('nextSteps'), event.get('completedTaskId')))
                        # Fetch the real ID generated by the database
                        cursor.execute("SELECT id FROM cri_cra_dev.crm.events WHERE operation_id = ? AND date = ? AND title = ? ORDER BY id DESC LIMIT 1", (op_id, event.get('date'), event.get('title')))
                        new_event_row = cursor.fetchone()
                        if new_event_row:
                            db_event_id = new_event_row.id
                            client_event_id_to_db_id_map[event.get('id')] = db_event_id
                            log_action(cursor, event.get('registeredBy'), 'CREATE', 'Event', db_event_id, f"Evento '{event.get('title')}' adicionado.")

                for rh in data.get('ratingHistory', []):
                    if rh.get('id') not in db_rh_ids:
                        client_event_id = rh.get('eventId')
                        # Use the map to find the real DB ID, otherwise keep what was sent (could be null)
                        db_event_id_for_rh = client_event_id_to_db_id_map.get(client_event_id, client_event_id)
                        cursor.execute("INSERT INTO cri_cra_dev.crm.rating_history (operation_id, date, rating_operation, rating_group, watchlist, sentiment, event_id) VALUES (?, ?, ?, ?, ?, ?, ?)", (op_id, rh.get('date'), rh.get('ratingOperation'), rh.get('ratingGroup'), rh.get('watchlist'), rh.get('sentiment'), db_event_id_for_rh))

                cursor.execute("SELECT id, name FROM cri_cra_dev.crm.task_rules WHERE operation_id = ?", (op_id,))
                db_rules_map = {row.id: row.name for row in cursor.fetchall()}
                client_rule_ids = {r['id'] for r in data.get('taskRules', []) if 'id' in r and isinstance(r['id'], int)}

                for rule_id_to_delete in set(db_rules_map.keys()) - client_rule_ids:
                    cursor.execute("DELETE FROM cri_cra_dev.crm.task_rules WHERE id = ?", (rule_id_to_delete,))
                    log_action(cursor, data.get('responsibleAnalyst', 'System'), 'DELETE', 'TaskRule', rule_id_to_delete, f"Regra '{db_rules_map[rule_id_to_delete]}' deletada.")

                for rule in data.get('taskRules', []):
                    rule_id = rule.get('id')
                    if rule_id and rule_id in db_rules_map:
                        # FIX: Removed the guard that prevented updating 'Revisão Política' and 'Revisão Gerencial'
                        # This allows the frontend to update the startDate to the actual completion date.
                        cursor.execute("UPDATE cri_cra_dev.crm.task_rules SET name=?, frequency=?, start_date=?, end_date=?, description=?, priority=? WHERE id=?", (rule.get('name'), rule.get('frequency'), rule.get('startDate'), rule.get('endDate'), rule.get('description'), rule.get('priority') or 'Média', rule_id))
                    elif not rule_id or rule_id not in db_rules_map:
                        cursor.execute("INSERT INTO cri_cra_dev.crm.task_rules (operation_id, name, frequency, start_date, end_date, description, priority) VALUES (?, ?, ?, ?, ?, ?, ?)", (op_id, rule.get('name'), rule.get('frequency'), rule.get('startDate'), rule.get('endDate'), rule.get('description'), rule.get('priority') or 'Média'))
                        log_action(cursor, data.get('responsibleAnalyst', 'System'), 'CREATE', 'TaskRule', 'new', f"Regra '{rule.get('name')}' adicionada.")
                
                details = generate_diff_details(old_op_db, data, {'name': 'Nome', 'ratingOperation': 'Rating Op.', 'ratingGroup': 'Rating Grupo', 'watchlist': 'Watchlist'})
                if details: log_action(cursor, data.get('responsibleAnalyst', 'System'), 'UPDATE', 'Operation', op_id, details)
            
            conn.commit()
            
            with conn.cursor() as cursor:
                updated_operation_full = fetch_full_operation(cursor, op_id)
            return jsonify(updated_operation_full)
        except Exception as e:
            app.logger.error(f"Erro ao manipular operação {op_id}: {e}. Dados recebidos: {json.dumps(data)}", exc_info=True)
            return jsonify({"error": str(e)}), 500
        finally: 
            if conn: conn.close()

    elif request.method == 'DELETE':
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT name, responsible_analyst FROM cri_cra_dev.crm.operations WHERE id = ?", (op_id,))
                op_info = cursor.fetchone()
                cursor.execute("DELETE FROM cri_cra_dev.crm.operation_projects WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri_cra_dev.crm.operation_guarantees WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri_cra_dev.crm.rating_history WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri_cra_dev.crm.events WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri_cra_dev.crm.task_rules WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri_cra_dev.crm.task_exceptions WHERE operation_id = ?", (op_id,))
                cursor.execute("DELETE FROM cri_cra_dev.crm.operations WHERE id = ?", (op_id,))
                log_action(cursor, op_info.responsible_analyst if op_info else 'System', 'DELETE', 'Operation', op_id, f"Operação '{op_info.name if op_info else 'ID: ' + str(op_id)}' deletada.")
            conn.commit()
            return '', 204
        except Exception as e:
            app.logger.error(f"Error deleting operation {op_id}: {e}", exc_info=True)
            return jsonify({"error": str(e)}), 500
        finally:
            if conn: conn.close()

@app.route('/api/operations/sync-rules', methods=['POST'])
def sync_operation_rules():
    """
    Endpoint de manutenção para garantir que operações inseridas manualmente
    tenham suas regras de tarefas e histórico inicial criados.
    Também corrige datas de início de regras baseadas no histórico existente.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            fixed_count = 0
            
            # 1. Busca operações que não possuem NENHUMA regra de tarefa (Criação Inicial)
            cursor.execute("""
                SELECT o.id, o.name, o.rating_group, o.review_frequency, o.call_frequency, o.df_frequency, 
                       o.maturity_date, o.monitoring_news, o.rating_operation, o.watchlist, o.responsible_analyst
                FROM cri_cra_dev.crm.operations o
                LEFT JOIN cri_cra_dev.crm.task_rules tr ON o.id = tr.operation_id
                WHERE tr.operation_id IS NULL
                LIMIT 10
            """)
            ops_to_create_rules = [format_row(row, cursor) for row in cursor.fetchall()]
            
            for op in ops_to_create_rules:
                op_id = op['id']
                politica_freq = RATING_TO_POLITICA_FREQUENCY.get(op['rating_group'], 'Anual')
                gerencial_freq = op['review_frequency']
                
                if FREQUENCY_VALUE_MAP.get(gerencial_freq, 999) > FREQUENCY_VALUE_MAP.get(politica_freq, 0):
                    gerencial_freq = politica_freq
                
                # Tenta buscar histórico existente para usar como data base
                cursor.execute("SELECT date FROM cri_cra_dev.crm.rating_history WHERE operation_id = ? ORDER BY date DESC LIMIT 1", (op_id,))
                last_history = cursor.fetchone()
                start_date_base = last_history.date if last_history else datetime.now()
                
                end_date = op['maturity_date']
                
                rules = [
                    ('Revisão Gerencial', gerencial_freq, 'Revisão periódica gerencial.', 'Alta'),
                    ('Revisão Política', politica_freq, 'Revisão de política de crédito anual.', 'Alta'),
                    ('Call de Acompanhamento', op['call_frequency'], 'Call de acompanhamento.', 'Média'),
                    ('Análise de DFs & Dívida', op['df_frequency'], 'Análise dos DFs.', 'Média')
                ]
                
                if op.get('monitoring_news'):
                    rules.append(('Monitorar Notícias', 'Semanal', 'Acompanhar notícias.', 'Baixa'))
                
                for name, freq, desc, *rest in rules:
                    priority = rest[0] if rest else 'Média'
                    cursor.execute("""
                        INSERT INTO cri_cra_dev.crm.task_rules (operation_id, name, frequency, start_date, end_date, description, priority)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (op_id, name, freq, start_date_base, end_date, desc, priority))
                
                # Se não tinha histórico e usamos datetime.now(), cria o histórico inicial
                if not last_history:
                    cursor.execute("""
                        INSERT INTO cri_cra_dev.crm.rating_history (operation_id, date, rating_operation, rating_group, watchlist, sentiment, event_id)
                        VALUES (?, ?, ?, ?, ?, ?, NULL)
                    """, (op_id, start_date_base, op['rating_operation'], op['rating_group'], op['watchlist'], 'Neutro'))
                
                log_action(cursor, 'System', 'UPDATE', 'Operation', op_id, "Regras criadas via sync.")
                fixed_count += 1

            # 2. Corrige datas de regras existentes que estão desincronizadas com o histórico
            # Busca regras onde o start_date é diferente da data do último histórico de rating
            cursor.execute("""
                SELECT tr.id, tr.operation_id, tr.name, tr.start_date, MAX(rh.date) as last_history_date
                FROM cri_cra_dev.crm.task_rules tr
                JOIN cri_cra_dev.crm.rating_history rh ON tr.operation_id = rh.operation_id
                WHERE tr.name IN ('Revisão Gerencial', 'Revisão Política')
                GROUP BY tr.id, tr.operation_id, tr.name, tr.start_date
                HAVING MAX(rh.date) <> tr.start_date
                LIMIT 50
            """)
            rules_to_fix = [format_row(row, cursor) for row in cursor.fetchall()]
            
            for rule in rules_to_fix:
                # Atualiza start_date para a data do último histórico
                new_start_date = rule['last_history_date']
                cursor.execute("UPDATE cri_cra_dev.crm.task_rules SET start_date = ? WHERE id = ?", (new_start_date, rule['id']))
                log_action(cursor, 'System', 'UPDATE', 'TaskRule', rule['id'], f"Data base da regra '{rule['name']}' corrigida para {new_start_date} (baseado no histórico).")
                fixed_count += 1
                
            conn.commit()
            return jsonify({"status": "success", "fixed_count": fixed_count, "message": f"{fixed_count} itens processados (criação ou correção)."})
    except Exception as e:
        app.logger.error(f"Error syncing operation rules: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@app.route('/api/tasks/delete', methods=['POST'])
def delete_task():
    conn = get_db_connection()
    try:
        data = request.json
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO cri_cra_dev.crm.task_exceptions (task_id, operation_id, deleted_at, deleted_by) VALUES (?, ?, ?, ?)", (data['taskId'], data['operationId'], datetime.now(), data.get('responsibleAnalyst')))
            log_action(cursor, data.get('responsibleAnalyst'), 'DELETE', 'Task', data['taskId'], f"Tarefa deletada para a operação ID {data['operationId']}.")
        conn.commit()
        with conn.cursor() as cursor:
            updated_op = fetch_full_operation(cursor, data['operationId'])
        return jsonify(updated_op)
    except Exception as e:
        app.logger.error(f"Error deleting task: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        if conn: conn.close()
        
@app.route('/api/tasks/edit', methods=['PUT'])
def edit_task():
    conn = get_db_connection()
    try:
        data = request.json
        updates = data['updates']
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO cri_cra_dev.crm.task_exceptions (task_id, operation_id, deleted_at, deleted_by) VALUES (?, ?, ?, ?)", (data['originalTaskId'], data['operationId'], datetime.now(), data.get('responsibleAnalyst')))
            due_date = updates['dueDate']
            cursor.execute("INSERT INTO cri_cra_dev.crm.task_rules (operation_id, name, frequency, start_date, end_date, description, priority) VALUES (?, ?, 'Pontual', ?, ?, ?, ?)", (data['operationId'], updates['name'], due_date, due_date, f"Tarefa editada a partir de {data['originalTaskId']}", updates.get('priority') or 'Média'))
            log_action(cursor, data.get('responsibleAnalyst'), 'UPDATE', 'Task', data['originalTaskId'], f"Tarefa editada para ter nome '{updates['name']}' e vencimento em {due_date}.")
        conn.commit()
        with conn.cursor() as cursor:
            updated_op = fetch_full_operation(cursor, data['operationId'])
        return jsonify(updated_op)
    except Exception as e:
        app.logger.error(f"Error editing task: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        if conn: conn.close()

@app.route('/api/audit_logs', methods=['GET'])
def get_audit_logs():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM cri_cra_dev.crm.audit_logs ORDER BY timestamp DESC")
            logs = [format_row(row, cursor) for row in cursor.fetchall()]
            for log in logs:
                if log.get('timestamp'):
                    log['timestamp'] = log['timestamp'].isoformat()
            return jsonify(logs)
    except Exception as e:
        app.logger.error(f"Error fetching audit logs: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        if conn: conn.close()

@app.route('/api/operation_review_notes', methods=['POST'])
def manage_operation_review_notes():
    conn = get_db_connection()
    try:
        data = request.json
        with conn.cursor() as cursor:
            # Using MERGE for "upsert" logic
            cursor.execute("""
                MERGE INTO cri_cra_dev.crm.operation_review_notes AS target
                USING (SELECT ? AS operation_id) AS source
                ON target.operation_id = source.operation_id
                WHEN MATCHED THEN
                    UPDATE SET notes = ?, updated_at = ?, updated_by = ?
                WHEN NOT MATCHED THEN
                    INSERT (operation_id, notes, updated_at, updated_by)
                    VALUES (?, ?, ?, ?)
            """, (
                data['operationId'], 
                data['notes'], datetime.now(), data.get('userName', 'System'), # for UPDATE
                data['operationId'], data['notes'], datetime.now(), data.get('userName', 'System') # for INSERT
            ))
            log_action(cursor, data.get('userName', 'System'), 'UPDATE', 'OperationReviewNote', data['operationId'], f"Nota de revisão para operação {data['operationId']} atualizada.")
        conn.commit()
        return jsonify({'status': 'success', 'operationId': data['operationId'], 'notes': data['notes']}), 200
    except Exception as e:
        app.logger.error(f"Error saving operation review note: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        if conn: conn.close()

@app.route('/api/change-requests', methods=['GET', 'POST'])
def manage_change_requests():
    conn = get_db_connection()
    try:
        if request.method == 'GET':
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM cri_cra_dev.crm.change_requests ORDER BY created_at DESC")
                requests = [format_row(row, cursor) for row in cursor.fetchall()]
                for req in requests:
                    req['createdAt'] = req['created_at'].isoformat()
                    req['updatedAt'] = req['updated_at'].isoformat()
                    del req['created_at']
                    del req['updated_at']
                return jsonify(requests)
        
        elif request.method == 'POST':
            data = request.json
            now = datetime.now()
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO cri_cra_dev.crm.change_requests (title, description, requester, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (data['title'], data['description'], data['requester'], 'pending', now, now)
                )
                # Querying by title and requester to get the latest ID
                cursor.execute(
                    "SELECT id FROM cri_cra_dev.crm.change_requests WHERE title = ? AND requester = ? ORDER BY id DESC LIMIT 1", 
                    (data['title'], data['requester'])
                )
                new_id = cursor.fetchone().id
            conn.commit()
            return jsonify({'id': new_id, 'status': 'pending', 'createdAt': now.isoformat(), 'updatedAt': now.isoformat()}), 201
            
    except Exception as e:
        app.logger.error(f"Error in /api/change-requests: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        if conn: conn.close()

@app.route('/api/change-requests/<int:req_id>', methods=['PUT'])
def update_change_request(req_id):
    conn = get_db_connection()
    try:
        data = request.json
        now = datetime.now()
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE cri_cra_dev.crm.change_requests SET status = ?, updated_at = ? WHERE id = ?",
                (data['status'], now, req_id)
            )
        conn.commit()
        return jsonify({'status': 'success', 'updatedAt': now.isoformat()})
    except Exception as e:
        app.logger.error(f"Error updating change request {req_id}: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        if conn: conn.close()

@app.route('/api/patch-notes', methods=['GET'])
def get_patch_notes():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM cri_cra_dev.crm.patch_notes ORDER BY date DESC")
            notes = [format_row(row, cursor) for row in cursor.fetchall()]
            for note in notes:
                note['date'] = note['date'].isoformat()
                note['changes'] = json.loads(note['changes'])
            return jsonify(notes)
    except Exception as e:
        app.logger.error(f"Error fetching patch notes: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        if conn: conn.close()


# ================== Servidor de Frontend ==================
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve_react_app(path):
    if path != "" and os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    else:
        return send_from_directory(app.static_folder, 'index.html')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 3000))
    print(f"Running app on port {port}")
    app.run(host='0.0.0.0', port=port)
