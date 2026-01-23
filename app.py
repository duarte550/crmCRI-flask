import os
import json
import logging
import concurrent.futures
from datetime import datetime, date, timedelta
from collections import defaultdict

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

from db import get_db_connection
from task_engine import generate_tasks_for_operation

# Configurações básicas de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configura o Flask para servir os arquivos estáticos da pasta raiz do projeto
app = Flask(__name__, static_folder=os.path.join(os.path.dirname(__file__), '..'), static_url_path='')
CORS(app, supports_credentials=True)

# Regras de negócio centralizadas
RATING_TO_POLITICA_FREQUENCY = {
    'A4': 'Anual', 'Baa1': 'Anual', 'Baa3': 'Anual', 'Baa4': 'Anual', 'Ba1': 'Anual', 'Ba6': 'Anual',
    'B1': 'Semestral', 'B2': 'Semestral', 'B3': 'Semestral',
}

# Usado para comparar a "velocidade" das frequências. Menor número = mais frequente.
FREQUENCY_VALUE_MAP = {
    'Diário': 1, 'Semanal': 7, 'Quinzenal': 15, 'Mensal': 30,
    'Trimestral': 90, 'Semestral': 180, 'Anual': 365
}

def run_with_timeout(func, timeout=10, *args, **kwargs):
    """
    Executa func(*args, **kwargs) num ThreadPoolExecutor e retorna o resultado.
    Lança concurrent.futures.TimeoutError se exceder o timeout.
    Usar para proteger chamadas de cursor.execute() que possam bloquear.
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(func, *args, **kwargs)
        return future.result(timeout=timeout)

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
@app.route('/api/operations', methods=['GET', 'POST'])
def operations():
    conn = get_db_connection()

    if request.method == 'GET':
        try:
            def fetch_all_ops():
                with conn.cursor() as cursor:
                    # --- Otimização "1+N": Busca todas as operações e depois os dados relacionados em lotes ---
                    cursor.execute("SELECT * FROM cri.crm.operations ORDER BY name")
                    operations_list = [format_row(row, cursor) for row in cursor.fetchall()]

                    if not operations_list:
                        return []

                    op_ids = [op['id'] for op in operations_list]
                    operations_map = {op['id']: op for op in operations_list}

                    # Inicializa as listas de dados relacionados em cada operação
                    for op in operations_map.values():
                        op['projects'] = []
                        op['guarantees'] = []
                        op['events'] = []
                        op['taskRules'] = []
                        op['ratingHistory'] = []
                        op['tasks'] = []

                    # 2. Busca todos os dados relacionados de uma vez usando "WHERE IN"
                    placeholders = ', '.join(['?'] * len(op_ids))
                    cursor.execute(f"SELECT p.id, p.name, op.operation_id FROM cri.crm.projects p JOIN cri.crm.operation_projects op ON p.id = op.project_id WHERE op.operation_id IN ({placeholders})", op_ids)
                    for row in cursor.fetchall():
                        operations_map[row.operation_id]['projects'].append({'id': row.id, 'name': row.name})

                    cursor.execute(f"SELECT g.id, g.name, og.operation_id FROM cri.crm.guarantees g JOIN cri.crm.operation_guarantees og ON g.id = og.guarantee_id WHERE og.operation_id IN ({placeholders})", op_ids)
                    for row in cursor.fetchall():
                        operations_map[row.operation_id]['guarantees'].append({'id': row.id, 'name': row.name})

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

                    cursor.execute(f"SELECT task_id, operation_id FROM cri.crm.task_exceptions WHERE operation_id IN ({placeholders})", op_ids)
                    for row in cursor.fetchall():
                        operations_map[row.operation_id].setdefault('taskExceptions', []).append(row.task_id)

                    # Gerar tarefas para cada operação (pode ser pesado; avalie mover para background se necessário)
                    for op in operations_map.values():
                        task_exceptions = {t for t in op.get('taskExceptions', [])}
                        op['tasks'] = generate_tasks_for_operation(op, task_exceptions)
                        op['overdueCount'] = sum(1 for task in op['tasks'] if task['status'] == 'Atrasada')

                    return list(operations_map.values())

            # Executa a função do DB com timeout para evitar bloqueio do worker
            try:
                ops = run_with_timeout(fetch_all_ops, timeout=15)  # ajuste timeout conforme necessário
                return jsonify(ops)
            except concurrent.futures.TimeoutError:
                logger.error("DB query timed out while fetching operations")
                return jsonify({"error": "database timeout fetching operations"}), 504

        except Exception as e:
            app.logger.error(f"Error fetching operations: {e}", exc_info=True)
            return jsonify({'error': str(e)}), 500
        finally:
            if conn:
                conn.close()

    if request.method == 'POST':
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
                # Protege fetch_full_operation com timeout para evitar bloqueio indefinido
                def fetch_full():
                    return fetch_full_operation(cursor, new_op_id)
                try:
                    new_operation_full = run_with_timeout(fetch_full, timeout=12)
                except concurrent.futures.TimeoutError:
                    logger.error("DB query timed out while fetching new operation id=%s", new_op_id)
                    return jsonify({"error": "database timeout fetching operation"}), 504

            return jsonify(new_operation_full), 201
        except Exception as e:
            logger.exception("Error creating operation: %s", e)
            return jsonify({'error': str(e)}), 500
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

                # Protege a SELECT inicial com timeout
                def fetch_old_op():
                    cursor.execute("SELECT * FROM cri.crm.operations WHERE id = ?", (op_id,))
                    return cursor.fetchone()

                try:
                    row = run_with_timeout(fetch_old_op, timeout=12)  # timeout em segundos (ajustar conforme necessário)
                except concurrent.futures.TimeoutError:
                    logger.error("DB query timed out while fetching operation id=%s", op_id)
                    return jsonify({"error": "database timeout"}), 504

                old_op_db = format_row(row, cursor) if row else {}
                old_rating_group = old_op_db.get('rating_group')
                new_rating_group = data['ratingGroup']
                
                if old_rating_group != new_rating_group:
                    cursor.execute("SELECT name, frequency, start_date FROM cri.crm.task_rules WHERE operation_id = ?", (op_id,))
                    all_rules = {row.name: {'frequency': row.frequency, 'start_date': row.start_date} for row in cursor.fetchall()}
                    
                    cursor.execute("SELECT type, MAX(date) as max_date FROM cri.crm.events WHERE operation_id = ? AND type = 'Revisão Periódica' GROUP BY type", (op_id,))
                    last_review_date_row = cursor.fetchone()

                # 1. UPDATE operations table
                cov = data.get('covenants', {})
                cursor.execute(
                    "UPDATE cri.crm.operations SET name = ?, area = ?, rating_operation = ?, rating_group = ?, watchlist = ?, ltv = ?, dscr = ? WHERE id = ?", 
                    (data['name'], data['area'], data['ratingOperation'], data['ratingGroup'], data['watchlist'], cov.get('ltv'), cov.get('dscr'), op_id)
                )
                
                # 2. INSERT events (novos)
                for event in data.get('events', []):
                    if not isinstance(event.get('id'), int):
                        cursor.execute("INSERT INTO cri.crm.events (operation_id, date, type, title, description, registered_by, next_steps, completed_task_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                       (op_id, event['date'], event['type'], event['title'], event['description'], event['registeredBy'], event['nextSteps'], event.get('completedTaskId')))

                # 3. INSERT rating_history entries (novos)
                for rh in data.get('ratingHistory', []):
                    if not isinstance(rh.get('id'), int):
                        cursor.execute("INSERT INTO cri.crm.rating_history (operation_id, date, rating_operation, rating_group, watchlist, sentiment, event_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                       (op_id, rh['date'], rh['ratingOperation'], rh['ratingGroup'], rh['watchlist'], rh['sentiment'], rh['eventId']))

                # 4. UPDATE task_rules if rating changed (exemplo simplificado)
                cursor.execute("SELECT id, name FROM cri.crm.task_rules WHERE operation_id = ?", (op_id,))
                db_rules_map = {row.id: row.name for row in cursor.fetchall()}
                client_rule_ids = {r['id'] for r in data.get('taskRules', []) if 'id' in r and isinstance(r['id'], int)}

                # Remove rules that the client removed
                for db_rule_id in list(db_rules_map.keys()):
                    if db_rule_id not in client_rule_ids:
                        cursor.execute("DELETE FROM cri.crm.task_rules WHERE id = ?", (db_rule_id,))

                # Update or insert client-provided rules
                for r in data.get('taskRules', []):
                    if isinstance(r.get('id'), int) and r['id'] in db_rules_map:
                        cursor.execute("UPDATE cri.crm.task_rules SET name = ?, frequency = ?, start_date = ?, end_date = ?, description = ? WHERE id = ?",
                                       (r['name'], r['frequency'], r.get('startDate'), r.get('endDate'), r.get('desc'), r['id']))
                    else:
                        cursor.execute("INSERT INTO cri.crm.task_rules (operation_id, name, frequency, start_date, end_date, description) VALUES (?, ?, ?, ?, ?, ?)",
                                       (op_id, r['name'], r['frequency'], r.get('startDate'), r.get('endDate'), r.get('desc')))

                # 5. INSERT audit_log
                details = generate_diff_details(old_op_db, data, {
                    'name': 'Nome', 'area': 'Área', 'ratingGroup': 'Rating Grupo', 'watchlist': 'Watchlist'
                })
                log_action(cursor, data.get('responsibleAnalyst', 'System'), 'UPDATE', 'Operation', op_id, details)

            conn.commit()

            # Buscar e retornar a operação atualizada (protege com timeout)
            with conn.cursor() as cursor:
                def fetch_full():
                    return fetch_full_operation(cursor, op_id)
                try:
                    new_operation_full = run_with_timeout(fetch_full, timeout=12)
                except concurrent.futures.TimeoutError:
                    logger.error("DB query timed out while fetching full operation id=%s", op_id)
                    return jsonify({"error": "database timeout fetching operation"}), 504

            return jsonify(new_operation_full), 200

        except Exception as e:
            logger.exception("Erro ao manipular operação %s: %s", op_id, e)
            return jsonify({"error": "internal server error"}), 500
        finally:
            if conn:
                conn.close()

    if request.method == 'DELETE':
        try:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM cri.crm.operations WHERE id = ?", (op_id,))
                log_action(cursor, 'System', 'DELETE', 'Operation', op_id, f"Operação id={op_id} excluída.")
            conn.commit()
            return jsonify({}), 204
        except Exception as e:
            logger.exception("Erro ao deletar operação %s: %s", op_id, e)
            return jsonify({"error": "internal server error"}), 500
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


if __name__ == '__main__':
    # Opcional: logar hostname do Databricks (sem expor token)
    try:
        from db import os as _os
        host = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        if host:
            logger.info("Databricks host configured: %s", host)
    except Exception:
        pass

    port = int(os.getenv("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
