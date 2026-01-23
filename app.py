
import os
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from db import get_db_connection
from task_engine import generate_tasks_for_operation
from datetime import datetime, date, timedelta

# Configura o Flask para servir os arquivos estáticos da pasta raiz do projeto
app = Flask(__name__, static_folder=os.path.join(os.path.dirname(__file__), '..'), static_url_path='')

# Configuração de CORS para permitir requisições de qualquer origem.
# Isso é essencial para que o frontend (rodando em um domínio diferente, como o do AI Studio)
# possa se comunicar com este backend sem ser bloqueado pela política de segurança do navegador.
CORS(app, supports_credentials=True)


def format_row(row, cursor):
    """ Converte uma linha do banco de dados em um dicionário. """
    return {desc[0]: value for desc, value in zip(cursor.description, row)}

def fetch_full_operation(cursor, operation_id):
    """ Busca uma operação completa com todos os seus dados relacionados e tarefas geradas. """
    cursor.execute("SELECT * FROM cri.crm.operations WHERE id = ?", (operation_id,))
    op_row = cursor.fetchone()
    if not op_row:
        return None
    
    operation_db = format_row(op_row, cursor)

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

    tasks = generate_tasks_for_operation(operation)
    operation['tasks'] = tasks
    operation['overdueCount'] = sum(1 for task in tasks if task['status'] == 'Atrasada')

    return operation

# ================== Rotas da API ==================
@app.route('/api/operations', methods=['GET', 'POST'])
def manage_operations_collection():
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
            new_op_id = None
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
                new_op_row = cursor.fetchone()
                if not new_op_row: raise Exception("Failed to get new operation ID.")
                new_op_id = new_op_row.id

                for proj in data.get('projects', []):
                    cursor.execute("SELECT id FROM cri.crm.projects WHERE name = ?", (proj['name'],))
                    existing_proj = cursor.fetchone()
                    proj_id = existing_proj.id if existing_proj else None
                    if not proj_id:
                        cursor.execute("INSERT INTO cri.crm.projects (name) VALUES (?)", (proj['name'],))
                        cursor.execute("SELECT id FROM cri.crm.projects WHERE name = ? ORDER BY id DESC LIMIT 1", (proj['name'],))
                        new_proj_row = cursor.fetchone()
                        if not new_proj_row: raise Exception(f"Failed to create and retrieve project '{proj['name']}'")
                        proj_id = new_proj_row.id
                    cursor.execute("INSERT INTO cri.crm.operation_projects (operation_id, project_id) VALUES (?, ?)", (new_op_id, proj_id))

                for guar in data.get('guarantees', []):
                    cursor.execute("SELECT id FROM cri.crm.guarantees WHERE name = ?", (guar['name'],))
                    existing_guar = cursor.fetchone()
                    guar_id = existing_guar.id if existing_guar else None
                    if not guar_id:
                        cursor.execute("INSERT INTO cri.crm.guarantees (name) VALUES (?)", (guar['name'],))
                        cursor.execute("SELECT id FROM cri.crm.guarantees WHERE name = ? ORDER BY id DESC LIMIT 1", (guar['name'],))
                        new_guar_row = cursor.fetchone()
                        if not new_guar_row: raise Exception(f"Failed to create and retrieve guarantee '{guar['name']}'")
                        guar_id = new_guar_row.id
                    cursor.execute("INSERT INTO cri.crm.operation_guarantees (operation_id, guarantee_id) VALUES (?, ?)", (new_op_id, guar_id))

                today = datetime.now()
                start_date_iso = today.isoformat()
                end_date_iso = data['maturityDate']
                rules_to_add = [
                    {'name': 'Revisão Periódica', 'frequency': data['reviewFrequency'], 'desc': 'Revisão periódica da operação, conforme frequência definida.'},
                    {'name': 'Call de Acompanhamento', 'frequency': data['callFrequency'], 'desc': 'Call de acompanhamento com o cliente/gestor.'},
                    {'name': 'Análise de DFs & Dívida', 'frequency': data['dfFrequency'], 'desc': 'Análise dos demonstrativos financeiros e endividamento.'},
                ]
                if dm.get('news'):
                    rules_to_add.append({'name': 'Monitorar Notícias', 'frequency': 'Semanal', 'desc': 'Acompanhar notícias relacionadas à operação e ao mercado.'})

                for rule in rules_to_add:
                    cursor.execute(
                        "INSERT INTO cri.crm.task_rules (operation_id, name, frequency, start_date, end_date, description) VALUES (?, ?, ?, ?, ?, ?)",
                        (new_op_id, rule['name'], rule['frequency'], start_date_iso, end_date_iso, rule['desc'])
                    )
                
                cursor.execute(
                    "INSERT INTO cri.crm.rating_history (operation_id, date, rating_operation, rating_group, sentiment) VALUES (?, ?, ?, ?, ?)",
                    (new_op_id, start_date_iso, data['ratingOperation'], data['ratingGroup'], 'Neutro')
                )
            
            conn.commit()
            
            with conn.cursor() as cursor:
                new_operation_full = fetch_full_operation(cursor, new_op_id)
            
            return jsonify(new_operation_full), 201
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error in POST /api/operations: {e}")
            return jsonify({"error": str(e)}), 500
        finally:
            conn.close()

@app.route('/api/operations/<int:op_id>', methods=['PUT', 'DELETE'])
def manage_operation(op_id):
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
                
                db_event_ids = {row.id for row in cursor.execute("SELECT id FROM cri.crm.events WHERE operation_id = ?", (op_id,)).fetchall()}
                newly_inserted_event_ids_map = {}

                for event in data.get('events', []):
                    if event.get('id') not in db_event_ids:
                        cursor.execute(
                            "INSERT INTO cri.crm.events (operation_id, date, type, title, description, registered_by, next_steps, completed_task_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (op_id, event['date'], event['type'], event['title'], event['description'], event['registeredBy'], event['nextSteps'], event.get('completedTaskId'))
                        )
                        cursor.execute("SELECT id FROM cri.crm.events WHERE operation_id = ? ORDER BY id DESC LIMIT 1", (op_id,))
                        new_event_row = cursor.fetchone()
                        if new_event_row:
                            newly_inserted_event_ids_map[event.get('id')] = new_event_row.id

                db_rh_ids = {row.id for row in cursor.execute("SELECT id FROM cri.crm.rating_history WHERE operation_id = ?", (op_id,)).fetchall()}
                for rh_entry in data.get('ratingHistory', []):
                    if rh_entry.get('id') not in db_rh_ids:
                        client_event_id = rh_entry.get('eventId')
                        db_event_id = newly_inserted_event_ids_map.get(client_event_id, client_event_id)
                        cursor.execute(
                            "INSERT INTO cri.crm.rating_history (operation_id, event_id, date, rating_operation, rating_group, sentiment) VALUES (?, ?, ?, ?, ?, ?)",
                            (op_id, db_event_id, rh_entry['date'], rh_entry['ratingOperation'], rh_entry['ratingGroup'], rh_entry['sentiment'])
                        )

                db_rule_ids = {row.id for row in cursor.execute("SELECT id FROM cri.crm.task_rules WHERE operation_id = ?", (op_id,)).fetchall()}
                for rule in data.get('taskRules', []):
                    if rule.get('id') not in db_rule_ids:
                         cursor.execute(
                            "INSERT INTO cri.crm.task_rules (operation_id, name, frequency, start_date, end_date, description) VALUES (?, ?, ?, ?, ?, ?)",
                            (op_id, rule['name'], rule['frequency'], rule['startDate'], rule['endDate'], rule['description'])
                        )

            conn.commit()

            with conn.cursor() as cursor:
                updated_operation_full = fetch_full_operation(cursor, op_id)
            
            return jsonify(updated_operation_full)
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error in PUT /api/operations/{op_id}: {e}")
            return jsonify({"error": str(e)}), 500
        finally:
            conn.close()

    elif request.method == 'DELETE':
        try:
            with conn.cursor() as cursor:
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

# ================== Servidor de Frontend ==================
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve_react_app(path):
    """
    Serve a aplicação React.
    - Se o caminho corresponde a um arquivo existente (CSS, JS, etc.), serve esse arquivo.
    - Para qualquer outro caminho, serve o 'index.html' para que o React Router possa assumir.
    """
    if path != "" and os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    else:
        return send_from_directory(app.static_folder, 'index.html')
