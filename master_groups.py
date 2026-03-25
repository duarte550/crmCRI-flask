from flask import Blueprint, jsonify, request
from db import get_db_connection
from utils import safe_isoformat, parse_iso_date
from datetime import datetime
import logging
from task_engine import generate_tasks_for_operation
from collections import defaultdict

master_groups_bp = Blueprint('master_groups', __name__)

def format_row(row, cursor):
    return {desc[0]: value for desc, value in zip(cursor.description, row)}

def fetch_full_master_group(cursor, mg_id):
    cursor.execute("SELECT * FROM cri_cra_dev.crm.master_groups WHERE id = ?", (mg_id,))
    mg_row = cursor.fetchone()
    if not mg_row:
        return None
    
    mg = format_row(mg_row, cursor)
    
    # Fetch economic groups
    cursor.execute("SELECT * FROM cri_cra_dev.crm.economic_groups WHERE master_group_id = ?", (mg_id,))
    mg['economicGroups'] = [format_row(r, cursor) for r in cursor.fetchall()]
    
    # Fetch active/legacy operations
    cursor.execute("SELECT id, name, area, operation_type, status, rating_operation FROM cri_cra_dev.crm.operations WHERE master_group_id = ? AND (is_structuring IS NULL OR is_structuring = FALSE)", (mg_id,))
    mg['operations'] = [{
        'id': r.get('id'), 'name': r.get('name'), 'area': r.get('area'), 
        'operationType': r.get('operation_type'), 'status': r.get('status'),
        'ratingOperation': r.get('rating_operation')
    } for r in cursor.fetchall()]
    
    # Fetch structuring operations
    cursor.execute("SELECT id, name, pipeline_stage as stage, liquidation_date, responsible_analyst as analyst, risk, temperature, is_active FROM cri_cra_dev.crm.operations WHERE master_group_id = ? AND is_structuring = TRUE", (mg_id,))
    so_rows = [format_row(r, cursor) for r in cursor.fetchall()]
    mg['structuringOperations'] = []
    
    for so in so_rows:
        cursor.execute("SELECT * FROM cri_cra_dev.crm.operation_series WHERE operation_id = ?", (so['id'],))
        series_rows = [format_row(r, cursor) for r in cursor.fetchall()]
        mg['structuringOperations'].append({
            'id': so['id'],
            'name': so['name'],
            'stage': so['stage'],
            'liquidationDate': safe_isoformat(so.get('liquidation_date')),
            'risk': so.get('risk'),
            'temperature': so.get('temperature'),
            'analyst': so.get('analyst'),
            'isActive': so.get('is_active', True),
            'series': [{
                'id': s['id'],
                'name': s['name'],
                'rate': s.get('rate'),
                'indexer': s.get('indexer'),
                'volume': s.get('volume'),
                'fund': s.get('fund')
            } for s in series_rows]
        })
        

    # Fetch events
    cursor.execute("""
        SELECT e.*, o.name as operation_name
        FROM cri_cra_dev.crm.events e
        LEFT JOIN cri_cra_dev.crm.operations o ON e.operation_id = o.id
        WHERE e.master_group_id = ? 
           OR o.master_group_id = ?
        ORDER BY e.date DESC
    """, (mg_id, mg_id))
    events = [format_row(r, cursor) for r in cursor.fetchall()]
    mg['events'] = [{
        'id': e.get('id'), 'date': safe_isoformat(e.get('date')),
        'type': e.get('type'), 'title': e.get('title'), 'description': e.get('description'),
        'registeredBy': e.get('registered_by'), 'nextSteps': e.get('next_steps'),
        'isOrigination': e.get('is_origination') or False,
        'completedTaskId': e.get('completed_task_id'),
        'operationName': e.get('operation_name')
    } for e in events]
    
    # Fetch recent changes (audit logs from operations)
    cursor.execute("""
        SELECT a.*, o.name as operation_name 
        FROM cri_cra_dev.crm.audit_logs a
        JOIN cri_cra_dev.crm.operations o ON a.entity_id = CAST(o.id AS STRING) AND a.entity_type = 'Operation'
        WHERE o.master_group_id = ?
        ORDER BY a.timestamp DESC
        LIMIT 10
    """, (mg_id,))
    recent_changes = [format_row(r, cursor) for r in cursor.fetchall()]
    mg['recentChanges'] = [{
        'id': c.get('id'),
        'operationId': c.get('entity_id'),
        'operationName': c.get('operation_name'),
        'timestamp': safe_isoformat(c.get('timestamp')),
        'user': c.get('user_name'),
        'action': c.get('action'),
        'entity': c.get('entity_type'),
    } for c in recent_changes]
    
    # Fetch rating history for this master group
    cursor.execute("""
        SELECT *
        FROM cri_cra_dev.crm.rating_history
        WHERE master_group_id = ?
        ORDER BY date DESC
    """, (mg_id,))
    rating_history = [format_row(r, cursor) for r in cursor.fetchall()]
    mg['ratingHistory'] = [{
        'id': r.get('id'),
        'date': safe_isoformat(r.get('date')),
        'ratingOperation': r.get('rating_operation'),
        'ratingGroup': r.get('rating_group'),
        'watchlist': r.get('watchlist'),
        'sentiment': r.get('sentiment')
    } for r in rating_history]
    
    # Fetch risks for this master group
    cursor.execute("""
        SELECT *
        FROM cri_cra_dev.crm.operation_risks
        WHERE master_group_id = ?
        ORDER BY created_at DESC
    """, (mg_id,))
    mg_risks = [format_row(r, cursor) for r in cursor.fetchall()]
    mg['risks'] = [{
        'id': r.get('id'),
        'title': r.get('title'),
        'description': r.get('description'),
        'severity': r.get('severity'),
        'createdAt': safe_isoformat(r.get('created_at')),
        'updatedAt': safe_isoformat(r.get('updated_at')),
        'masterGroupId': r.get('master_group_id')
    } for r in mg_risks]
    
    return mg

@master_groups_bp.route('/api/master-groups', methods=['GET', 'POST'])
def manage_master_groups():
    conn = get_db_connection()
    try:
        if request.method == 'GET':
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM cri_cra_dev.crm.master_groups ORDER BY name")
                mgs = [format_row(row, cursor) for row in cursor.fetchall()]
                for mg in mgs:
                    cursor.execute("SELECT * FROM cri_cra_dev.crm.economic_groups WHERE master_group_id = ?", (mg['id'],))
                    mg['economicGroups'] = [format_row(r, cursor) for r in cursor.fetchall()]

                    cursor.execute("SELECT id, name, rating_operation FROM cri_cra_dev.crm.operations WHERE master_group_id = ? AND (is_structuring IS NULL OR is_structuring = FALSE)", (mg['id'],))
                    mg['operations'] = [{
                        'id': r.get('id'), 'name': r.get('name'), 'ratingOperation': r.get('rating_operation')
                    } for r in cursor.fetchall()]
                    
                    cursor.execute("SELECT id, name, pipeline_stage as stage FROM cri_cra_dev.crm.operations WHERE master_group_id = ? AND is_structuring = TRUE", (mg['id'],))
                    so_rows = [format_row(r, cursor) for r in cursor.fetchall()]
                    for so in so_rows:
                        cursor.execute("SELECT * FROM cri_cra_dev.crm.operation_series WHERE operation_id = ?", (so['id'],))
                        series_rows = [format_row(r, cursor) for r in cursor.fetchall()]
                        so['series'] = [{
                           'id': s['id'], 'name': s['name'], 'rate': s.get('rate'), 'indexer': s.get('indexer'), 'volume': s.get('volume'), 'fund': s.get('fund')
                        } for s in series_rows]
                    mg['structuringOperations'] = so_rows
                return jsonify(mgs)
        elif request.method == 'POST':
            data = request.json
            with conn.cursor() as cursor:
                cursor.execute("INSERT INTO cri_cra_dev.crm.master_groups (name, sector, rating) VALUES (?, ?, ?)", 
                               (data.get('name'), data.get('sector'), data.get('rating')))
                cursor.execute("SELECT id FROM cri_cra_dev.crm.master_groups ORDER BY id DESC LIMIT 1")
                new_id = cursor.fetchone().id
                conn.commit()
                return jsonify(fetch_full_master_group(cursor, new_id)), 201
    except Exception as e:
        logging.error(f"Error in /api/master-groups: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@master_groups_bp.route('/api/master-groups/<int:mg_id>', methods=['GET', 'PUT', 'DELETE'])
def manage_master_group(mg_id):
    conn = get_db_connection()
    try:
        if request.method == 'GET':
            with conn.cursor() as cursor:
                mg = fetch_full_master_group(cursor, mg_id)
                if not mg: return jsonify({"error": "Not found"}), 404
                return jsonify(mg)
        elif request.method == 'PUT':
            data = request.json
            with conn.cursor() as cursor:
                cursor.execute("UPDATE cri_cra_dev.crm.master_groups SET name=?, sector=?, rating=? WHERE id=?",
                               (data.get('name'), data.get('sector'), data.get('rating'), mg_id))
                
                if 'rating' in data:
                    cursor.execute("UPDATE cri_cra_dev.crm.operations SET rating_group=? WHERE master_group_id=? AND (is_structuring IS NULL OR is_structuring = FALSE)", (data.get('rating'), mg_id))
                
                conn.commit()
                return jsonify(fetch_full_master_group(cursor, mg_id)), 200
        elif request.method == 'DELETE':
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM cri_cra_dev.crm.master_groups WHERE id=?", (mg_id,))
                conn.commit()
                return '', 204
    except Exception as e:
        logging.error(f"Error in /api/master-groups/{mg_id}: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()


@master_groups_bp.route('/api/master-groups/<int:mg_id>/risks', methods=['POST'])
def add_mg_risk(mg_id):
    conn = get_db_connection()
    try:
        data = request.json
        with conn.cursor() as cursor:
            now = datetime.now()
            cursor.execute(
                "INSERT INTO cri_cra_dev.crm.operation_risks (master_group_id, title, description, severity, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (mg_id, data.get('title'), data.get('description'), data.get('severity'), now, now)
            )
            # Log action
            user_name = data.get('userName', 'Sistema')
            cursor.execute("INSERT INTO cri_cra_dev.crm.audit_logs (timestamp, user_name, action, entity_type, entity_id, details) VALUES (?, ?, ?, ?, ?, ?)",
                           (now, user_name, 'CREATE', 'MasterGroup', str(mg_id), f"Risco/Ponto de Atenção Adicionado: {data.get('title')}"))
            conn.commit()
            return jsonify(fetch_full_master_group(cursor, mg_id)), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@master_groups_bp.route('/api/master-groups/<int:mg_id>/risks/<int:risk_id>', methods=['PUT', 'DELETE'])
def manage_mg_risk(mg_id, risk_id):
    conn = get_db_connection()
    try:
        user_name = request.json.get('userName', 'Sistema') if request.json else request.args.get('userName', 'Sistema')
        now = datetime.now()
        if request.method == 'PUT':
            data = request.json
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE cri_cra_dev.crm.operation_risks SET title = ?, description = ?, severity = ?, updated_at = ? WHERE id = ? AND master_group_id = ?",
                    (data.get('title'), data.get('description'), data.get('severity'), now, risk_id, mg_id)
                )
                cursor.execute("INSERT INTO cri_cra_dev.crm.audit_logs (timestamp, user_name, action, entity_type, entity_id, details) VALUES (?, ?, ?, ?, ?, ?)",
                               (now, user_name, 'UPDATE', 'MasterGroup', str(mg_id), f"Risco Atualizado: {data.get('title')}"))
                conn.commit()
                return jsonify(fetch_full_master_group(cursor, mg_id)), 200
        elif request.method == 'DELETE':
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM cri_cra_dev.crm.operation_risks WHERE id = ? AND master_group_id = ?", (risk_id, mg_id))
                cursor.execute("INSERT INTO cri_cra_dev.crm.audit_logs (timestamp, user_name, action, entity_type, entity_id, details) VALUES (?, ?, ?, ?, ?, ?)",
                               (now, user_name, 'DELETE', 'MasterGroup', str(mg_id), f"Risco Removido ID={risk_id}"))
                conn.commit()
                return jsonify(fetch_full_master_group(cursor, mg_id)), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@master_groups_bp.route('/api/structuring-operations', methods=['POST'])
def add_structuring_operation():
    conn = get_db_connection()
    try:
        data = request.json
        with conn.cursor() as cursor:
            master_group_id = data.get('masterGroupId')
            economic_group_id = data.get('economicGroupId')
            
            if economic_group_id == 'new' and data.get('newEGName'):
                cursor.execute("INSERT INTO cri_cra_dev.crm.economic_groups (name, master_group_id, rating) VALUES (?, ?, ?)",
                               (data.get('newEGName'), master_group_id, data.get('risk')))
                cursor.execute("SELECT id FROM cri_cra_dev.crm.economic_groups ORDER BY id DESC LIMIT 1")
                economic_group_id = cursor.fetchone().id
            elif economic_group_id == '':
                economic_group_id = None
                
            cursor.execute("INSERT INTO cri_cra_dev.crm.operations (master_group_id, economic_group_id, name, area, pipeline_stage, liquidation_date, risk, temperature, responsible_analyst, is_active, originator, modality, created_at, is_structuring) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)",
                           (master_group_id, economic_group_id, data.get('name'), data.get('area'), data.get('stage', 'Conversa Inicial'), parse_iso_date(data.get('liquidationDate')), data.get('risk'), data.get('temperature'), data.get('analyst'), True, data.get('originator'), data.get('modality'), datetime.now()))
            cursor.execute("SELECT id FROM cri_cra_dev.crm.operations ORDER BY id DESC LIMIT 1")
            new_id = cursor.fetchone().id
            
            default_stages = ['Conversa Inicial', 'Term Sheet', 'Due Diligence', 'Aprovação', 'Liquidação']
            for idx, sn in enumerate(default_stages):
                cursor.execute("INSERT INTO cri_cra_dev.crm.operation_stages (operation_id, name, order_index, is_completed) VALUES (?, ?, ?, ?)", (new_id, sn, idx, False))
            
            series = data.get('series', [])
            for s in series:
                cursor.execute("INSERT INTO cri_cra_dev.crm.operation_series (operation_id, name, rate, indexer, volume, fund) VALUES (?, ?, ?, ?, ?, ?)",
                               (new_id, s.get('name', 'Série Única'), s.get('rate'), s.get('indexer'), s.get('volume'), s.get('fund')))
                
            conn.commit()
            return jsonify({"status": "success"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@master_groups_bp.route('/api/structuring-operations', methods=['GET'])
def get_structuring_operations():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT o.id, o.name, o.area, o.pipeline_stage as stage, o.liquidation_date, o.risk, o.temperature, o.responsible_analyst as analyst, o.is_active, o.originator, o.modality, o.created_at, mg.name as master_group_name FROM cri_cra_dev.crm.operations o JOIN cri_cra_dev.crm.master_groups mg ON o.master_group_id = mg.id WHERE o.is_structuring = TRUE")
            sos = [format_row(r, cursor) for r in cursor.fetchall()]
            
            for so in sos:
                so['liquidationDate'] = safe_isoformat(so.get('liquidation_date'))
                so['createdAt'] = safe_isoformat(so.get('created_at'))
                so['masterGroupName'] = so.get('master_group_name')
                so['isActive'] = True if so.get('is_active') is None else bool(so.get('is_active'))
                
                cursor.execute("SELECT * FROM cri_cra_dev.crm.operation_stages WHERE operation_id = ? ORDER BY order_index", (so['id'],))
                so['stages'] = [format_row(r, cursor) for r in cursor.fetchall()]
                
                cursor.execute("SELECT * FROM cri_cra_dev.crm.operation_series WHERE operation_id = ?", (so['id'],))
                series_rows = [format_row(r, cursor) for r in cursor.fetchall()]
                so['series'] = [{
                    'id': s['id'], 'name': s['name'], 'rate': s.get('rate'), 'indexer': s.get('indexer'), 'volume': s.get('volume'), 'fund': s.get('fund')
                } for s in series_rows]
                
                cursor.execute("SELECT * FROM cri_cra_dev.crm.events WHERE operation_id = ? ORDER BY date DESC LIMIT 3", (so['id'],))
                events = [format_row(r, cursor) for r in cursor.fetchall()]
                so['recentEvents'] = [{
                    'id': e.get('id'), 'date': safe_isoformat(e.get('date')),
                    'type': e.get('type'), 'title': e.get('title'), 'description': e.get('description'),
                    'registeredBy': e.get('registered_by'), 'nextSteps': e.get('next_steps'),
                    'completedTaskId': e.get('completed_task_id'),
                    'isOrigination': e.get('is_origination') or False,
                    'operationStageId': e.get('operation_stage_id'),
                    'structuringOperationStageId': e.get('operation_stage_id') # For frontend compatibility
                } for e in events]
                
                cursor.execute("SELECT * FROM cri_cra_dev.crm.task_rules WHERE operation_id = ?", (so['id'],))
                so['taskRules'] = [{
                    'id': r.get('id'), 'name': r.get('name'), 'frequency': r.get('frequency'),
                    'startDate': safe_isoformat(r.get('start_date')),
                    'endDate': safe_isoformat(r.get('end_date')),
                    'description': r.get('description'),
                    'priority': r.get('priority')
                } for r in [format_row(row, cursor) for row in cursor.fetchall()]]
                
                cursor.execute("SELECT task_id FROM cri_cra_dev.crm.task_exceptions WHERE operation_id = ?", (so['id'],))
                task_exceptions = {row.task_id for row in cursor.fetchall()}
                
                so['tasks'] = generate_tasks_for_operation(so, task_exceptions)
                
            return jsonify(sos)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@master_groups_bp.route('/api/structuring-operations/<int:so_id>', methods=['GET', 'PUT', 'DELETE'])
def manage_structuring_operation(so_id):
    conn = get_db_connection()
    try:
        if request.method == 'GET':
            with conn.cursor() as cursor:
                cursor.execute("SELECT o.id, o.master_group_id, o.name, o.area, o.pipeline_stage as stage, o.liquidation_date, o.risk, o.temperature, o.responsible_analyst as analyst, o.is_active, o.originator, o.modality, o.created_at, mg.name as master_group_name FROM cri_cra_dev.crm.operations o JOIN cri_cra_dev.crm.master_groups mg ON o.master_group_id = mg.id WHERE o.id = ? AND o.is_structuring = TRUE", (so_id,))
                so_row = cursor.fetchone()
                if not so_row:
                    return jsonify({"error": "Not found"}), 404
                    
                so = format_row(so_row, cursor)
                so['liquidationDate'] = safe_isoformat(so.get('liquidation_date'))
                so['createdAt'] = safe_isoformat(so.get('created_at'))
                so['masterGroupName'] = so.get('master_group_name')
                so['isActive'] = True if so.get('is_active') is None else bool(so.get('is_active'))
                
                cursor.execute("SELECT * FROM cri_cra_dev.crm.operation_stages WHERE operation_id = ? ORDER BY order_index", (so_id,))
                so['stages'] = [format_row(r, cursor) for r in cursor.fetchall()]
                
                cursor.execute("SELECT * FROM cri_cra_dev.crm.operation_series WHERE operation_id = ?", (so_id,))
                series_rows = [format_row(r, cursor) for r in cursor.fetchall()]
                so['series'] = [{
                    'id': s['id'], 'name': s['name'], 'rate': s.get('rate'), 'indexer': s.get('indexer'), 'volume': s.get('volume'), 'fund': s.get('fund')
                } for s in series_rows]
                
                cursor.execute("SELECT * FROM cri_cra_dev.crm.events WHERE operation_id = ? ORDER BY date DESC", (so_id,))
                events = [format_row(r, cursor) for r in cursor.fetchall()]
                so['events'] = [{
                    'id': e.get('id'), 'date': safe_isoformat(e.get('date')),
                    'type': e.get('type'), 'title': e.get('title'), 'description': e.get('description'),
                    'registeredBy': e.get('registered_by'), 'nextSteps': e.get('next_steps'),
                    'completedTaskId': e.get('completed_task_id'),
                    'isOrigination': e.get('is_origination') or False,
                    'operationStageId': e.get('operation_stage_id'),
                    'structuringOperationStageId': e.get('operation_stage_id')
                } for e in events]
                
                cursor.execute("SELECT * FROM cri_cra_dev.crm.task_rules WHERE operation_id = ?", (so_id,))
                so['taskRules'] = [{
                    'id': r.get('id'), 'name': r.get('name'), 'frequency': r.get('frequency'),
                    'startDate': safe_isoformat(r.get('start_date')),
                    'endDate': safe_isoformat(r.get('end_date')),
                    'description': r.get('description'),
                    'priority': r.get('priority'),
                    'operationStageId': r.get('operation_stage_id'),
                    'structuringOperationStageId': r.get('operation_stage_id')
                } for r in [format_row(row, cursor) for row in cursor.fetchall()]]
                
                cursor.execute("SELECT task_id FROM cri_cra_dev.crm.task_exceptions WHERE operation_id = ?", (so_id,)) 
                task_exceptions = {row.task_id for row in cursor.fetchall()}
                
                so['tasks'] = generate_tasks_for_operation(so, task_exceptions)
                
                cursor.execute("SELECT * FROM cri_cra_dev.crm.master_group_contacts WHERE master_group_id = ?", (so['master_group_id'],))
                so['contacts'] = [format_row(r, cursor) for r in cursor.fetchall()]
                
                return jsonify(so)
        elif request.method == 'PUT':
            data = request.json
            with conn.cursor() as cursor:
                user_name = data.get('userName', 'Sistema')
                cursor.execute("SELECT risk, temperature, responsible_analyst as analyst, area, master_group_id, economic_group_id FROM cri_cra_dev.crm.operations WHERE id=?", (so_id,))
                old_row = cursor.fetchone()
                old_op = format_row(old_row, cursor) if old_row else {}
                
                cursor.execute("SELECT SUM(volume) as total_vol FROM cri_cra_dev.crm.operation_series WHERE operation_id=?", (so_id,))
                old_vol_row = cursor.fetchone()
                old_vol = float(old_vol_row.total_vol) if old_vol_row and old_vol_row.total_vol else 0.0

                is_active_val = data.get('isActive')
                if is_active_val is None:
                    is_active_val = True

                # If area is omitted from frontend json, fetch the original or assume CRI
                new_area = data.get('area')
                if not new_area:
                    new_area = old_op.get('area') or 'CRI'

                # Update EconomicGroup rating optionally or create it
                
                new_economic_group_id = data.get('economicGroupId')
                if new_economic_group_id == 'new' and data.get('newEGName'):
                    cursor.execute("INSERT INTO cri_cra_dev.crm.economic_groups (name, master_group_id, rating) VALUES (?, ?, ?)",
                                   (data.get('newEGName'), old_op.get('master_group_id'), data.get('risk')))
                    cursor.execute("SELECT id FROM cri_cra_dev.crm.economic_groups ORDER BY id DESC LIMIT 1")
                    new_economic_group_id = cursor.fetchone().id
                elif new_economic_group_id == '':
                    new_economic_group_id = None
                else:
                    new_economic_group_id = old_op.get('economic_group_id') if new_economic_group_id is None else new_economic_group_id

                cursor.execute("UPDATE cri_cra_dev.crm.operations SET name=?, area=?, pipeline_stage=?, liquidation_date=?, risk=?, temperature=?, is_active=?, responsible_analyst=?, originator=?, modality=?, economic_group_id=? WHERE id=?",
                               (data.get('name'), new_area, data.get('stage'), parse_iso_date(data.get('liquidationDate')), data.get('risk'), data.get('temperature'), is_active_val, data.get('analyst'), data.get('originator'), data.get('modality'), new_economic_group_id, so_id))
                               
                cursor.execute("DELETE FROM cri_cra_dev.crm.operation_series WHERE operation_id=?", (so_id,))
                series = data.get('series', [])
                new_vol = 0.0
                for s in series:
                    vol = float(s.get('volume', 0.0) or 0.0)
                    new_vol += vol
                    cursor.execute("INSERT INTO cri_cra_dev.crm.operation_series (operation_id, name, rate, indexer, volume, fund) VALUES (?, ?, ?, ?, ?, ?)",
                                   (so_id, s.get('name', 'Série Única'), s.get('rate'), s.get('indexer'), vol, s.get('fund')))
                
                changes = []
                if old_op.get('risk') != data.get('risk'): changes.append(f"Risco: {old_op.get('risk') or 'N/A'} -> {data.get('risk') or 'N/A'}")
                if old_op.get('temperature') != data.get('temperature'): changes.append(f"Temperatura: {old_op.get('temperature') or 'N/A'} -> {data.get('temperature') or 'N/A'}")
                if old_op.get('analyst') != data.get('analyst'): changes.append(f"Analista: {old_op.get('analyst') or 'N/A'} -> {data.get('analyst') or 'N/A'}")
                if old_vol != new_vol: changes.append(f"Volume: R$ {old_vol} -> R$ {new_vol}")
                
                if changes:
                    msg = "Alterações detectadas: " + " | ".join(changes)
                    cursor.execute("INSERT INTO cri_cra_dev.crm.events (operation_id, date, type, title, description, is_origination, registered_by) VALUES (?, ?, 'Atualização Automática', 'Alteração de Atributos', ?, ?, ?)", 
                                   (so_id, datetime.now(), msg, True, user_name))
                                   
                if 'taskRules' in data:
                    cursor.execute("SELECT id, name FROM cri_cra_dev.crm.task_rules WHERE operation_id = ?", (so_id,))
                    db_rules_map = {row.id: row.name for row in cursor.fetchall()}
                    client_rule_ids = {r['id'] for r in data.get('taskRules', []) if 'id' in r and isinstance(r['id'], int)}

                    for rule_id_to_delete in set(db_rules_map.keys()) - client_rule_ids:
                        cursor.execute("DELETE FROM cri_cra_dev.crm.task_rules WHERE id = ?", (rule_id_to_delete,))

                    for rule in data.get('taskRules', []):
                        rule_id = rule.get('id')
                        if rule_id and rule_id in db_rules_map:
                            cursor.execute("UPDATE cri_cra_dev.crm.task_rules SET name=?, frequency=?, start_date=?, end_date=?, description=?, priority=? WHERE id=?", 
                                           (rule.get('name'), rule.get('frequency'), rule.get('startDate'), rule.get('endDate'), rule.get('description'), rule.get('priority') or 'Média', rule_id))
                        elif not rule_id or rule_id not in db_rules_map:
                            cursor.execute("INSERT INTO cri_cra_dev.crm.task_rules (operation_id, name, frequency, start_date, end_date, description, priority, is_origination) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
                                           (so_id, rule.get('name'), rule.get('frequency'), rule.get('startDate'), rule.get('endDate'), rule.get('description'), rule.get('priority') or 'Média', True))
                                           
                if 'taskExceptions' in data:
                    cursor.execute("DELETE FROM cri_cra_dev.crm.task_exceptions WHERE operation_id = ?", (so_id,))
                    for task_id in data['taskExceptions']:
                        cursor.execute("INSERT INTO cri_cra_dev.crm.task_exceptions (operation_id, task_id) VALUES (?, ?)", (so_id, task_id))
                                   
                conn.commit()
                return jsonify({"status": "success"}), 200
        elif request.method == 'DELETE':
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM cri_cra_dev.crm.operations WHERE id=?", (so_id,))
                conn.commit()
                return '', 204
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@master_groups_bp.route('/api/master-groups/<int:mg_id>/events', methods=['POST'])
def add_master_group_event(mg_id):
    conn = get_db_connection()
    try:
        data = request.json
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO cri_cra_dev.crm.events (master_group_id, date, type, title, description, registered_by, next_steps, is_origination) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                           (mg_id, parse_iso_date(data.get('date')), data.get('type'), data.get('title'), data.get('description'), data.get('registeredBy'), data.get('nextSteps'), data.get('isOrigination', False)))
            conn.commit()
            return jsonify({"status": "success"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@master_groups_bp.route('/api/structuring-operations/<int:so_id>/events', methods=['POST'])
def add_structuring_operation_event(so_id):
    conn = get_db_connection()
    try:
        data = request.json
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO cri_cra_dev.crm.events (operation_id, date, type, title, description, registered_by, next_steps, is_origination, operation_stage_id, completed_task_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                           (so_id, parse_iso_date(data.get('date')), data.get('type'), data.get('title'), data.get('description'), data.get('registeredBy'), data.get('nextSteps'), True, data.get('structuringOperationStageId'), data.get('completedTaskId')))
            cursor.execute("SELECT id FROM cri_cra_dev.crm.events WHERE operation_id = ? ORDER BY id DESC LIMIT 1", (so_id,))
            new_id = cursor.fetchone().id
            conn.commit()
            return jsonify({"status": "success", "id": new_id}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@master_groups_bp.route('/api/structuring-operations/<int:so_id>/events/<int:event_id>', methods=['PUT', 'DELETE'])
def manage_structuring_operation_event(so_id, event_id):
    conn = get_db_connection()
    try:
        if request.method == 'DELETE':
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM cri_cra_dev.crm.events WHERE id = ? AND operation_id = ?", (event_id, so_id))
                conn.commit()
            return '', 204
        elif request.method == 'PUT':
            data = request.json
            with conn.cursor() as cursor:
                cursor.execute("UPDATE cri_cra_dev.crm.events SET date=?, type=?, title=?, description=?, registered_by=?, next_steps=?, completed_task_id=?, attention_points=?, our_attendees=?, operation_attendees=?, operation_stage_id=? WHERE id=? AND operation_id = ?",
                               (parse_iso_date(data.get('date')), data.get('type'), data.get('title'), data.get('description'), data.get('registeredBy'), data.get('nextSteps'), data.get('completedTaskId'), data.get('attentionPoints'), data.get('ourAttendees'), data.get('operationAttendees'), data.get('structuringOperationStageId'), event_id, so_id))
                conn.commit()
            return jsonify({"status": "success"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@master_groups_bp.route('/api/structuring-operations/<int:so_id>/stages', methods=['PUT'])
def update_structuring_operation_stages(so_id):
    conn = get_db_connection()
    try:
        stages = request.json.get('stages', [])
        with conn.cursor() as cursor:
            # Pega estágios existentes no banco
            cursor.execute("SELECT id FROM cri_cra_dev.crm.operation_stages WHERE operation_id=?", (so_id,))
            db_stage_ids = {row.id for row in cursor.fetchall()}
            client_stage_ids = {s.get('id') for s in stages if s.get('id')}

            for s in stages:
                s_id = s.get('id')
                # Se tem ID e está no DB, atualiza
                if s_id and s_id in db_stage_ids:
                    cursor.execute("UPDATE cri_cra_dev.crm.operation_stages SET name=?, order_index=?, is_completed=? WHERE id=?",
                                   (s.get('name'), s.get('order_index', 0), s.get('isCompleted', False), s_id))
                else:
                    # Sem ID ou não está no DB, insere novo
                    cursor.execute("INSERT INTO cri_cra_dev.crm.operation_stages (operation_id, name, order_index, is_completed) VALUES (?, ?, ?, ?)",
                                   (so_id, s.get('name'), s.get('order_index', 0), s.get('isCompleted', False)))
            
            # Deletar estágios não enviados (se precisar)
            stages_to_delete = db_stage_ids - client_stage_ids
            for del_id in stages_to_delete:
                cursor.execute("DELETE FROM cri_cra_dev.crm.operation_stages WHERE id=?", (del_id,))
                
            conn.commit()
            return jsonify({"status": "success"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()
