from flask import Blueprint, jsonify, request
from db import get_db_connection
from utils import safe_isoformat, parse_iso_date
from datetime import datetime
import logging
from task_engine import generate_tasks_for_operation

economic_groups_bp = Blueprint('economic_groups', __name__)

def format_row(row, cursor):
    return {desc[0]: value for desc, value in zip(cursor.description, row)}

def fetch_full_economic_group(cursor, eg_id):
    cursor.execute("SELECT e.*, m.name as master_group_name FROM cri_cra_dev.crm.economic_groups e JOIN cri_cra_dev.crm.master_groups m ON e.master_group_id = m.id WHERE e.id = ?", (eg_id,))
    eg_row = cursor.fetchone()
    if not eg_row:
        return None
    
    eg = format_row(eg_row, cursor)
    eg['masterGroupName'] = eg.get('master_group_name')
    
    # Fetch active/legacy operations
    cursor.execute("SELECT id, name, area, operation_type, status FROM cri_cra_dev.crm.operations WHERE economic_group_id = ? AND (is_structuring IS NULL OR is_structuring = FALSE)", (eg_id,))
    eg['operations'] = [format_row(r, cursor) for r in cursor.fetchall()]
    
    # Fetch structuring operations
    cursor.execute("SELECT id, name, pipeline_stage as stage, liquidation_date, responsible_analyst as analyst, risk, temperature, is_active FROM cri_cra_dev.crm.operations WHERE economic_group_id = ? AND is_structuring = TRUE", (eg_id,))
    so_rows = [format_row(r, cursor) for r in cursor.fetchall()]
    eg['structuringOperations'] = []
    
    for so in so_rows:
        cursor.execute("SELECT * FROM cri_cra_dev.crm.operation_series WHERE operation_id = ?", (so['id'],))
        series_rows = [format_row(r, cursor) for r in cursor.fetchall()]
        eg['structuringOperations'].append({
            'id': so['id'],
            'name': so['name'],
            'stage': so['stage'],
            'liquidationDate': safe_isoformat(so.get('liquidation_date')),
            'risk': so.get('risk'),
            'temperature': so.get('temperature'),
            'analyst': so.get('analyst'),
            'isActive': so.get('is_active', True),
            'series': [{
                'id': s['id'], 'name': s['name'], 'rate': s.get('rate'), 'indexer': s.get('indexer'), 'volume': s.get('volume'), 'fund': s.get('fund')
            } for s in series_rows]
        })
        
    # Events
    cursor.execute("""
        SELECT e.*, o.name as operation_name
        FROM cri_cra_dev.crm.events e
        JOIN cri_cra_dev.crm.operations o ON e.operation_id = o.id
        WHERE o.economic_group_id = ?
        ORDER BY e.date DESC
    """, (eg_id,))
    events = [format_row(r, cursor) for r in cursor.fetchall()]
    eg['events'] = [{
        'id': e.get('id'), 'date': safe_isoformat(e.get('date')),
        'type': e.get('type'), 'title': e.get('title'), 'description': e.get('description'),
        'registeredBy': e.get('registered_by'), 'nextSteps': e.get('next_steps'),
        'isOrigination': e.get('is_origination') or False,
        'completedTaskId': e.get('completed_task_id'),
        'operationName': e.get('operation_name')
    } for e in events]
    
    # Recent changes
    cursor.execute("""
        SELECT a.*, o.name as operation_name 
        FROM cri_cra_dev.crm.audit_logs a
        JOIN cri_cra_dev.crm.operations o ON a.entity_id = CAST(o.id AS STRING) AND a.entity_type = 'Operation'
        WHERE o.economic_group_id = ?
        ORDER BY a.timestamp DESC
        LIMIT 10
    """, (eg_id,))
    recent_changes = [format_row(r, cursor) for r in cursor.fetchall()]
    eg['recentChanges'] = [{
        'id': c.get('id'), 'operationId': c.get('entity_id'), 'operationName': c.get('operation_name'),
        'timestamp': safe_isoformat(c.get('timestamp')), 'user': c.get('user_name'),
        'action': c.get('action'), 'entity': c.get('entity_type')
    } for c in recent_changes]
    
    # Rating history
    cursor.execute("""
        SELECT rh.*, o.name as operation_name
        FROM cri_cra_dev.crm.rating_history rh
        JOIN cri_cra_dev.crm.operations o ON rh.operation_id = o.id
        WHERE o.economic_group_id = ?
        ORDER BY rh.date DESC
    """, (eg_id,))
    rating_history = [format_row(r, cursor) for r in cursor.fetchall()]
    eg['ratingHistory'] = [{
        'id': r.get('id'), 'date': safe_isoformat(r.get('date')),
        'ratingOperation': r.get('rating_operation'), 'ratingGroup': r.get('rating_group'),
        'watchlist': r.get('watchlist'), 'sentiment': r.get('sentiment'), 'operationName': r.get('operation_name')
    } for r in rating_history]
    
    return eg

@economic_groups_bp.route('/api/economic-groups', methods=['GET', 'POST'])
def manage_economic_groups():
    conn = get_db_connection()
    try:
        if request.method == 'GET':
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT e.*, m.name as master_group_name 
                    FROM cri_cra_dev.crm.economic_groups e 
                    LEFT JOIN cri_cra_dev.crm.master_groups m ON e.master_group_id = m.id 
                    ORDER BY e.name
                """)
                egs = [format_row(row, cursor) for row in cursor.fetchall()]
                return jsonify(egs)
        elif request.method == 'POST':
            data = request.json
            with conn.cursor() as cursor:
                cursor.execute("INSERT INTO cri_cra_dev.crm.economic_groups (master_group_id, name, sector, rating, created_at) VALUES (?, ?, ?, ?, ?)", 
                               (data.get('masterGroupId'), data.get('name'), data.get('sector'), data.get('rating'), datetime.now()))
                cursor.execute("SELECT id FROM cri_cra_dev.crm.economic_groups ORDER BY id DESC LIMIT 1")
                new_id = cursor.fetchone().id
                conn.commit()
                return jsonify(fetch_full_economic_group(cursor, new_id)), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@economic_groups_bp.route('/api/economic-groups/<int:eg_id>', methods=['GET', 'PUT', 'DELETE'])
def manage_economic_group(eg_id):
    conn = get_db_connection()
    try:
        if request.method == 'GET':
            with conn.cursor() as cursor:
                eg = fetch_full_economic_group(cursor, eg_id)
                if not eg: return jsonify({"error": "Not found"}), 404
                return jsonify(eg)
        elif request.method == 'PUT':
            data = request.json
            with conn.cursor() as cursor:
                cursor.execute("UPDATE cri_cra_dev.crm.economic_groups SET name=?, sector=?, rating=?, master_group_id=? WHERE id=?",
                               (data.get('name'), data.get('sector'), data.get('rating'), data.get('masterGroupId'), eg_id))
                
                if 'rating' in data:
                    cursor.execute("UPDATE cri_cra_dev.crm.operations SET rating_group=? WHERE economic_group_id=? AND (is_structuring IS NULL OR is_structuring = FALSE)", (data.get('rating'), eg_id))
                
                conn.commit()
                return jsonify(fetch_full_economic_group(cursor, eg_id)), 200
        elif request.method == 'DELETE':
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM cri_cra_dev.crm.economic_groups WHERE id=?", (eg_id,))
                conn.commit()
                return '', 204
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

