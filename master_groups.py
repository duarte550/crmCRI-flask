from flask import Blueprint, jsonify, request
from db import get_db_connection
from utils import safe_isoformat, parse_iso_date
from datetime import datetime
import logging

master_groups_bp = Blueprint('master_groups', __name__)

def format_row(row, cursor):
    return {desc[0]: value for desc, value in zip(cursor.description, row)}

def fetch_full_master_group(cursor, mg_id):
    cursor.execute("SELECT * FROM cri_cra_dev.crm.master_groups WHERE id = ?", (mg_id,))
    mg_row = cursor.fetchone()
    if not mg_row:
        return None
    
    mg = format_row(mg_row, cursor)
    
    # Fetch operations
    cursor.execute("SELECT id, name, area, operation_type, status FROM cri_cra_dev.crm.operations WHERE master_group_id = ?", (mg_id,))
    mg['operations'] = [format_row(r, cursor) for r in cursor.fetchall()]
    
    # Fetch structuring operations
    cursor.execute("SELECT * FROM cri_cra_dev.crm.structuring_operations WHERE master_group_id = ?", (mg_id,))
    so_rows = [format_row(r, cursor) for r in cursor.fetchall()]
    mg['structuringOperations'] = []
    for so in so_rows:
        cursor.execute("SELECT * FROM cri_cra_dev.crm.structuring_operation_series WHERE structuring_operation_id = ?", (so['id'],))
        series_rows = [format_row(r, cursor) for r in cursor.fetchall()]
        mg['structuringOperations'].append({
            'id': so['id'],
            'name': so['name'],
            'stage': so['stage'],
            'liquidationDate': safe_isoformat(so.get('liquidation_date')),
            'series': [{
                'id': s['id'],
                'name': s['name'],
                'rate': s.get('rate'),
                'indexer': s.get('indexer'),
                'volume': s.get('volume'),
                'fund': s.get('fund')
            } for s in series_rows]
        })
        
    # Fetch contacts
    cursor.execute("SELECT * FROM cri_cra_dev.crm.master_group_contacts WHERE master_group_id = ?", (mg_id,))
    mg['contacts'] = [format_row(r, cursor) for r in cursor.fetchall()]
    
    # Fetch events (for master group, its operations, and its structuring operations)
    cursor.execute("""
        SELECT e.*, 
               o.name as operation_name,
               so.name as structuring_operation_name
        FROM cri_cra_dev.crm.events e
        LEFT JOIN cri_cra_dev.crm.operations o ON e.operation_id = o.id
        LEFT JOIN cri_cra_dev.crm.structuring_operations so ON e.structuring_operation_id = so.id
        WHERE e.master_group_id = ? 
           OR o.master_group_id = ?
           OR so.master_group_id = ?
        ORDER BY e.date DESC
    """, (mg_id, mg_id, mg_id))
    events = [format_row(r, cursor) for r in cursor.fetchall()]
    mg['events'] = [{
        'id': e.get('id'), 'date': safe_isoformat(e.get('date')),
        'type': e.get('type'), 'title': e.get('title'), 'description': e.get('description'),
        'registeredBy': e.get('registered_by'), 'nextSteps': e.get('next_steps'),
        'isOrigination': e.get('is_origination') or False,
        'operationName': e.get('operation_name') or e.get('structuring_operation_name')
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
        'details': c.get('details')
    } for c in recent_changes]
    
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
                    cursor.execute("SELECT id, name FROM cri_cra_dev.crm.operations WHERE master_group_id = ?", (mg['id'],))
                    mg['operations'] = [format_row(r, cursor) for r in cursor.fetchall()]
                    cursor.execute("SELECT id, name, stage FROM cri_cra_dev.crm.structuring_operations WHERE master_group_id = ?", (mg['id'],))
                    so_rows = [format_row(r, cursor) for r in cursor.fetchall()]
                    for so in so_rows:
                        cursor.execute("SELECT * FROM cri_cra_dev.crm.structuring_operation_series WHERE structuring_operation_id = ?", (so['id'],))
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
                
                # Update operations rating_group if rating changed
                if 'rating' in data:
                    cursor.execute("UPDATE cri_cra_dev.crm.operations SET rating_group=? WHERE master_group_id=?", (data.get('rating'), mg_id))
                
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

@master_groups_bp.route('/api/master-groups/<int:mg_id>/contacts', methods=['POST'])
def add_contact(mg_id):
    conn = get_db_connection()
    try:
        data = request.json
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO cri_cra_dev.crm.master_group_contacts (master_group_id, name, email, phone, role) VALUES (?, ?, ?, ?, ?)",
                           (mg_id, data.get('name'), data.get('email'), data.get('phone'), data.get('role')))
            conn.commit()
            return jsonify({"status": "success"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@master_groups_bp.route('/api/master-groups/<int:mg_id>/contacts/<int:contact_id>', methods=['PUT', 'DELETE'])
def manage_contact(mg_id, contact_id):
    conn = get_db_connection()
    try:
        if request.method == 'PUT':
            data = request.json
            with conn.cursor() as cursor:
                cursor.execute("UPDATE cri_cra_dev.crm.master_group_contacts SET name=?, email=?, phone=?, role=? WHERE id=? AND master_group_id=?",
                               (data.get('name'), data.get('email'), data.get('phone'), data.get('role'), contact_id, mg_id))
                conn.commit()
                return jsonify({"status": "success"}), 200
        elif request.method == 'DELETE':
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM cri_cra_dev.crm.master_group_contacts WHERE id=? AND master_group_id=?", (contact_id, mg_id))
                conn.commit()
                return '', 204
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@master_groups_bp.route('/api/master-groups/<int:mg_id>/structuring-operations', methods=['POST'])
def add_structuring_operation(mg_id):
    conn = get_db_connection()
    try:
        data = request.json
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO cri_cra_dev.crm.structuring_operations (master_group_id, name, stage, liquidation_date) VALUES (?, ?, ?, ?)",
                           (mg_id, data.get('name'), data.get('stage', 'conversa inicial'), parse_iso_date(data.get('liquidationDate'))))
            cursor.execute("SELECT id FROM cri_cra_dev.crm.structuring_operations ORDER BY id DESC LIMIT 1")
            new_id = cursor.fetchone().id
            
            series = data.get('series', [])
            for s in series:
                cursor.execute("INSERT INTO cri_cra_dev.crm.structuring_operation_series (structuring_operation_id, name, rate, indexer, volume, fund) VALUES (?, ?, ?, ?, ?, ?)",
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
            cursor.execute("SELECT so.*, mg.name as master_group_name FROM cri_cra_dev.crm.structuring_operations so JOIN cri_cra_dev.crm.master_groups mg ON so.master_group_id = mg.id")
            sos = [format_row(r, cursor) for r in cursor.fetchall()]
            
            for so in sos:
                so['liquidationDate'] = safe_isoformat(so.get('liquidation_date'))
                so['masterGroupName'] = so.get('master_group_name')
                
                cursor.execute("SELECT * FROM cri_cra_dev.crm.structuring_operation_series WHERE structuring_operation_id = ?", (so['id'],))
                series_rows = [format_row(r, cursor) for r in cursor.fetchall()]
                so['series'] = [{
                    'id': s['id'], 'name': s['name'], 'rate': s.get('rate'), 'indexer': s.get('indexer'), 'volume': s.get('volume'), 'fund': s.get('fund')
                } for s in series_rows]
                
                # Fetch recent events for this structuring operation
                cursor.execute("SELECT * FROM cri_cra_dev.crm.events WHERE structuring_operation_id = ? ORDER BY date DESC LIMIT 3", (so['id'],))
                events = [format_row(r, cursor) for r in cursor.fetchall()]
                so['recentEvents'] = [{
                    'id': e.get('id'), 'date': safe_isoformat(e.get('date')),
                    'type': e.get('type'), 'title': e.get('title'), 'description': e.get('description'),
                    'registeredBy': e.get('registered_by'), 'nextSteps': e.get('next_steps'),
                    'isOrigination': e.get('is_origination') or False
                } for e in events]
                
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
                cursor.execute("SELECT so.*, mg.name as master_group_name FROM cri_cra_dev.crm.structuring_operations so JOIN cri_cra_dev.crm.master_groups mg ON so.master_group_id = mg.id WHERE so.id = ?", (so_id,))
                so_row = cursor.fetchone()
                if not so_row:
                    return jsonify({"error": "Not found"}), 404
                    
                so = format_row(so_row, cursor)
                so['liquidationDate'] = safe_isoformat(so.get('liquidation_date'))
                so['masterGroupName'] = so.get('master_group_name')
                
                cursor.execute("SELECT * FROM cri_cra_dev.crm.structuring_operation_series WHERE structuring_operation_id = ?", (so_id,))
                series_rows = [format_row(r, cursor) for r in cursor.fetchall()]
                so['series'] = [{
                    'id': s['id'], 'name': s['name'], 'rate': s.get('rate'), 'indexer': s.get('indexer'), 'volume': s.get('volume'), 'fund': s.get('fund')
                } for s in series_rows]
                
                cursor.execute("SELECT * FROM cri_cra_dev.crm.events WHERE structuring_operation_id = ? ORDER BY date DESC", (so_id,))
                events = [format_row(r, cursor) for r in cursor.fetchall()]
                so['events'] = [{
                    'id': e.get('id'), 'date': safe_isoformat(e.get('date')),
                    'type': e.get('type'), 'title': e.get('title'), 'description': e.get('description'),
                    'registeredBy': e.get('registered_by'), 'nextSteps': e.get('next_steps'),
                    'isOrigination': e.get('is_origination') or False
                } for e in events]
                
                # Fetch contacts from master group
                cursor.execute("SELECT * FROM cri_cra_dev.crm.master_group_contacts WHERE master_group_id = ?", (so['master_group_id'],))
                so['contacts'] = [format_row(r, cursor) for r in cursor.fetchall()]
                
                return jsonify(so)
        elif request.method == 'PUT':
            data = request.json
            with conn.cursor() as cursor:
                cursor.execute("UPDATE cri_cra_dev.crm.structuring_operations SET name=?, stage=?, liquidation_date=? WHERE id=?",
                               (data.get('name'), data.get('stage'), parse_iso_date(data.get('liquidationDate')), so_id))
                               
                cursor.execute("DELETE FROM cri_cra_dev.crm.structuring_operation_series WHERE structuring_operation_id=?", (so_id,))
                series = data.get('series', [])
                for s in series:
                    cursor.execute("INSERT INTO cri_cra_dev.crm.structuring_operation_series (structuring_operation_id, name, rate, indexer, volume, fund) VALUES (?, ?, ?, ?, ?, ?)",
                                   (so_id, s.get('name', 'Série Única'), s.get('rate'), s.get('indexer'), s.get('volume'), s.get('fund')))
                                   
                conn.commit()
                return jsonify({"status": "success"}), 200
        elif request.method == 'DELETE':
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM cri_cra_dev.crm.structuring_operations WHERE id=?", (so_id,))
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
            cursor.execute("INSERT INTO cri_cra_dev.crm.events (structuring_operation_id, date, type, title, description, registered_by, next_steps, is_origination) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                           (so_id, parse_iso_date(data.get('date')), data.get('type'), data.get('title'), data.get('description'), data.get('registeredBy'), data.get('nextSteps'), True))
            conn.commit()
            return jsonify({"status": "success"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()
