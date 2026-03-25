
from datetime import datetime, date

def safe_isoformat(val):
    """
    Safely converts a value to ISO format string.
    If it's already a string, returns it.
    If it has an isoformat method (datetime/date), calls it.
    Otherwise returns None or string representation.
    """
    if val is None:
        return None
    if hasattr(val, 'isoformat'):
        return val.isoformat()
    return str(val)

def parse_iso_date(val):
    """
    Safely parses an ISO date string into a datetime object.
    If it's already a datetime/date object, returns it.
    """
    if not val:
        return None
    if isinstance(val, (datetime, date)):
        return val
    try:
        # Handle both "YYYY-MM-DD" and "YYYY-MM-DDTHH:MM:SS"
        if 'T' in val:
            return datetime.fromisoformat(val.replace('Z', ''))
        else:
            return datetime.strptime(val, '%Y-%m-%d')
    except Exception:
        return val # Fallback to string if parsing fails

def format_row(row, cursor):
    """ Converts a cursor row into a dict using cursor.description """
    return {desc[0]: value for desc, value in zip(cursor.description, row)}
