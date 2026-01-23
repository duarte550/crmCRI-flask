
from datetime import date, timedelta, datetime

def get_next_date(current_date, frequency):
    """
    Calculates the next due date based on a given frequency.
    Note: This is a simplified implementation for monthly/yearly steps.
    """
    next_d = current_date
    if frequency == 'Diário':
        next_d += timedelta(days=1)
    elif frequency == 'Semanal':
        next_d += timedelta(days=7)
    elif frequency == 'Quinzenal':
        next_d += timedelta(days=15)
    elif frequency == 'Mensal':
        # A more robust library like dateutil.relativedelta is better for edge cases
        try:
            next_d = next_d.replace(month=next_d.month + 1)
        except ValueError: # Handles months with different number of days
            if next_d.month == 12:
                next_d = next_d.replace(year=next_d.year + 1, month=1)
            else:
                 # Go to the first day of the month after next, then subtract one day
                next_d = next_d.replace(month=next_d.month + 2, day=1) - timedelta(days=1)

    elif frequency == 'Trimestral':
        try:
            next_d = next_d.replace(month=next_d.month + 3)
        except ValueError:
             # Logic to handle jumping over year-end and different month lengths
            new_month = next_d.month + 3
            new_year = next_d.year
            if new_month > 12:
                new_year += 1
                new_month -= 12
            next_d = next_d.replace(year=new_year, month=new_month)

    elif frequency == 'Semestral':
        try:
            next_d = next_d.replace(month=next_d.month + 6)
        except ValueError:
            new_month = next_d.month + 6
            new_year = next_d.year
            if new_month > 12:
                new_year += 1
                new_month -= 12
            next_d = next_d.replace(year=new_year, month=new_month)

    elif frequency == 'Anual':
        next_d = next_d.replace(year=next_d.year + 1)
    return next_d

def generate_tasks_for_rule(operation, rule):
    """ Generates all task instances for a single rule. """
    tasks = []
    # Use a set for efficient lookup
    completed_task_ids = {e.get('completedTaskId') for e in operation.get('events', []) if e.get('completedTaskId')}
    
    today = date.today()

    # Skip if rule has no dates
    if not rule.get('startDate') or not rule.get('endDate'):
        return []

    # Handle one-off 'Pontual' tasks
    if rule['frequency'] == 'Pontual':
        due_date = datetime.fromisoformat(rule['startDate']).date()
        task_id = f"op{operation['id']}-rule{rule['id']}-{due_date.isoformat()}"
        
        status = 'Concluída' if task_id in completed_task_ids else 'Atrasada' if due_date < today else 'Pendente'
        
        tasks.append({
            'id': task_id,
            'operationId': operation['id'],
            'ruleId': rule['id'],
            'ruleName': rule['name'],
            'dueDate': due_date.isoformat() + "T00:00:00",
            'status': status,
        })
        return tasks

    # Handle recurring tasks
    current_date = datetime.fromisoformat(rule['startDate']).date()
    end_date = datetime.fromisoformat(rule['endDate']).date()

    while current_date <= end_date:
        due_date = current_date
        task_id = f"op{operation['id']}-rule{rule['id']}-{due_date.isoformat()}"
        
        status = 'Concluída' if task_id in completed_task_ids else 'Atrasada' if due_date < today else 'Pendente'

        tasks.append({
            'id': task_id,
            'operationId': operation['id'],
            'ruleId': rule['id'],
            'ruleName': rule['name'],
            'dueDate': due_date.isoformat() + "T00:00:00",
            'status': status,
        })
        current_date = get_next_date(current_date, rule['frequency'])
    
    return tasks

def generate_tasks_for_operation(operation):
    """ Generates all task instances for all rules within an operation. """
    all_tasks = []
    for rule in operation.get('taskRules', []):
        all_tasks.extend(generate_tasks_for_rule(operation, rule))
    return all_tasks
