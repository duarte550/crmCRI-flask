
from datetime import date, timedelta, datetime
import calendar

def get_next_date(current_date, frequency):
    """
    Calculates the next due date based on a given frequency, correctly handling month-end variations.
    """
    if frequency == 'Diário':
        return current_date + timedelta(days=1)
    elif frequency == 'Semanal':
        return current_date + timedelta(days=7)
    elif frequency == 'Quinzenal':
        return current_date + timedelta(days=15)

    # For month-based frequencies, handle month-end variations carefully.
    month_offset = 0
    year_offset = 0

    if frequency == 'Mensal':
        month_offset = 1
    elif frequency == 'Trimestral':
        month_offset = 3
    elif frequency == 'Semestral':
        month_offset = 6
    elif frequency == 'Anual':
        year_offset = 1

    # This logic applies to all month/year-based frequencies
    if month_offset > 0 or year_offset > 0:
        # Calculate target year and month
        new_month_raw = current_date.month + month_offset
        new_year = current_date.year + year_offset + (new_month_raw - 1) // 12
        new_month = (new_month_raw - 1) % 12 + 1

        # Find the last day of the target month
        last_day_of_target_month = calendar.monthrange(new_year, new_month)[1]

        # Use the original day if it's valid, otherwise use the last day of the month
        new_day = min(current_date.day, last_day_of_target_month)

        return date(new_year, new_month, new_day)
    
    # Fallback for any other frequency type (shouldn't happen with current data)
    return current_date

def generate_tasks_for_rule(operation, rule, task_exceptions):
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

        if task_id in task_exceptions:
            return []
        
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
    start_date_obj = datetime.fromisoformat(rule['startDate']).date()
    # FIX: The first due date is one frequency period AFTER the start date.
    current_date = get_next_date(start_date_obj, rule['frequency'])
    end_date = datetime.fromisoformat(rule['endDate']).date()

    while current_date <= end_date:
        due_date = current_date
        task_id = f"op{operation['id']}-rule{rule['id']}-{due_date.isoformat()}"
        
        if task_id not in task_exceptions:
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

def generate_tasks_for_operation(operation, task_exceptions):
    """ Generates all task instances for all rules within an operation. """
    all_tasks = []
    for rule in operation.get('taskRules', []):
        all_tasks.extend(generate_tasks_for_rule(operation, rule, task_exceptions))
    return all_tasks
