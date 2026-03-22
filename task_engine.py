
from datetime import date, timedelta, datetime
import calendar
from utils import safe_isoformat, parse_iso_date

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
    # If frequency is unknown, default to Monthly to avoid infinite loops, or raise error.
    # Here we default to Monthly + 1 month to be safe.
    # return current_date # DANGEROUS: Causes infinite loop if frequency is unknown
    
    # Default to 30 days if unknown to prevent infinite loop
    return current_date + timedelta(days=30)

def generate_tasks_for_rule(operation, rule, task_exceptions):
    """ Generates all task instances for a single rule. """
    tasks = []
    # Use a set for efficient lookup
    completed_task_ids = {e.get('completedTaskId') for e in operation.get('events', []) if e.get('completedTaskId')}
    
    today = date.today()

    # Skip if rule has no dates (except for 'Sem Prazo')
    if rule['frequency'] != 'Sem Prazo' and (not rule.get('startDate') or not rule.get('endDate')):
        return []

    # Detect if this is an operation or a structuring operation
    is_struct_op = 'liquidationDate' in operation or 'structuringOperationId' in rule
    op_id = operation['id']
    id_prefix = f"sop{op_id}" if is_struct_op else f"op{op_id}"
    
    # Handle 'Sem Prazo' tasks
    if rule['frequency'] == 'Sem Prazo':
        task_id = f"{id_prefix}-rule{rule['id']}-nodate"
        if task_id in task_exceptions:
            return []
        
        status = 'Concluída' if task_id in completed_task_ids else 'Pendente'
        
        task_obj = {
            'id': task_id,
            'ruleId': rule['id'],
            'ruleName': rule['name'],
            'dueDate': None,
            'status': status,
            'priority': rule.get('priority') or 'Média',
            'notes': rule.get('description')
        }
        if is_struct_op:
            task_obj['structuringOperationId'] = op_id
        else:
            task_obj['operationId'] = op_id
            
        tasks.append(task_obj)
        return tasks

    # Handle one-off 'Pontual' tasks
    if rule['frequency'] == 'Pontual':
        due_date = parse_iso_date(rule['startDate'])
        if hasattr(due_date, 'date'):
            due_date = due_date.date()
        task_id = f"{id_prefix}-rule{rule['id']}-{safe_isoformat(due_date)}"

        if task_id in task_exceptions:
            return []
        
        status = 'Concluída' if task_id in completed_task_ids else 'Atrasada' if due_date and due_date < today else 'Pendente'
        
        task_obj = {
            'id': task_id,
            'ruleId': rule['id'],
            'ruleName': rule['name'],
            'dueDate': safe_isoformat(due_date) + "T00:00:00" if due_date else None,
            'status': status,
            'priority': rule.get('priority') or 'Média',
            'notes': rule.get('description')
        }
        if is_struct_op:
            task_obj['structuringOperationId'] = op_id
        else:
            task_obj['operationId'] = op_id
            
        tasks.append(task_obj)
        return tasks

    # Handle recurring tasks
    start_date_obj = parse_iso_date(rule['startDate'])
    if hasattr(start_date_obj, 'date'):
        start_date_obj = start_date_obj.date()
    # FIX: The first due date is one frequency period AFTER the start date.
    current_date = get_next_date(start_date_obj, rule['frequency'])
    end_date = parse_iso_date(rule['endDate'])
    if hasattr(end_date, 'date'):
        end_date = end_date.date()

    # Safety counter to prevent infinite loops even if logic fails
    max_iterations = 1000 
    iteration_count = 0

    while current_date <= end_date:
        iteration_count += 1
        if iteration_count > max_iterations:
            # Break loop to prevent crash, maybe log warning if possible
            break

        due_date = current_date
        task_id = f"{id_prefix}-rule{rule['id']}-{safe_isoformat(due_date)}"
        
        if task_id not in task_exceptions:
            status = 'Concluída' if task_id in completed_task_ids else 'Atrasada' if due_date < today else 'Pendente'

            task_obj = {
                'id': task_id,
                'ruleId': rule['id'],
                'ruleName': rule['name'],
                'dueDate': safe_isoformat(due_date) + "T00:00:00" if due_date else None,
                'status': status,
                'priority': rule.get('priority') or 'Média',
                'notes': rule.get('description')
            }
            if is_struct_op:
                task_obj['structuringOperationId'] = op_id
            else:
                task_obj['operationId'] = op_id
            
            tasks.append(task_obj)
        
        next_date = get_next_date(current_date, rule['frequency'])
        
        # Double check: if next_date is not greater than current_date, force break
        if next_date <= current_date:
             break
             
        current_date = next_date
    
    return tasks

def generate_tasks_for_operation(operation, task_exceptions):
    """ Generates all task instances for all rules within an operation. """
    all_tasks = []
    for rule in operation.get('taskRules', []):
        all_tasks.extend(generate_tasks_for_rule(operation, rule, task_exceptions))
    return all_tasks
