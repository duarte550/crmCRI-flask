
import pytest
from backend.app import app as flask_app
import json
from unittest.mock import MagicMock, ANY

# Este é um "fixture" do pytest. Ele configura um cliente de teste para nossa aplicação Flask.
# O escopo 'module' significa que ele será executado uma vez por arquivo de teste.
@pytest.fixture(scope='module')
def test_client():
    flask_app.config.update({
        "TESTING": True,
    })
    # Contexto da aplicação para que o logger funcione
    with flask_app.app_context():
        with flask_app.test_client() as testing_client:
            yield testing_client

# --- Mocks para simular o banco de dados ---

# Mock para a linha do banco de dados, para que possamos acessá-la como um objeto (ex: row.operation_id)
class MockRow:
    def __init__(self, data):
        self.__dict__.update(data)
    
    # Adicionando um método get para compatibilidade com `row.get('id')`
    def get(self, key, default=None):
        return self.__dict__.get(key, default)


# Mock da descrição do cursor para a função format_row
MOCK_CURSOR_DESC = [('id',), ('name',), ('operation_type',), ('maturity_date',), ('responsible_analyst',), ('review_frequency',), ('call_frequency',), ('df_frequency',), ('segmento',), ('rating_operation',), ('rating_group',), ('watchlist',), ('ltv',), ('dscr',), ('monitoring_news',), ('monitoring_fii_report',), ('monitoring_operational_info',), ('monitoring_receivables_portfolio',), ('monitoring_construction_report',), ('monitoring_commercial_info',), ('monitoring_spe_dfs',)]

MOCK_OPERATIONS_DATA = [
    {
        'id': 1, 'name': 'Mock Operation 1', 'operation_type': 'CRI', 'maturity_date': '2030-01-01T00:00:00',
        'responsible_analyst': 'Analyst A', 'review_frequency': 'Mensal', 'call_frequency': 'Semanal',
        'df_frequency': 'Trimestral', 'segmento': 'Infra', 'rating_operation': 'Baa1', 'rating_group': 'A4',
        'watchlist': 'Verde', 'ltv': 0.5, 'dscr': 1.2, 'monitoring_news': True, 'monitoring_fii_report': False,
        'monitoring_operational_info': True, 'monitoring_receivables_portfolio': False, 'monitoring_construction_report': True,
        'monitoring_commercial_info': False, 'monitoring_spe_dfs': True
    }
]

# --- Testes da API ---

def test_get_operations_success(test_client, mocker):
    """
    Testa se o endpoint GET /api/operations funciona corretamente em um cenário de sucesso.
    """
    # 1. Preparação (Mocking)
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    
    mock_cursor.fetchall.side_effect = [
        [MockRow(op) for op in MOCK_OPERATIONS_DATA],
        [MockRow({'operation_id': 1, 'id': 101, 'name': 'Project Alpha'})],
        [], [], [], [] # guarantees, events, task_rules, rating_history
    ]
    mock_cursor.description = MOCK_CURSOR_DESC
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mocker.patch('backend.app.get_db_connection', return_value=mock_conn)
    mocker.patch('backend.app.generate_tasks_for_operation', return_value=[{'id': 'task-1', 'status': 'Atrasada'}])

    # 2. Ação
    response = test_client.get('/api/operations')

    # 3. Verificação (Assertions)
    assert response.status_code == 200
    data = json.loads(response.data)
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]['name'] == 'Mock Operation 1'
    assert data[0]['overdueCount'] == 1
    assert data[0]['projects'][0]['name'] == 'Project Alpha'

def test_post_operation_success(test_client, mocker):
    """ Testa a criação de uma nova operação via POST /api/operations """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.side_effect = [
        MockRow({'id': 99}), None, MockRow({'id': 101}),
    ]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mocker.patch('backend.app.get_db_connection', return_value=mock_conn)
    mocker.patch('backend.app.fetch_full_operation', return_value={'id': 99, 'name': 'New Test Op'})
    
    new_op_data = {
      "name": "New Test Op", "operationType": "CRI", "maturityDate": "2035-12-31T00:00:00.000Z",
      "responsibleAnalyst": "Tester", "reviewFrequency": "Trimestral", "callFrequency": "Mensal",
      "dfFrequency": "Anual", "segmento": "Crédito Corporativo", "ratingOperation": "Baa3",
      "ratingGroup": "Baa1", "watchlist": "Verde", "projects": [{"name": "New Project"}],
      "guarantees": [], "defaultMonitoring": {"news": True}
    }

    response = test_client.post('/api/operations', data=json.dumps(new_op_data), content_type='application/json')
    
    assert response.status_code == 201
    data = json.loads(response.data)
    assert data['id'] == 99
    assert data['name'] == 'New Test Op'
    mock_conn.commit.assert_called_once()

def test_put_operation_success(test_client, mocker):
    """ Testa a atualização de uma operação existente via PUT /api/operations/<id> """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    # Mock das chamadas de SELECT para verificar a existência de sub-itens (eventos, regras, etc.)
    mock_cursor.execute.return_value.fetchall.side_effect = [
        [], # db_event_ids
        [], # db_rh_ids
        [], # db_rule_ids
    ]

    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mocker.patch('backend.app.get_db_connection', return_value=mock_conn)
    mocker.patch('backend.app.generate_tasks_for_operation', return_value=[])

    updated_op_data = MOCK_OPERATIONS_DATA[0].copy()
    updated_op_data['name'] = 'Updated Operation Name'
    updated_op_data['watchlist'] = 'Amarelo'

    response = test_client.put(f"/api/operations/{updated_op_data['id']}", 
                                data=json.dumps(updated_op_data),
                                content_type='application/json')

    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['name'] == 'Updated Operation Name'
    assert data['watchlist'] == 'Amarelo'
    
    # Verifica se a query de UPDATE foi chamada com os dados corretos
    mock_cursor.execute.assert_any_call(ANY, ('Updated Operation Name', 'Baa1', 'A4', 'Amarelo', 0.5, 1.2, 1))
    mock_conn.commit.assert_called_once()

def test_delete_operation_success(test_client, mocker):
    """ Testa a deleção de uma operação via DELETE /api/operations/<id> """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mocker.patch('backend.app.get_db_connection', return_value=mock_conn)

    op_id_to_delete = 1
    response = test_client.delete(f'/api/operations/{op_id_to_delete}')

    assert response.status_code == 204
    # Verifica se a query de DELETE foi chamada para a operação correta
    mock_cursor.execute.assert_any_call("DELETE FROM cri.crm.operations WHERE id = ?", (op_id_to_delete,))
    mock_conn.commit.assert_called_once()

def test_api_handles_database_error(test_client, mocker):
    """ Testa se a API retorna 500 quando o banco de dados falha """
    # Força a função get_db_connection a levantar uma exceção
    mocker.patch('backend.app.get_db_connection', side_effect=Exception("Database connection failed"))

    response = test_client.get('/api/operations')

    assert response.status_code == 500
    data = json.loads(response.data)
    assert 'error' in data
    assert data['error'] == "Database connection failed"
