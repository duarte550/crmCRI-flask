
-- CRM Mock Data Seeding Script (v2)
-- Este script primeiro APAGARÁ todos os dados existentes e depois INSERIRÁ um novo conjunto de 5 operações
-- com históricos e tarefas variadas para testes abrangentes do frontend.

-- Usar o schema correto
USE cri_cra_dev.crm;

-- ====================================================================
-- 1. LIMPEZA: Apagar todos os dados existentes na ordem correta de dependência
-- ====================================================================
DELETE FROM cri_cra_dev.crm.task_exceptions;
DELETE FROM cri_cra_dev.crm.rating_history;
DELETE FROM cri_cra_dev.crm.events;
DELETE FROM cri_cra_dev.crm.task_rules;
DELETE FROM cri_cra_dev.crm.operation_projects;
DELETE FROM cri_cra_dev.crm.operation_guarantees;
DELETE FROM cri_cra_dev.crm.operation_review_notes;
DELETE FROM cri_cra_dev.crm.audit_logs;
DELETE FROM cri_cra_dev.crm.operations;
DELETE FROM cri_cra_dev.crm.projects;
DELETE FROM cri_cra_dev.crm.guarantees;

-- ====================================================================
-- 2. INSERÇÃO DE DADOS MOCK
-- ====================================================================

-- Inserir entidades compartilhadas (Projetos e Garantias)
INSERT INTO cri_cra_dev.crm.projects (id, name) VALUES
(1, 'Edifício Faria Lima Prime'),
(2, 'Complexo Agroindustrial Forte'),
(3, 'Shopping Pátio Central'),
(4, 'Fazendas Reunidas Bioenergia'),
(5, 'Centro Logístico Sul'),
(6, 'Parque Solar Horizonte');

INSERT INTO cri_cra_dev.crm.guarantees (id, name) VALUES
(10, 'Alienação Fiduciária de Imóvel'),
(11, 'Cessão Fiduciária de Recebíveis'),
(12, 'Fiança Corporativa do Grupo'),
(13, 'Penhor de Ações da SPE'),
(14, 'Aval dos Sócios');

-- --- Operação 1: CRI Faria Lima (Operação CRI estável e de alta qualidade) ---
INSERT INTO cri_cra_dev.crm.operations 
(id, name, area, operation_type, maturity_date, responsible_analyst, review_frequency, call_frequency, df_frequency, segmento, rating_operation, rating_group, watchlist, ltv, dscr, monitoring_news, monitoring_spe_dfs) VALUES
(10, 'CRI Edifício Faria Lima', 'CRI', 'CRI', '2032-06-30T00:00:00', 'Fernanda', 'Anual', 'Trimestral', 'Semestral', 'Asset Finance - FII', 'A4', 'A4', 'Verde', 0.50, 2.5, true, true);

INSERT INTO cri_cra_dev.crm.operation_projects (operation_id, project_id) VALUES (10, 1);
INSERT INTO cri_cra_dev.crm.operation_guarantees (operation_id, guarantee_id) VALUES (10, 10);

INSERT INTO cri_cra_dev.crm.task_rules (operation_id, name, frequency, start_date, end_date, description) VALUES
(10, 'Revisão Política', 'Anual', '2024-01-20T00:00:00', '2032-06-30T00:00:00', 'Revisão de política de crédito anual.'),
(10, 'Call de Acompanhamento', 'Trimestral', '2024-01-20T00:00:00', '2032-06-30T00:00:00', 'Call de acompanhamento com o cliente.');

INSERT INTO cri_cra_dev.crm.events (operation_id, date, type, title, description, registered_by, next_steps, completed_task_id) VALUES
(10, '2024-01-20T10:00:00', 'Inicial', 'Criação da Operação', 'Operação iniciada com rating A4 e status Verde.', 'System', 'Monitoramento inicial.', NULL),
(10, '2024-04-25T11:00:00', 'Call Trimestral', 'Call de Acompanhamento', 'Performance em linha com o esperado, vacância baixa.', 'Fernanda', 'Agendar próxima call.', 'op10-rule102-2024-04-20');

INSERT INTO cri_cra_dev.crm.rating_history (operation_id, date, rating_operation, rating_group, watchlist, sentiment, event_id) VALUES
(10, '2024-01-20T10:00:00', 'A4', 'A4', 'Verde', 'Neutro', NULL);


-- --- Operação 2: Debênture Agro Forte (Piorou para Amarelo) ---
INSERT INTO cri_cra_dev.crm.operations
(id, name, area, operation_type, maturity_date, responsible_analyst, review_frequency, call_frequency, df_frequency, segmento, rating_operation, rating_group, watchlist, ltv, dscr, monitoring_news, monitoring_operational_info) VALUES
(20, 'Debênture Agro Forte', 'Capital Solutions', 'Debênture', '2029-09-30T00:00:00', 'Ricardo', 'Semestral', 'Mensal', 'Semestral', 'Crédito Corporativo - Carteira', 'Ba1', 'Baa4', 'Amarelo', 0.6, 1.5, true, true);

INSERT INTO cri_cra_dev.crm.operation_projects (operation_id, project_id) VALUES (20, 2);
INSERT INTO cri_cra_dev.crm.operation_guarantees (operation_id, guarantee_id) VALUES (20, 11), (20, 12);

INSERT INTO cri_cra_dev.crm.task_rules (operation_id, name, frequency, start_date, end_date, description) VALUES
(20, 'Revisão Política', 'Anual', '2024-02-10T00:00:00', '2029-09-30T00:00:00', 'Revisão de política de crédito.'),
(20, 'Revisão Gerencial', 'Semestral', '2024-02-10T00:00:00', '2029-09-30T00:00:00', 'Revisão periódica gerencial.'),
(20, 'Análise de DFs & Dívida', 'Semestral', '2024-02-10T00:00:00', '2029-09-30T00:00:00', 'Análise dos DFs e endividamento.');

INSERT INTO cri_cra_dev.crm.events (operation_id, date, type, title, description, registered_by, next_steps) VALUES
(20, '2024-02-10T09:00:00', 'Inicial', 'Criação da Operação', 'Operação iniciada em Capital Solutions.', 'System', 'Acompanhamento da performance da carteira.'),
(20, '2024-06-05T15:00:00', 'Mudança de Watchlist', 'Alteração para Amarelo e Downgrade', 'Aumento inesperado da PDD na carteira de recebíveis cedida.', 'Ricardo', 'Aumentar frequência de monitoramento e solicitar plano de ação do cliente.');

INSERT INTO cri_cra_dev.crm.rating_history (operation_id, date, rating_operation, rating_group, watchlist, sentiment, event_id) VALUES
(20, '2024-02-10T09:00:00', 'Baa4', 'Baa4', 'Verde', 'Neutro', NULL),
(20, '2024-06-05T15:00:00', 'Ba1', 'Baa4', 'Amarelo', 'Negativo', NULL);


-- --- Operação 3: Shopping Pátio (Histórico complexo, em recuperação) ---
INSERT INTO cri_cra_dev.crm.operations
(id, name, area, operation_type, maturity_date, responsible_analyst, review_frequency, call_frequency, df_frequency, segmento, rating_operation, rating_group, watchlist, ltv, dscr, monitoring_receivables_portfolio, monitoring_spe_dfs) VALUES
(30, 'CRI Shopping Pátio Central', 'CRI', 'CRI', '2027-03-31T00:00:00', 'Fernanda', 'Trimestral', 'Mensal', 'Trimestral', 'Asset Finance', 'B3', 'Ba5', 'Amarelo', 0.75, 1.2, true, true);

INSERT INTO cri_cra_dev.crm.operation_projects (operation_id, project_id) VALUES (30, 3);
INSERT INTO cri_cra_dev.crm.operation_guarantees (operation_id, guarantee_id) VALUES (30, 10), (30, 11);

INSERT INTO cri_cra_dev.crm.task_rules (operation_id, name, frequency, start_date, end_date, description) VALUES
(30, 'Revisão Gerencial', 'Trimestral', '2023-01-15T00:00:00', '2027-03-31T00:00:00', 'Revisão periódica gerencial.'),
(30, 'Revisão Política', 'Semestral', '2023-01-15T00:00:00', '2027-03-31T00:00:00', 'Revisão de política de crédito (rating C).'),
(30, 'Call de Acompanhamento', 'Mensal', '2023-01-15T00:00:00', '2027-03-31T00:00:00', 'Call de acompanhamento.');

INSERT INTO cri_cra_dev.crm.events (operation_id, date, type, title, description, registered_by, next_steps, completed_task_id) VALUES
(30, '2023-11-20T16:00:00', 'Mudança de Watchlist', 'Alteração para Vermelho', 'Saída de âncora importante, vacância subiu para 30%. Risco de rompimento de covenant.', 'Fernanda', 'Notificar garantidores e elaborar plano de reestruturação.', NULL),
(30, '2024-02-15T10:00:00', 'Reunião', 'Aprovação do Plano de Ação', 'Plano de ação com novas condições comerciais e prospecção de novos lojistas foi aprovado.', 'Fernanda', 'Acompanhar execução do plano.', 'op30-rule301-2024-01-15'),
(30, '2024-05-30T14:00:00', 'Mudança de Watchlist', 'Alteração para Amarelo e Upgrade', 'Assinatura de contrato com duas novas lojas, vacância reduzida para 15%. DSCR projetado normalizado.', 'Fernanda', 'Manter monitoramento próximo da performance comercial.', NULL);

INSERT INTO cri_cra_dev.crm.rating_history (operation_id, date, rating_operation, rating_group, watchlist, sentiment, event_id) VALUES
(30, '2023-11-20T16:00:00', 'C2', 'B1', 'Vermelho', 'Negativo', NULL),
(30, '2024-02-15T10:00:00', 'C2', 'B1', 'Vermelho', 'Neutro', NULL),
(30, '2024-05-30T14:00:00', 'B3', 'Ba5', 'Amarelo', 'Positivo', NULL);


-- --- Operação 4: CRA Fazendas (Recém-criada) ---
INSERT INTO cri_cra_dev.crm.operations
(id, name, area, operation_type, maturity_date, responsible_analyst, review_frequency, call_frequency, df_frequency, segmento, rating_operation, rating_group, watchlist, ltv, dscr, monitoring_operational_info, monitoring_receivables_portfolio) VALUES
(40, 'CRA Fazendas Reunidas', 'CRI', 'CRA', '2031-08-31T00:00:00', 'Ricardo', 'Semestral', 'Trimestral', 'Semestral', 'Crédito Corporativo', 'Ba4', 'Ba1', 'Verde', null, null, true, true);

INSERT INTO cri_cra_dev.crm.operation_projects (operation_id, project_id) VALUES (40, 4);
INSERT INTO cri_cra_dev.crm.operation_guarantees (operation_id, guarantee_id) VALUES (40, 11), (40, 14);

INSERT INTO cri_cra_dev.crm.task_rules (operation_id, name, frequency, start_date, end_date, description) VALUES
(40, 'Revisão Gerencial', 'Semestral', '2024-07-01T00:00:00', '2031-08-31T00:00:00', 'Revisão periódica gerencial.'),
(40, 'Revisão Política', 'Anual', '2024-07-01T00:00:00', '2031-08-31T00:00:00', 'Revisão de política de crédito.');

INSERT INTO cri_cra_dev.crm.events (operation_id, date, type, title, description, registered_by, next_steps) VALUES
(40, '2024-07-01T14:00:00', 'Inicial', 'Criação da Operação', 'Operação de CRA recém-criada.', 'System', 'Iniciar monitoramento trimestral.');

INSERT INTO cri_cra_dev.crm.rating_history (operation_id, date, rating_operation, rating_group, watchlist, sentiment, event_id) VALUES
(40, '2024-07-01T14:00:00', 'Ba4', 'Ba1', 'Verde', 'Neutro', NULL);


-- --- Operação 5: Logística Sul (Com tarefa atrasada) ---
INSERT INTO cri_cra_dev.crm.operations
(id, name, area, operation_type, maturity_date, responsible_analyst, review_frequency, call_frequency, df_frequency, segmento, rating_operation, rating_group, watchlist, ltv, dscr, monitoring_construction_report, monitoring_commercial_info) VALUES
(50, 'CRI Logística Sul', 'CRI', 'CRI', '2028-02-28T00:00:00', 'Fernanda', 'Semestral', 'Mensal', 'Trimestral', 'Financiamento Construção', 'Ba6', 'Ba4', 'Verde', 0.7, 1.6, true, true);

INSERT INTO cri_cra_dev.crm.operation_projects (operation_id, project_id) VALUES (50, 5);
INSERT INTO cri_cra_dev.crm.operation_guarantees (operation_id, guarantee_id) VALUES (50, 10);

INSERT INTO cri_cra_dev.crm.task_rules (operation_id, name, frequency, start_date, end_date, description) VALUES
-- Esta regra ABAIXO irá gerar uma tarefa ATRASADA se a data atual for posterior a 30/04/2024
(50, 'Relatório Mensal de Obra', 'Mensal', '2024-03-30T00:00:00', '2025-12-31T00:00:00', 'Análise do relatório mensal de avanço da obra.'),
(50, 'Revisão Gerencial', 'Semestral', '2024-03-01T00:00:00', '2028-02-28T00:00:00', 'Revisão periódica gerencial.');

INSERT INTO cri_cra_dev.crm.events (operation_id, date, type, title, description, registered_by, next_steps) VALUES
(50, '2024-03-01T10:00:00', 'Inicial', 'Criação da Operação', 'Operação de financiamento à construção.', 'System', 'Acompanhar cronograma de obras.');

INSERT INTO cri_cra_dev.crm.rating_history (operation_id, date, rating_operation, rating_group, watchlist, sentiment, event_id) VALUES
(50, '2024-03-01T10:00:00', 'Ba6', 'Ba4', 'Verde', 'Neutro', NULL);
