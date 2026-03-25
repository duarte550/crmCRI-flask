-- CRM Mock Data Seeding Script (com testes par a todas as funcionalidades)
USE cri_cra_dev.crm;

-- ====================================================================
-- 1. LIMPEZA TOTAL DE TODAS AS TABELAS
-- ====================================================================
DELETE FROM cri_cra_dev.crm.task_exceptions;
DELETE FROM cri_cra_dev.crm.rating_history;
DELETE FROM cri_cra_dev.crm.events;
DELETE FROM cri_cra_dev.crm.task_rules;
DELETE FROM cri_cra_dev.crm.sync_queue;
DELETE FROM cri_cra_dev.crm.operation_review_notes;
DELETE FROM cri_cra_dev.crm.operation_risks;
DELETE FROM cri_cra_dev.crm.operation_projects;
DELETE FROM cri_cra_dev.crm.operation_guarantees;
DELETE FROM cri_cra_dev.crm.audit_logs;
DELETE FROM cri_cra_dev.crm.change_requests;
DELETE FROM cri_cra_dev.crm.patch_notes;
DELETE FROM cri_cra_dev.crm.analyst_notes;
DELETE FROM cri_cra_dev.crm.operations;
DELETE FROM cri_cra_dev.crm.projects;
DELETE FROM cri_cra_dev.crm.guarantees;
DELETE FROM cri_cra_dev.crm.structuring_operation_series;
DELETE FROM cri_cra_dev.crm.structuring_operations;
DELETE FROM cri_cra_dev.crm.master_group_contacts;
DELETE FROM cri_cra_dev.crm.master_groups;
DELETE FROM cri_cra_dev.crm.structuring_operation_stages;

-- ====================================================================
-- 2. INSERÇÃO DE MASTER GROUPS E CONTATOS
-- ====================================================================
INSERT INTO cri_cra_dev.crm.master_groups (id, name, sector, rating) VALUES
(1, 'Grupo Faria Lima & Shoppings', 'Real Estate / Shopping Centers', 'A4'),
(2, 'Agro forte S.A.', 'Agronegócio', 'Ba1'),
(3, 'Logística Expansão S.A.', 'Logística', 'Ba4'),
(4, 'Tech Infra Holding', 'Infraestrutura', 'A1');

INSERT INTO cri_cra_dev.crm.master_group_contacts (master_group_id, name, email, phone, role) VALUES
(1, 'Roberto Carlos', 'roberto@flima.com', '(11) 99999-1111', 'CFO'),
(1, 'Ana Clara', 'ana@flima.com', '(11) 99999-1112', 'RI'),
(2, 'João Agro', 'joao@agroforte.com', '(62) 98888-2222', 'Diretor Financeiro'),
(3, 'Maria Silva', 'maria@logexpansao.com', '(41) 97777-3333', 'Gerente de Tesouraria'),
(4, 'Pedro Tech', 'pedro@techinfra.com', '(31) 96666-4444', 'CEO');


-- ====================================================================
-- 3. INSERÇÃO ENTIDADES COMPARTILHADAS (Projetos e Garantias)
-- ====================================================================
INSERT INTO cri_cra_dev.crm.projects (id, name) VALUES
(1, 'Edifício Faria Lima Prime'), (2, 'Complexo Agroindustrial Forte'), (3, 'Shopping Pátio Central'),
(4, 'Fazendas Reunidas Bioenergia'), (5, 'Centro Logístico Sul'), (6, 'Datacenter SP1');

INSERT INTO cri_cra_dev.crm.guarantees (id, name) VALUES
(10, 'Alienação Fiduciária de Imóvel'), (11, 'Cessão Fiduciária de Recebíveis'),
(12, 'Fiança Corporativa do Grupo'), (13, 'Penhor de Ações da SPE'), (14, 'Aval dos Sócios');

-- ====================================================================
-- 4. INSERÇÃO DE OPERAÇÕES DE ESTOQUE (Operations)
-- ====================================================================
INSERT INTO cri_cra_dev.crm.operations 
(id, name, master_group_id, area, operation_type, maturity_date, responsible_analyst, review_frequency, call_frequency, df_frequency, segmento, rating_operation, rating_group, watchlist, ltv, dscr, monitoring_news, monitoring_spe_dfs, status, description, moved_to_legacy_date) VALUES
(10, 'CRI Edifício Faria Lima', 1, 'CRI', 'CRI', '2032-06-30T00:00:00', 'Fernanda', 'Anual', 'Trimestral', 'Semestral', 'Asset Finance - FII', 'A4', 'A4', 'Verde', 0.50, 2.5, true, true, 'Ativa', 'Operação estabilizada de locação comercial. Alocada em FIIs.', NULL),
(20, 'Debênture Agro Forte', 2, 'Capital Solutions', 'Debênture', '2029-09-30T00:00:00', 'Ricardo', 'Semestral', 'Mensal', 'Semestral', 'Crédito Corporativo', 'Ba1', 'Baa4', 'Amarelo', 0.6, 1.5, true, false, 'Ativa', 'Dívida para ampliação de capacidade produtiva.', NULL),
(30, 'CRI Shopping Pátio Central', 1, 'CRI', 'CRI', '2027-03-31T00:00:00', 'Fernanda', 'Trimestral', 'Mensal', 'Trimestral', 'Asset Finance', 'B3', 'Ba5', 'Amarelo', 0.75, 1.2, true, true, 'Ativa', 'Recuperação judicial, foco em trazer novas âncoras.', NULL),
(40, 'CRA Fazendas Reunidas', 2, 'CRA', 'CRA', '2031-08-31T00:00:00', 'Ricardo', 'Semestral', 'Trimestral', 'Semestral', 'Crédito Corporativo', 'Ba4', 'Ba1', 'Verde', null, null, true, true, 'Ativa', 'Produção e exportação de soja e milho.', NULL),
(50, 'CRI Logística Sul', 3, 'CRI', 'CRI', '2028-02-28T00:00:00', 'Fernanda', 'Semestral', 'Mensal', 'Trimestral', 'Financiamento Construção', 'Ba6', 'Ba4', 'Verde', 0.7, 1.6, true, true, 'Ativa', 'Galpões BTS BTL em SC.', NULL),
(60, 'Debênture Tech Infra (Legado)', 4, 'Capital Solutions', 'Debênture', '2024-01-30T00:00:00', 'João', 'Anual', 'Anual', 'Anual', 'Infraestrutura', 'A1', 'A1', 'Verde', 0.3, 3.5, false, false, 'Legada', 'Operação já liquidada.', '2024-02-05T00:00:00');

INSERT INTO cri_cra_dev.crm.operation_projects (operation_id, project_id) VALUES (10, 1), (20, 2), (30, 3), (40, 4), (50, 5), (60, 6);
INSERT INTO cri_cra_dev.crm.operation_guarantees (operation_id, guarantee_id) VALUES (10, 10), (20, 11), (20, 12), (30, 10), (30, 11), (40, 11), (40, 14), (50, 10), (60, 12);

-- Histórico de Ratings
INSERT INTO cri_cra_dev.crm.rating_history (operation_id, date, rating_operation, rating_group, watchlist, sentiment, event_id) VALUES
(10, '2024-01-20T10:00:00', 'A4', 'A4', 'Verde', 'Neutro', NULL),
(20, '2024-02-10T09:00:00', 'Baa4', 'Baa4', 'Verde', 'Neutro', NULL),
(20, '2024-06-05T15:00:00', 'Ba1', 'Baa4', 'Amarelo', 'Negativo', NULL),
(30, '2023-11-20T16:00:00', 'C2', 'B1', 'Vermelho', 'Negativo', NULL),
(30, '2024-02-15T10:00:00', 'C2', 'B1', 'Vermelho', 'Neutro', NULL),
(30, '2024-05-30T14:00:00', 'B3', 'Ba5', 'Amarelo', 'Positivo', NULL),
(40, '2024-07-01T14:00:00', 'Ba4', 'Ba1', 'Verde', 'Neutro', NULL),
(50, '2024-03-01T10:00:00', 'Ba6', 'Ba4', 'Verde', 'Neutro', NULL),
(60, '2021-01-01T10:00:00', 'A1', 'A1', 'Verde', 'Neutro', NULL);

-- Regras de Tarefas da Gestão de Estoque
INSERT INTO cri_cra_dev.crm.task_rules (operation_id, name, frequency, start_date, end_date, description, priority, is_origination) VALUES
(10, 'Revisão Política', 'Anual', '2024-01-20T00:00:00', '2032-06-30T00:00:00', 'Revisão de política de crédito anual.', 'Média', false),
(10, 'Call Trimestral', 'Trimestral', '2024-01-20T00:00:00', '2032-06-30T00:00:00', 'Call de acompanhamento.', 'Baixa', false),
(20, 'Revisão Gerencial Semestral', 'Semestral', '2024-02-10T00:00:00', '2029-09-30T00:00:00', 'Revisão periódica', 'Alta', false),
(30, 'Revisão Crise (Gerencial Trimestral)', 'Trimestral', '2023-01-15T00:00:00', '2027-03-31T00:00:00', 'Revisão de crise do shopping.', 'Urgente', false),
(50, 'Relatório Mensal de Obra', 'Mensal', '2024-03-30T00:00:00', '2025-12-31T00:00:00', 'Análise Eng.', 'Alta', false),
(50, 'Envio Aditivo Cartório', 'Pontual', '2024-10-15T00:00:00', '2024-10-15T00:00:00', 'Aditivo de prazo.', 'Urgente', false);


-- ====================================================================
-- 5. INSERÇÃO DE ESTRUTURAÇÕES (Originação)
-- ====================================================================
INSERT INTO cri_cra_dev.crm.structuring_operations 
(id, master_group_id, name, stage, liquidation_date, rate, indexer, volume, fund, risk, temperature, is_active) VALUES
(1, 1, 'CRI Faria Lima Fase 2', 'em estruturação', '2024-12-15T00:00:00', 'CDI + 2.5%', 'CDI', 150000000.00, 'FII XPTO', 'High Grade', 'Morno', true),
(2, 2, 'CRA Agro Forte Expansão', 'mandato assinado', '2024-11-30T00:00:00', 'IPCA + 7.0%', 'IPCA', 85000000.00, 'Fundo Agro CRA', 'High Yield', 'Quente', true),
(3, 3, 'Debênture Logística Sul', 'conversa inicial', '2025-03-01T00:00:00', 'CDI + 3.0%', 'CDI', 200000000.00, 'Múltiplos Investidores', 'High Yield', 'Frio', true),
(4, 1, 'CRI Faria Lima Refin', 'comitê aprovado', '2024-10-15T00:00:00', 'IPCA + 6.5%', 'IPCA', 50000000.00, 'FII ABC', 'High Grade', 'Quente', true),
(5, 4, 'Debênture Tech Infra Nova', 'liquidação', '2024-10-30T00:00:00', 'CDI + 1.5%', 'CDI', 300000000.00, 'Fundo Mestre', 'High Grade', 'Morno', false);

-- Séries das Estruturações
INSERT INTO cri_cra_dev.crm.structuring_operation_series (structuring_operation_id, name, rate, indexer, volume, fund) VALUES
(1, 'Série Sênior', '2.5%', 'CDI', 100000000.00, 'FII XPTO'),
(1, 'Série Subordinada', '4.5%', 'CDI', 50000000.00, 'FII XPTO'),
(2, 'Série Única', '7.0%', 'IPCA', 85000000.00, 'Fundo Agro CRA'),
(3, 'Série 1', '3.0%', 'CDI', 100000000.00, 'Fundo A'),
(3, 'Série 2', '3.5%', 'CDI', 100000000.00, 'Fundo B'),
(4, 'Série Refinanciamento', '6.5%', 'IPCA', 50000000.00, 'FII ABC');

-- Regras de Tarefas da Originação
INSERT INTO cri_cra_dev.crm.task_rules (structuring_operation_id, name, frequency, start_date, end_date, description, priority, is_origination) VALUES
(1, 'Diligence Jurídica', 'Pontual', '2024-11-20T00:00:00', '2024-11-20T00:00:00', 'Revisar contratos.', 'Urgente', true),
(1, 'KYC', 'Pontual', '2024-11-25T00:00:00', '2024-11-25T00:00:00', 'Verificar apontamentos.', 'Média', true),
(2, 'Comitê 2', 'Pontual', '2024-11-15T00:00:00', '2024-11-15T00:00:00', 'Aprovar condições definitivas.', 'Alta', true),
(3, 'Modelo DCF', 'Pontual', '2024-12-05T00:00:00', '2024-12-05T00:00:00', 'Valuation.', 'Alta', true);

-- Etapas do Kanban
INSERT INTO cri_cra_dev.crm.structuring_operation_stages (structuring_operation_id, order_index, name, is_completed) VALUES
(1, 0, 'Conversa Inicial', true),
(1, 1, 'Proposta', true),
(1, 2, 'Comitê Operacional', false),
(1, 3, 'Due Diligence', false),
(1, 4, 'Liquidação', false),

(2, 0, 'Conversa Inicial', true),
(2, 1, 'Proposta', true),
(2, 2, 'Comitê Operacional', true),
(2, 3, 'Due Diligence', true),
(2, 4, 'Liquidação', false),

(3, 0, 'Conversa Inicial', true),
(3, 1, 'Proposta', false),
(3, 2, 'Comitê Operacional', false),
(3, 3, 'Due Diligence', false),
(3, 4, 'Liquidação', false),

(4, 0, 'Conversa Inicial', true),
(4, 1, 'Proposta', true),
(4, 2, 'Comitê Operacional', true),
(4, 3, 'Due Diligence', true),
(4, 4, 'Liquidação', true),

(5, 0, 'Conversa Inicial', true),
(5, 1, 'Proposta', true),
(5, 2, 'Comitê Operacional', true),
(5, 3, 'Due Diligence', true),
(5, 4, 'Liquidação', true);


-- ====================================================================
-- 6. INSERÇÃO DE EVENTOS MISTOS
-- ====================================================================
-- Master Groups Events
INSERT INTO cri_cra_dev.crm.events (master_group_id, date, type, title, description, registered_by, is_origination) VALUES
(1, '2024-05-10T10:00:00', 'Reunião', 'Governança', 'Alinhamento com CFO do Grupo sobre a estratégia 2025.', 'Fernanda', false),
(2, '2024-06-20T14:00:00', 'Call', 'Call Trimestral Holding', 'Aumento de margem Ebitda.', 'Ricardo', false);

-- Operações de Estoque Events
INSERT INTO cri_cra_dev.crm.events (operation_id, date, type, title, description, registered_by, next_steps, attention_points, our_attendees, operation_attendees) VALUES
(10, '2024-04-25T11:00:00', 'Call Trimestral', 'Acompanhamento', 'Vacância baixa.', 'Fernanda', 'Monitorar INCC.', 'Sem pontos.', 'Fernanda', 'Roberto CFO'),
(20, '2024-06-05T15:00:00', 'Mudança de Watchlist', 'Downgrade PDD', 'Aumento inesperado. Risco de quebra de cov.', 'Ricardo', 'Novo cronograma.', 'Covenant técnico', 'Ricardo', 'João Agro'),
(30, '2023-11-20T16:00:00', 'Reunião Especial', 'Aumento Vacância', 'Saída de ancoras.', 'Fernanda', 'Aprov. em assembleia.', 'Despejo da loja X', 'Fernanda', 'Adm Shopping');

-- Structuring Operations Events
INSERT INTO cri_cra_dev.crm.events (structuring_operation_id, date, type, title, description, registered_by, is_origination, structuring_operation_stage_id) VALUES
(1, '2024-08-01T11:00:00', 'Upload de Documento', 'Term Sheet Assinada', 'Contrato inicial fechado com o cliente, pendente aprovação em comitê interno.', 'Fernanda', true, 2),
(2, '2024-08-15T15:00:00', 'Reunião', 'Comitê Operacional', 'Termos totalmente fechados. Equipe aprovou.', 'Ricardo', true, 8),
(3, '2024-09-01T15:00:00', 'Reunião Presencial', 'Conversa Inicial com o Cliente', 'Call introdutório para apresentação da operação de logística e viabilidade do indexador.', 'Tiago', true, 11);

-- ====================================================================
-- 7. OUTROS DADOS COMPLEMENTARES
-- ====================================================================
-- Analyst Notes
INSERT INTO cri_cra_dev.crm.analyst_notes (analyst_name, notes, updated_at) VALUES 
('Fernanda', 'Lembrar de cobrar CFO na terça.', '2024-10-01T00:00:00');

-- Operation Risks
INSERT INTO cri_cra_dev.crm.operation_risks (operation_id, title, description, severity, created_at, updated_at) VALUES
(30, 'Risco de Liquidez das SPEs', 'SPE12 está queimando caixa devido vacância.', 'Alta', '2024-09-01T10:00:00', '2024-09-01T10:00:00');

-- Operation Review Notes
INSERT INTO cri_cra_dev.crm.operation_review_notes (operation_id, notes, updated_at, updated_by) VALUES
(10, 'Excelente ativo. Monitorar apenas macroeconomia.', '2024-10-05T00:00:00', 'Fernanda');

-- Audit Logs
INSERT INTO cri_cra_dev.crm.audit_logs (timestamp, user_name, action, entity_type, entity_id, details) VALUES
('2024-10-20T10:00:00', 'Fernanda', 'UPDATE', 'Operation', '10', 'Alterou rating de A3 para A4');

-- Change Requests
INSERT INTO cri_cra_dev.crm.change_requests (title, description, requester, status, created_at, updated_at) VALUES
('Filtros Avançados', 'Poder cruzar risco vs yield na analise de comparáveis', 'Ricardo', 'pending', '2024-10-25T10:00:00', '2024-10-25T10:00:00');

