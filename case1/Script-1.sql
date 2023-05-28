-- CASE 1

--create schema case_1;

-- Создание таблицы контрагента
create table Counterparty (
Counterparty_rk int not null,
Counterparty_name text,
Dict_counterparty_type_cd text not null,
Counterparty_open_date date,
Effective_from_date date not null,
Effective_to_date date not null,
"User" text not null,
Deleted_flg bool not null,
processed_dttm timestamp not null
)


-- Создание таблицы счёта
create table Account (
Account_num text not null,
Account_type text,
Dict_status_cd text not null,
Dict_currency_cd text not null,
Dict_balance_account_2_cd text not null,
Account_open_date date not null,
Account_close_date date,
Dict_bank_division_cd text not null,
Effective_from_date date not null,
Effective_to_date date not null,
"User" text not null,
Deleted_flg bool not null,
processed_dttm timestamp not null
)


-- Создание моста контрагент-счёт
create table Counterparty_x_account (
Counterparty_rk int not null,
Account_num text not null,
Effective_from_date date not null,
Effective_to_date date not null,
"User" text not null,
Deleted_flg bool not null,
processed_dttm timestamp not null
)

 
-- Создание справочника банковских подразделений
create table Dict_bank_division (
Dict_bank_division_cd text not null,
Dict_bank_division_name text,
Dict_bank_division_adress text,
Dict_bank_division_phone_number text,
Dict_bank_division_creation_date date,
Effective_from_date date not null,
Effective_to_date date not null,
"User" text not null,
Deleted_flg bool not null,
processed_dttm timestamp not null
)


 

-- Создание справочника планов счёта
create table Dict_balance_account (
Dict_balance_account_1_cd text not null,
Dict_balance_account_2_cd text not null,
Dict_balance_account_name text,
Dict_balance_account_sign text,
Effective_from_date date not null,
Effective_to_date date not null,
"User" text not null,
Deleted_flg bool not null,
processed_dttm timestamp not null
)


-- Создание справочника статусов счёта
create table Dict_status (
Dict_status_cd text not null,
Dict_status_type text not null,
Effective_from_date date not null,
Effective_to_date date not null,
"User" text not null,
Deleted_flg bool not null,
processed_dttm timestamp not null
)


-- Создание справочника валют
create table Dict_currency (
Dict_currency_cd text not null,
Dict_currency_name text not null,
Dict_currency_strcd text not null,
Effective_from_date date not null,
Effective_to_date date not null,
"User" text not null,
Deleted_flg bool not null,
processed_dttm timestamp not null
)


-- Создание справочника типов контрагента
create table Dict_counterparty_type (
Dict_counterparty_type_cd text not null,
Dict_counterparty_type_name text not null,
Effective_from_date date not null,
Effective_to_date date not null,
"User" text not null,
Deleted_flg bool not null,
processed_dttm timestamp not null
)



-- Добавление данных counterparty
insert into counterparty values
(1, 'Козлов Савелий Петрович', '1', '2015-10-03', '2015-10-03', '2999-12-31', 'Denis Melekhin', false, '2015-10-03'),
(2, 'ИП Алексеев Алексей Максимович', '2', '2015-11-11', '2015-11-11', '2999-12-31', 'Denis Melekhin', false, '2015-11-11'),
(3, 'Виноградова Дарья Львовна', '1', '2016-03-19', '2016-03-19', '2999-12-31', 'Denis Melekhin', false, '2016-03-19'),
(4, 'ИП Белов Максим Тимофеевич', '2', '2016-05-29', '2016-05-29', '2999-12-31', 'Denis Melekhin', false, '2016-05-29'),
(5, 'Чернышев Захар Богданович', '1', '2016-05-30', '2016-05-30', '2999-12-31', 'Denis Melekhin', false, '2016-05-30');

 
-- Добавление данных в account
insert into account values
('40817810162010000001', 'Текущий', '1', '810', '40817', '2015-10-08', '2999-12-31', '6201', '2015-10-08', '2999-12-31', 'Denis Melekhin', false, '2015-10-08'),
('40802810162020000002', 'Расчётный', '1', '810', '40802', '2015-11-16', '2999-12-31', '6202','2015-11-16', '2999-12-31', 'Denis Melekhin', false, '2015-11-16'),
('40813840162030000003', 'Валютный', '1', '840', '40813','2016-03-24', '2999-12-31', '6203', '2016-03-24', '2999-12-31', 'Denis Melekhin', false, '2016-03-24'),
('42109810162040000004', 'Депозитный', '1', '810', '42109', '2016-06-03', '2016-07-02', '6204', '2016-06-03', '2016-07-02', 'Denis Melekhin', true, '2016-06-03'),
('45507810162040000005', 'Кредитный', '1', '810', '45507', '2016-06-05', '2999-12-31', '6204', '2016-06-05', '2999-12-31', 'Denis Melekhin', false, '2016-06-05'),
('42109810162040000004', 'Депозитный', '2', '810', '42109', '2016-06-03', '2016-07-02', '6204', '2016-07-02', '2999-12-31', 'Denis Melekhin', false, '2016-07-03');


-- Добавление данных в counterparty_x_account
insert into counterparty_x_account values
(1, '40817810162010000001', '2015-10-08', '2999-12-31', 'Denis Melekhin', false, '2015-10-08'),
(2, '40802810162020000002', '2015-11-16', '2999-12-31', 'Denis Melekhin', false, '2015-11-16'),
(3, '40813840162030000003', '2016-03-24', '2999-12-31', 'Denis Melekhin', false, '2016-03-24'),
(4, '42109810162040000004', '2016-07-03', '2999-12-31', 'Denis Melekhin', false, '2016-07-03'),
(5, '45507810162040000005', '2016-06-05', '2999-12-31', 'Denis Melekhin', false, '2016-06-05');

