--create schema oltp_src_system;
--create schema dwh_stage;
--create schema oltp_cdc_src_system;
--create schema dwh_ods;
--create schema report;

-- таблица системы генерации синтетических данных
drop table if exists oltp_src_system.tracking_products;
create table oltp_src_system.tracking_products (
    id int4 null,
    status_nm text null,
    create_dttm timestamp null,
    update_dttm timestamp null
);

-- лог изменений CDC
drop table if exists oltp_cdc_src_system.cdc_tracking_products_information;
create table oltp_cdc_src_system.cdc_tracking_products_information (
    id int4 not null,
    status_nm text null,
    create_dttm timestamptz null,
    update_dttm timestamptz null,
    operation_type bpchar(1) not null,
    load_dttm timestamptz not null,
    hash bytea null generated always as (digest((((coalesce(status_nm::text, '#$%^&'::text) 
    || date_part('epoch'::text, coalesce(timezone('utc'::text, create_dttm), '1990-01-01 00:00:00'::timestamp without time zone))::text) 
    || date_part('epoch'::text, coalesce(timezone('utc'::text, update_dttm), '1990-01-01 00:00:00'::timestamp without time zone))::text)
    || coalesce(operation_type::text, '#$%^&'::text))
    , 'sha256'::text)) stored
);

-- функция загрузки изменений в CDC
create or replace function oltp_cdc_src_system.cdc_tracking_products_information()
returns trigger
language plpgsql
as $function$
    begin
        if (tg_op = 'DELETE') then
            insert into oltp_cdc_src_system.cdc_tracking_products_information select old.*, 'D', now();
            return old;
        elsif (tg_op = 'UPDATE') then
            insert into oltp_cdc_src_system.cdc_tracking_products_information select new.*, 'U', now();
            return new;
        elsif (tg_op = 'INSERT') then
            insert into oltp_cdc_src_system.cdc_tracking_products_information select new.*, 'I', now();
            return new;
        end if;
        return null;
    end;
$function$
;

-- триггер на изменения записей в синтетических данных 
create trigger tracking_products_changes after
insert
    or
delete
    or
update
    on
    oltp_src_system.tracking_products for each row execute procedure oltp_cdc_src_system.cdc_tracking_products_information();

-- функция создания записей
create or replace function oltp_src_system.create_tracking()
returns boolean
language plpgsql
as $function$
declare
    v_rc int;
begin
insert into oltp_src_system.tracking_products (id,status_nm,create_dttm, update_dttm)
select id
     , 'ordered' status_nm
     , now() create_dttm
     , now() update_dttm
  from (select n id
          from generate_series((select coalesce((select max(id) 
                                  from oltp_src_system.tracking_products) + 1, 1)
                                )
                              , (select coalesce((select max(id) 
                                                    from oltp_src_system.tracking_products) + 1, 1)
                              ) + round(random()*3)::int
                    ) n) nn;
    get diagnostics v_rc = row_count;           
    raise notice '% rows inserted into tracking_products',v_rc;
    return true;
end $function$
;

-- функция удаления записей 
create or replace function oltp_src_system.delete_existed_tracking()
returns boolean
language plpgsql
as $function$
declare
    v_rc int;
begin                   
delete from oltp_src_system.tracking_products
where id in (
             select id
               from (select id 
                          , round(random()*100) rnd
                       from oltp_src_system.tracking_products
                    ) rnd_tbl
              where (rnd - floor(rnd/100)) = 1
            );
    get diagnostics v_rc = row_count;           
    raise notice '% rows deleted from tracking_products',v_rc;
    return true;
end $function$
;

-- функция обновления записей
create or replace function oltp_src_system.update_existed_tracking()
returns boolean
language plpgsql
as $function$
declare
    v_rc int;
begin                   
update oltp_src_system.tracking_products
set status_nm = case (floor(random() * (5 - 1 + 1) + 1)::int)
                     when 1 then 'out of stock'
                     when 2 then 'in stock'
                     when 3 then 'expired'
                     when 4 then 'reserved'
                     when 5 then 'stopped'
                     else null
                 end 
    , update_dttm = now()
where id in (select id
               from (select id 
                          , round(random()*10) rnd
                       from oltp_src_system.tracking_products
                       where status_nm not in ('stopped')
                    ) rnd_tbl
              where (rnd - floor(rnd/10)) = 1);

    get diagnostics v_rc = row_count;           
    raise notice '% rows updated into tracking_products',v_rc;
    return true;
end $function$
;

-- создание слоя stage
drop table if exists dwh_stage.tracking_products_information_src;
create table dwh_stage.tracking_products_information_src (
    id int4 not null,
    status_nm text null,
    create_dttm timestamptz null,
    update_dttm timestamptz null,
    operation_type bpchar(1) not null,
    load_dttm timestamp not null,
  	hash bytea null
);

-- функция заполнения stage
create or replace function dwh_stage.load_from_cdc_tracking_products_information()
returns boolean
language plpgsql
as $function$
begin
	truncate table dwh_stage.tracking_products_information_src;
	insert into dwh_stage.tracking_products_information_src (id, status_nm, create_dttm, update_dttm, operation_type, load_dttm, hash)
		select id
		, status_nm
		, create_dttm
		, update_dttm
		, operation_type
		, load_dttm
		, hash
        from oltp_cdc_src_system.cdc_tracking_products_information cdc
   			where not exists (select null 
                      from dwh_ods.tracking_products_information_hist ods
                      where ods.id = cdc.id
              and cdc.hash in (select hash 
                      from dwh_ods.tracking_products_information_hist ods));
    return true;
end $function$
;

-- создание слоя ods
drop table if exists dwh_ods.tracking_products_information_hist;
create table dwh_ods.tracking_products_information_hist(
 	id int4 not null,
    status_nm text null,
    create_dttm timestamptz null,
    update_dttm timestamptz null,
    operation_type bpchar(1) not null,
    load_dttm timestamp not null,
    hash bytea null,
 	valid_from_dttm timestamptz null,
    valid_to_dttm timestamptz null,
    deleted_flg bpchar(1) null,
    deleted_dttm timestamptz null
);

-- функция заполнения ods
create or replace function dwh_ods.load_from_tracking_products_information_src()
 returns boolean
 language plpgsql
as $function$
    declare
        v_load_dttm timestamp = now();
    begin
        update dwh_ods.tracking_products_information_hist ods
           set valid_to_dttm = now() - interval '1 second'
         where exists (select null
                         from dwh_stage.tracking_products_information_src src
                        where src.operation_type in ('U', 'D')
                              and ods.id = src.id);
        insert into dwh_ods.tracking_products_information_hist (
            id, status_nm, create_dttm, update_dttm
            , operation_type, load_dttm, hash
            , valid_from_dttm, valid_to_dttm, deleted_flg, deleted_dttm)
        select  id
        	, status_nm
        	, create_dttm
        	, update_dttm
            , operation_type
            , load_dttm
            , hash
            , now() valid_from_dttm
            , to_timestamp('2999/12/31 23:59:59', 'yyyy/mm/dd hh24:mi:ss') valid_to_dttm
            , case operation_type
                when 'D' then 'Y'
                else 'N'
               end deleted_flg
            , case operation_type
                when 'D' then update_dttm
                else null
               end deleted_dttm
        from dwh_stage.tracking_products_information_src src; 
        return true;
    end
$function$
;

-- создание календаря
drop table if exists dwh_ods.dim_date;
create table dwh_ods.dim_date (
	date_key int not null,   
	date_actual date not null,  
	year int not null,   
	quarter int not null,  
	month int not null,   
	week int not null,   
	day_of_month int not null,
	day_of_week int not null,   
	is_weekday boolean not null,  
	is_holiday boolean not null,  
	fiscal_year int not null
);

-- заполнение календаря
create or replace function dwh_ods.load_dim_date()
returns boolean
language plpgsql
as $function$
declare
    v_rc int;
begin
	truncate table dwh_ods.dim_date;
insert into dwh_ods.dim_date (date_key, date_actual, year, quarter, month, week, day_of_month, day_of_week, is_weekday, is_holiday, fiscal_year) 
select to_char(load_dttm, 'yyyymmdd')::int date_key
	 , load_dttm::date as date_actual
     , (extract (year from load_dttm::date))::int as year
     , (extract (quarter from load_dttm::date))::int as quarter
     , (extract (month from load_dttm::date))::int as month
     , (extract (week from load_dttm::date))::int as week
     , (extract (day from load_dttm::date))::int day_of_month
     , (extract (dow from load_dttm::date))::int day_of_week
     , case when (extract (dow from load_dttm::date))::int
     in (1, 2, 3, 4, 5) then true else false end as is_weekday
     , case when (extract (dow from load_dttm::date))::int
     in (6, 0) then true else false end as is_holiday
     , extract (year from load_dttm::date) fiscal_year
    from dwh_ods.tracking_products_information_hist;
    get diagnostics v_rc = row_count;           
    raise notice '% rows inserted into dim_date', v_rc;
    return true;
end $function$
;

--создание актуального среза
drop table if exists report.tracking_products_information_actual;
create table report.tracking_products_information_actual (
	id int4 not null,
    status_nm text null,
    create_dttm timestamp null,
    update_dttm timestamp null,
    operation_type bpchar(1) not null,
    load_dttm timestamp not null,
    hash bytea null,
 	valid_from_dttm timestamp null,
    valid_to_dttm timestamp null
);

-- заполнение актуального среза
create or replace function report.load_tracking_products_information_actual()
returns boolean
language plpgsql
as $function$
begin
	truncate table report.tracking_products_information_actual;
 insert into report.tracking_products_information_actual (
            id, status_nm, create_dttm, update_dttm
            , operation_type, load_dttm, hash
            , valid_from_dttm, valid_to_dttm)
        select  id
        	, status_nm
        	, create_dttm
        	, update_dttm
            , operation_type
            , load_dttm
            , hash
            , valid_from_dttm
            , valid_to_dttm
        from dwh_ods.tracking_products_information_hist ods
        where (ods.valid_to_dttm > now() or ods.status_nm in ('stopped'))
       		  and operation_type != 'D';
        return true;
    end $function$
;

-- витрина устаревших данных
drop table if exists report.tracking_products_information_outdated;
create table report.tracking_products_information_outdated (
	id int4 not null,
    status_nm text null,
    create_dttm timestamp null,
    update_dttm timestamp null,
    operation_type bpchar(1) not null,
    load_dttm timestamp not null,
    hash bytea null,
 	valid_from_dttm timestamp null,
    valid_to_dttm timestamp null
);

-- заполнение витрины устаревших данных
create or replace function report.load_tracking_products_information_outdated()
returns boolean
language plpgsql
as $function$
begin
	truncate table report.tracking_products_information_outdated;
 	insert into report.tracking_products_information_outdated (
            id, status_nm, create_dttm, update_dttm
            , operation_type, load_dttm, hash
            , valid_from_dttm, valid_to_dttm)
        select  id
        	, status_nm
        	, create_dttm
        	, update_dttm
            , operation_type
            , load_dttm
            , hash
            , valid_from_dttm
            , valid_to_dttm
        from dwh_ods.tracking_products_information_hist ods
        where ods.valid_to_dttm < now() or operation_type = 'D';
        return true;
    end $function$
;

-- витрина записей, статус которых out of stock
drop table if exists report.tracking_products_information_out_of_stock;
create table report.tracking_products_information_out_of_stock (
	date_key int not null,  
	id int4 not null,
    status_nm text null,
    create_dttm timestamp null,
    update_dttm timestamp null,
    operation_type bpchar(1) not null,
    load_dttm timestamp not null,
    is_weekday boolean not null,  
	is_holiday boolean not null, 
    hash bytea null,
 	valid_from_dttm timestamp null,
    valid_to_dttm timestamp null
);

-- заполнение витрины записями, статус которых out of stock
create or replace function report.load_tracking_products_information_out_of_stock()
returns boolean
language plpgsql
as $function$
begin
	truncate table report.tracking_products_information_out_of_stock;
 insert into report.tracking_products_information_out_of_stock (
            date_key, id, status_nm, create_dttm, update_dttm
            , operation_type, load_dttm, is_weekday
            , is_holiday, hash, valid_from_dttm, valid_to_dttm)
        select distinct dd.date_key as date_key
       	, ods.id as id
       	, ods.status_nm as status_nm
       	, ods.create_dttm as create_dttm
        , ods.update_dttm as update_dttm
        , ods.operation_type as operation_type
        , ods.load_dttm as load_dttm
        , dd.is_weekday as is_weekday
        , dd.is_holiday as is_holiday
        , ods.hash as hash
        , ods.valid_from_dttm as valid_from_dttm
        , ods.valid_to_dttm as valid_to_dttm  
        from dwh_ods.dim_date dd
        inner join dwh_ods.tracking_products_information_hist ods on dd.date_key = to_char(ods.load_dttm, 'yyyymmdd')::int
        where ods.status_nm = 'out of stock';
        return true;
    end $function$
;

-- витрина с остановленными записями
drop table if exists report.tracking_products_information_stopped;
create table report.tracking_products_information_stopped (
	date_key int not null,  
	id int4 not null,
    status_nm text null,
    create_dttm timestamp null,
    update_dttm timestamp null,
    operation_type bpchar(1) not null,
    load_dttm timestamp not null,
    is_weekday boolean not null,  
	is_holiday boolean not null, 
    hash bytea null,
 	valid_from_dttm timestamp null,
    valid_to_dttm timestamp null
);

-- заполнение витрины с остановленными записями
create or replace function report.load_tracking_products_information_stopped()
returns boolean
language plpgsql
as $function$
begin
	truncate table report.tracking_products_information_stopped;
 	insert into report.tracking_products_information_stopped (
            date_key, id, status_nm, create_dttm, update_dttm
            , operation_type, load_dttm, is_weekday
            , is_holiday, hash, valid_from_dttm, valid_to_dttm)
        select distinct dd.date_key as date_key
       	, ods.id as id
       	, ods.status_nm as status_nm
       	, ods.create_dttm as create_dttm
        , ods.update_dttm as update_dttm
        , ods.operation_type as operation_type
        , ods.load_dttm as load_dttm
        , dd.is_weekday as is_weekday
        , dd.is_holiday as is_holiday
        , ods.hash as hash
        , ods.valid_from_dttm as valid_from_dttm
        , ods.valid_to_dttm as valid_to_dttm  
        from dwh_ods.dim_date dd
        inner join dwh_ods.tracking_products_information_hist ods on dd.date_key = to_char(ods.load_dttm, 'yyyymmdd')::int
        where ods.status_nm = 'stopped';
        return true;
    end $function$
;

-- витрина с удалёнными записями
drop table if exists report.tracking_products_information_deleted;
create table report.tracking_products_information_deleted (
	date_key int not null,  
	id int4 not null,
    status_nm text null,
    create_dttm timestamp null,
    update_dttm timestamp null,
    operation_type bpchar(1) not null,
    load_dttm timestamp not null,
    is_weekday boolean not null,  
	is_holiday boolean not null, 
    hash bytea null,
 	valid_from_dttm timestamp null,
    valid_to_dttm timestamp null
);

-- заполение витрины удалённых записей
create or replace function report.load_tracking_products_information_deleted()
returns boolean
language plpgsql
as $function$
begin
	truncate table report.tracking_products_information_deleted;
 	insert into report.tracking_products_information_deleted (
            date_key, id, status_nm, create_dttm, update_dttm
            , operation_type, load_dttm, is_weekday
            , is_holiday, hash, valid_from_dttm, valid_to_dttm)
        select distinct dd.date_key as date_key
       	, ods.id as id
       	, ods.status_nm as status_nm
       	, ods.create_dttm as create_dttm
        , ods.update_dttm as update_dttm
        , ods.operation_type as operation_type
        , ods.load_dttm as load_dttm
        , dd.is_weekday as is_weekday
        , dd.is_holiday as is_holiday
        , ods.hash as hash
        , ods.valid_from_dttm as valid_from_dttm
        , ods.valid_to_dttm as valid_to_dttm  
        from dwh_ods.dim_date dd
        inner join dwh_ods.tracking_products_information_hist ods on dd.date_key = to_char(ods.load_dttm, 'yyyymmdd')::int
        where ods.operation_type = 'D';
        return true;
    end $function$
;

-- права для airflow
grant  all privileges on all tables in schema oltp_src_system to airflow;
grant  all privileges on all tables in schema oltp_cdc_src_system to airflow;
grant  all privileges on all tables in schema dwh_stage to airflow;
grant  all privileges on all tables in schema dwh_ods to airflow;
grant  all privileges on all tables in schema report to airflow;

grant all privileges on function oltp_src_system.delete_existed_tracking to airflow;
grant all privileges on function oltp_src_system.update_existed_tracking to airflow;
grant all privileges on function oltp_src_system.create_tracking to airflow;
grant all privileges on function dwh_stage.load_from_cdc_tracking_products_information to airflow;
grant all privileges on function dwh_ods.load_from_tracking_products_information_src to airflow;
grant all privileges on function dwh_ods.load_dim_date to airflow;
grant all privileges on function report.load_tracking_products_information_actual to airflow;
grant all privileges on function report.load_tracking_products_information_outdated to airflow;
grant all privileges on function report.load_tracking_products_information_out_of_stock to airflow;
grant all privileges on function report.load_tracking_products_information_stopped to airflow;
grant all privileges on function report.load_tracking_products_information_deleted to airflow;

grant all privileges on schema oltp_src_system to airflow;
grant all privileges on schema oltp_cdc_src_system to airflow;
grant all privileges on schema dwh_stage to airflow;
grant all privileges on schema dwh_ods to airflow;
grant all privileges on schema report to airflow;

-- удаление данных из таблиц
truncate table oltp_src_system.tracking_products;
truncate table oltp_cdc_src_system.cdc_tracking_products_information;
truncate table dwh_stage.tracking_products_information_src;
truncate table dwh_ods.tracking_products_information_hist;
truncate table dwh_ods.dim_date;
truncate table report.tracking_products_information_actual;
truncate table report.tracking_products_information_outdated;
truncate table report.tracking_products_information_out_of_stock;
truncate table report.tracking_products_information_stopped;
truncate table report.tracking_products_information_deleted;

/* запуск всех функций */

-- создание записей
select * from oltp_src_system.create_tracking();
select * from dwh_stage.load_from_cdc_tracking_products_information();
select * from dwh_ods.load_from_tracking_products_information_src();
select * from dwh_ods.load_dim_date();
select * from report.load_tracking_products_information_actual();
select * from report.load_tracking_products_information_outdated();
select * from report.load_tracking_products_information_out_of_stock();
select * from report.load_tracking_products_information_stopped();
select * from report.load_tracking_products_information_deleted();

-- обновление записей
select * from oltp_src_system.update_existed_tracking();
select * from dwh_stage.load_from_cdc_tracking_products_information();
select * from dwh_ods.load_from_tracking_products_information_src();
select * from dwh_ods.load_dim_date();
select * from report.load_tracking_products_information_actual();
select * from report.load_tracking_products_information_outdated();
select * from report.load_tracking_products_information_out_of_stock();
select * from report.load_tracking_products_information_stopped();
select * from report.load_tracking_products_information_deleted();

-- удаление записей
select * from oltp_src_system.delete_existed_tracking();
select * from dwh_stage.load_from_cdc_tracking_products_information();
select * from dwh_ods.load_from_tracking_products_information_src();
select * from dwh_ods.load_dim_date();
select * from report.load_tracking_products_information_actual();
select * from report.load_tracking_products_information_outdated();
select * from report.load_tracking_products_information_out_of_stock();
select * from report.load_tracking_products_information_stopped();
select * from report.load_tracking_products_information_deleted();




