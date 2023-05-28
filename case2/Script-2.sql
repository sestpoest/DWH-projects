-- Таблица физ-лиц системы MDM
drop table if exists dfct_phone_0010;
create table dfct_phone_0010 as
	select
	c_.counterparty_rk,
	c_.effective_from_date,
	c_.effective_to_date,
	c_.counterparty_type_cd,
	c_.src_cd,
	/* Значения ценности флага для приоритизации */
	case when c_.src_cd = 'MDMP' then '4000' when c_.src_cd = 'WAYN' then '3000'
		 when c_.src_cd = 'RTLL' then '2000' when c_.src_cd = 'RTLS' then '1000'
		 when c_.src_cd = 'CFTB' then '0' end as src_cd_value
from counterparty c_
inner join dict_counterparty_type_cd dctc_ on c_.counterparty_type_cd = dctc_.counterparty_type_cd /* связь со справочником */
	where counterparty_type_desc = 'физическое лицо' 
	and c_.src_cd = 'MDMP'
	and dctc_.src_cd = 'MDMP';


-- Телефоны физ-лиц системы MDM
drop table if exists dfct_phone_0020;
create table dfct_phone_0020 as
	select
	cc_.counterparty_rk,
	cc_.effective_from_date,
	cc_.effective_to_date,
	cc_.contact_desc,
	cc_.contact_type_cd,
	cc_.contact_quality_code,
	cc_.trust_system_flg,
	cc_.src_cd,
	src_cd_value
from counterparty_contact cc_ 
inner join dfct_phone_0010 src_ on cc_.counterparty_rk = src_.counterparty_rk /* связь физ-лиц и таблицы телефонов */
and src_.effective_to_date >= now() --cast(&p_load_dt as date) /* параметр даты запуска расчёта, по умолчанию соответствует now() */
and src_.effective_from_date < now() --cast(&p_load_dt as date)
	where cc_.src_cd = 'MDMP'
	and src_.src_cd = 'MDMP';


--Соответствие других источников к MDM
drop table if exists dfct_phone_0030;
create table dfct_phone_0030 as
select
	src_.counterparty_rk,
	cc_.effective_from_date,
	cc_.effective_to_date,
	cc_.contact_desc,
	cc_.contact_type_cd,
	cc_.contact_quality_code,
	cc_.trust_system_flg,
	cc_.src_cd as src_cd_original, /* Исходная система */
	cxuc_.src_cd,    
	/* Значения ценности флага для приоритизации */
	case when cc_.src_cd = 'MDMP' then '4000' when cc_.src_cd = 'WAYN' then '3000'
	when cc_.src_cd = 'RTLL' then '2000' when cc_.src_cd = 'RTLS' then '1000'
	when cc_.src_cd = 'CFTB' then '0' end as src_cd_value
from counterparty_contact cc_ 
inner join counterparty_x_uniq_counterparty cxuc_ on cc_.counterparty_rk = cxuc_.counterparty_rk /* связь контактов и моста */
  	and cxuc_.effective_to_date >= now()
  	and cxuc_.effective_from_date < now() 
inner join dfct_phone_0010 src_ on cxuc_.uniq_counterparty_rk = src_.counterparty_rk /* связь моста и физ-лиц */
  	and src_.effective_to_date >= now()
  	and src_.effective_from_date < now() 
	where cc_.src_cd in ('RTLL', 'RTLS', 'CFTB', 'WAYN');


--Объединение телефонов-клиентов MDM с другими системами в общую MDM  
drop table if exists dfct_phone_0040;
create table dfct_phone_0040 as
	select *
from dfct_phone_0020
union
	select counterparty_rk, effective_from_date,
		   effective_to_date, contact_desc,
		   contact_type_cd, contact_quality_code,
		   trust_system_flg, src_cd,
		   src_cd_value
from dfct_phone_0030;
	
	
-- Определение флагов
drop table if exists dfct_phone_0050;
create table dfct_phone_0050 as
	select
	src_.counterparty_rk,
	src_.contact_type_cd,
	src_.effective_from_date,
	src_.effective_to_date,
	src_.contact_desc,
	src_.contact_quality_code,
	case when src_.contact_type_cd ='NotificPhone' then true else false end as notification_flg,
	case when src_.contact_type_cd ='ATMPhone' then true else false end as atm_flg,
	src_.trust_system_flg,
	/* Если номер телефона встречается более 1 раза, то ставим флаг дубликата */
	case when  (count(contact_desc) over (partition by src_.contact_desc, src_.effective_to_date) > 1) 
		 then true else false end as duplication_flg,
	src_.src_cd_value
	from dfct_phone_0040 src_;
	
	
-- Определение ценности каждого флага для приоритизации 
drop table if exists dfct_phone_0060;
create table dfct_phone_0060 as
	select *,
	case when src_.duplication_flg = false then '1000000' else '0' end as duplication_flg_value,
	case when src_.trust_system_flg = true then '100000' else '0' end as trust_system_flg_value,
	case when src_.contact_quality_code = 'GOOD' or src_.contact_quality_code = 'GOOD;;'
		 or src_.contact_quality_code = 'GOOD_REPLACED_CODE' then '10000' else '0' end as contact_quality_code_value,
	case when src_.contact_type_cd = 'NotificPhone' then '400' when src_.contact_type_cd = 'ATMPhone' then '300'
		 when src_.contact_type_cd = 'MobilePersonalPhone' then '200' when src_.contact_type_cd = 'MobileWorkNumber' then '100'
		 when src_.contact_type_cd = 'HomePhone' then '0' end as contact_type_cd_value,
	/* определение более свежей записи */
	case when src_.effective_from_date = max(src_.effective_from_date) over (partition by src_.contact_desc)
  then '10' else '0' end as date_value_1,
  case when src_.effective_from_date = max(src_.effective_from_date) over (partition by src_.counterparty_rk)
  then '10' else '0' end as date_value_2,
  case when src_.effective_to_date > now() then '10000000' else '0' end as actual_flg
from dfct_phone_0050 src_;


-- Расчёт приоритетов
drop table if exists dfct_phone_0070;
create table dfct_phone_0070 as
	select *, 
	/* складываем ценности всех флагов каждой записи в столбец value, значения которого будем сравнивать для определения main флагов */
	/* value_1 для флага main_dup_flg */
	cast(src_cd_value as int) + cast(trust_system_flg_value as int)
	+ cast(contact_quality_code_value as int) + cast(contact_type_cd_value as int)
	+ cast(duplication_flg_value as int) + cast(date_value_1 as int)
	+ cast (actual_flg as int) as value_1,
	/* value_2 для флага main_phone_flg */
	cast(src_cd_value as int) + cast(trust_system_flg_value as int)
	+ cast(contact_quality_code_value as int) + cast(contact_type_cd_value as int)
	+ cast(duplication_flg_value as int) + cast(date_value_2 as int)
	+ cast (actual_flg as int) as value_2
from dfct_phone_0060 src_;




-- Определение main флагов 
drop table if exists dfct_phone_0080;
create table dfct_phone_0080 as
	select 
	src_.counterparty_rk,
	src_.contact_type_cd,
	src_.effective_from_date,
	src_.effective_to_date,
	src_.contact_desc,
	src_.notification_flg,
	src_.atm_flg,
	src_.trust_system_flg,
	src_.duplication_flg,
	/* условие для main_dup_flg */
	case when src_.duplication_flg = true and src_.value_1 = max(src_.value_1) over (partition by src_.contact_desc)
   		 then true
   		 when src_.duplication_flg = true and src_.value_1 <> max(src_.value_1) over (partition by src_.contact_desc)
   		 then false
   		 when src_.duplication_flg = false 
   		 then true
   		 end as main_dup_flg,
   		 /* условие для main_phone_flg */
	case when src_.value_2 = max(src_.value_2) over (partition by src_.counterparty_rk)
   		 then true else false end as main_phone_flg
	from dfct_phone_0070 src_;


--Схлопывание истории
drop table if exists dfct_phone_0090;
create table dfct_phone_0090 as
	select *,
	/*
	1. для каждого атрибута, по которому мы отслеживаем историю, мы сравниваем значение из текущей строки со значением из предыдущей строки
	2. если значения совпадают или оба значения NULL, считаем, что это одинаковые версии, и их нужно схлопнуть
	3. отдельно проверяем первую строку - её нужно явно включить в выборку
	*/
   	case when
			(bool_or(src_.notification_flg) over (partition by src_.counterparty_rk, contact_type_cd, src_.contact_desc order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) = src_.notification_flg
				or (bool_or(src_.notification_flg) over (partition by src_.counterparty_rk, contact_type_cd, src_.contact_desc order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) is null and src_.notification_flg  is null))
			and (bool_or(src_.atm_flg) over (partition by src_.counterparty_rk, contact_type_cd, src_.contact_desc order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) = src_.atm_flg
				or (bool_or(src_.atm_flg) over (partition by src_.counterparty_rk, contact_type_cd, src_.contact_desc order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) is null and src_.atm_flg  is null))
			and (bool_or(src_.trust_system_flg) over (partition by src_.counterparty_rk, contact_type_cd, src_.contact_desc order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) = src_.trust_system_flg
				or (bool_or(src_.trust_system_flg) over (partition by src_.counterparty_rk, contact_type_cd, src_.contact_desc order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) is null and src_.trust_system_flg  is null))
			and (bool_or(src_.duplication_flg) over (partition by src_.counterparty_rk, contact_type_cd, src_.contact_desc order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) = src_.duplication_flg
				or (bool_or(src_.duplication_flg) over (partition by src_.counterparty_rk, contact_type_cd, src_.contact_desc order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) is null and src_.duplication_flg  is null))
			and  (bool_or(src_.main_dup_flg) over (partition by src_.counterparty_rk, contact_type_cd, src_.contact_desc order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) = src_.main_dup_flg
				or (bool_or(src_.main_dup_flg) over (partition by src_.counterparty_rk, contact_type_cd, src_.contact_desc order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) is null and src_.main_dup_flg  is null))
			and (bool_or(src_.main_phone_flg) over (partition by src_.counterparty_rk, contact_type_cd, src_.contact_desc order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) = src_.main_phone_flg
				or (bool_or(src_.main_phone_flg) over (partition by src_.counterparty_rk, contact_type_cd, src_.contact_desc order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) is null and src_.main_phone_flg  is null))
			and row_number() over (partition by src_.counterparty_rk, contact_type_cd, src_.contact_desc order by src_.effective_from_date) > 1
		then 0 
		else 1 
	end as flg 
	from dfct_phone_0080 src_;


-- Итоговая таблица
delete from dfct_phone;
insert into dfct_phone 
	select 
	src_.counterparty_rk as mdm_customer_rk,
	src_.contact_type_cd as phone_type_cd,
	src_.effective_from_date as business_start_dt,
	src_.effective_to_date as business_end_dt,
	src_.contact_desc as phone_num,
	src_.notification_flg,
	src_.atm_flg,
	src_.trust_system_flg,
	src_.duplication_flg,
	src_.main_dup_flg,
	src_.main_phone_flg
from dfct_phone_0090 src_
where src_.flg = 1;
