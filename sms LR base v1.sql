

set tez.queue.name = rocai_7;
set hive.cli.print.header=true;
set hive.cli.print.current.db=true;
set hive.groupby.orderby.position.alias=false;
set hive.resultset.use.unique.column.names=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
USE rocai_analysis2;





------------------------------------sms liner Linear Regression model-----------------------
-------------------------base------------------------------


DROP TABLE rocai_analysis2.20230927_01_H_sms_lrm_base_jul_v1;

CREATE TABLE rocai_analysis2.20230927_01_H_sms_lrm_base_jul_v1 stored as orc as
select 
bundle_code,
msisdn
from rocai_analysis2.prof_prod_msisdn_daily
where 
substr(part_date,1,6)='202307'
and lower(service_type)='sms'
and bundle_code in ('58','59','60','61','63','342')
AND (txn_cnt > 0 or txn_amt > 0)
and LENGTH(msisdn) = 12
and substr(msisdn, 1, 5) in ('26377', '26378')
group by 
bundle_code,
msisdn;

DROP TABLE rocai_analysis2.20230927_01_H_sms_lrm_base_jul_v2;

CREATE TABLE rocai_analysis2.20230927_01_H_sms_lrm_base_jul_v2 stored as orc as
select 
a.*,
case when b.msisdn is null then 'zwl' else 'ZWL+usd' end as flag
from rocai_analysis2.20230927_01_H_sms_lrm_base_jul_v1 a 
left join
(
select 
msisdn
from rocai_analysis2.prof_prod_msisdn_daily
where 
substr(part_date,1,6)='202307'
and bundle_code in  ('11573','11577','11578','11883','12971','13170','13171','12970','11874','11884','11881')
AND (txn_cnt > 0 or txn_amt > 0)
and LENGTH(msisdn) = 12
and substr(msisdn, 1, 5) in ('26377', '26378')
group by msisdn)b
on a.msisdn=b.msisdn;



DROP TABLE rocai_analysis2.20230927_01_H_sms_lrm_base_jul_v3;

CREATE TABLE rocai_analysis2.20230927_01_H_sms_lrm_base_jul_v3 stored as orc as
select 
* from 
rocai_analysis2.20230927_01_H_sms_lrm_base_jul_v2
where flag='zwl';


select 
bundle_code,
flag,
count(distinct msisdn) as customers
FROM
rocai_analysis2.20230927_01_H_sms_lrm_base_jul_v3
group by 
bundle_code,
flag;





DROP TABLE rocai_analysis2.20230927_01_H_sms_lrm_base_aug_v1;

CREATE TABLE rocai_analysis2.20230927_01_H_sms_lrm_base_aug_v1 stored as orc as
select 
bundle_code,
msisdn
from rocai_analysis2.prof_prod_msisdn_daily
where 
substr(part_date,1,6)='202308'
and lower(service_type)='sms'
and bundle_code in ('58','59','60','61','63','342')
AND (txn_cnt > 0 or txn_amt > 0)
and LENGTH(msisdn) = 12
and substr(msisdn, 1, 5) in ('26377', '26378')
group by 
bundle_code,
msisdn;

DROP TABLE rocai_analysis2.20230927_01_H_sms_lrm_base_aug_v2;

CREATE TABLE rocai_analysis2.20230927_01_H_sms_lrm_base_aug_v2 stored as orc as
select 
a.*,
case when b.msisdn is null then 'zwl' else 'ZWL+usd' end as flag
from rocai_analysis2.20230927_01_H_sms_lrm_base_aug_v1 a 
left join
(
select 
calling_nbr as msisdn
from rocai_analysis3.prof_usd_msisdn_daily_final_cco ----12.0.2
where 
substr(part_date,1,6)='202308'
and (nvl(sms_acct_main_balance_chg_usd,0) + nvl(sms_acct_credit_balance_chg_usd,0) + nvl(sms_bundle_revenue_usd,0)) >0 
and LENGTH(calling_nbr) = 12
and substr(calling_nbr, 1, 5) in ('26377', '26378')
group by calling_nbr)b
on a.msisdn=b.msisdn;


select count(1) , count(distinct msisdn) as cc from rocai_analysis2.20230927_01_H_sms_lrm_base_jul_v3;

select count(1) , count(distinct msisdn) as cc from rocai_analysis2.20230927_01_H_sms_lrm_base_aug_jul_basewchkkk;

select count(1) , count(distinct msisdn) as cc from rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1;


DROP TABLE rocai_analysis2.20230927_01_H_sms_lrm_base_aug_jul_basewchkkk;

CREATE TABLE rocai_analysis2.20230927_01_H_sms_lrm_base_aug_jul_basewchkkk stored as orc as
select 
a.msisdn,a.bundle_code,nvl(b.flag,'NRGS') as aug_chk_flag
from rocai_analysis2.20230927_01_H_sms_lrm_base_jul_v3 a -----only july zwl base-----------
left join
rocai_analysis2.20230927_01_H_sms_lrm_base_aug_v2 b 
on a.msisdn=b.msisdn;

--483528 customer common base

DROP TABLE rocai_analysis2.20230927_01_H_sms_lrm_base_aug_jul_basewchkkk_summ;

CREATE TABLE rocai_analysis2.20230927_01_H_sms_lrm_base_aug_jul_basewchkkk_summ stored as orc as
select 
bundle_code,
aug_chk_flag,
count(distinct msisdn) as customers
FROM
rocai_analysis2.20230927_01_H_sms_lrm_base_aug_jul_basewchkkk
group by 
bundle_code,
aug_chk_flag;
=================================================================model base prep========================================================================================================

---0 zwl customer
---1 usd customer
--483528 customer common base


DROP TABLE rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1;

CREATE TABLE rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1 stored as orc as
select 
bundle_code,
msisdn,
case when aug_chk_flag='zwl' or aug_chk_flag='NRGS' then 0 else 1 end as flag_final
from 
rocai_analysis2.20230927_01_H_sms_lrm_base_aug_jul_basewchkkk
group by 
bundle_code,
msisdn,
case when aug_chk_flag='zwl' or aug_chk_flag='NRGS' then 0 else 1 end ;



select 
bundle_code,
flag_final,
count(distinct msisdn) as customers
FROM
rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1
group by 
bundle_code,
flag_final;

==========================================extra columns====================================

/*DROP TABLE rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1_extra_cols;

CREATE TABLE rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1_extra_cols stored as orc as
select 
a.msidn,
a.bundle_code,
a.flag_final,
nvl(homing_bsc,'Unmapped') as region,
sms_distinct_days,
overall_cust_segment,
sum(nvl(data_volume_gb,0)) as data_usage,
sum(nvl(voice_usage_mins,0)) as voice_usage,
sum(nvl(sms_total_msgs,0)) as sms_usage,
sum(nvl(sms_acct_main_balance_chg,0) + nvl(sms_acct_credit_balance_chg,0)  + nvl(sms_bundle_revenue,0) ) as sms_revenue,
sum(nvl(total_revenue,0)) as total_revenue
from 
select 
msisdn,
bundle_code,
flag_final
FROM
rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1 a 
left join 
select * from 
rocai_analysis3.cust_360_dashboard_backend_device_cco where part_month='202308' b
on a.msisdn=b.calling_nbr;*/

DROP TABLE IF EXISTS rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1_extra_cols;
CREATE TABLE rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1_extra_cols STORED AS ORC AS
SELECT
a.msisdn,
a.bundle_code,
a.flag_final,
NVL(b.homing_bsc, 'Unmapped') AS region,
b.sms_distinct_days,
b.overall_cust_segment,
SUM(NVL(b.data_volume_gb, 0)) AS data_usage,
SUM(NVL(b.voice_usage_mins, 0)) AS voice_usage,
SUM(NVL(b.sms_total_msgs, 0)) AS sms_usage,
SUM(NVL(b.sms_acct_main_balance_chg, 0) + NVL(sms_acct_credit_balance_chg, 0) + NVL(sms_bundle_revenue, 0)) AS sms_revenue,
SUM(NVL(b.total_revenue, 0)) AS total_revenue
FROM rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1 a
LEFT JOIN rocai_analysis3.cust_360_dashboard_backend_device_cco b
ON a.msisdn = b.calling_nbr
WHERE b.part_month = '202308'
group by 
a.msisdn,
a.bundle_code,
a.flag_final,
NVL(homing_bsc, 'Unmapped') AS region,
sms_distinct_days,
overall_cust_segment;



--------------------------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS rocai_analysis2.tmp_cltv_base_final;

CREATE TABLE rocai_analysis2.tmp_cltv_base_final STORED AS ORC AS
SELECT
a.calling_nbr,
a.last_active_date,
a.total_revenue_segment,
b.gender,
c.device_type,
c.dual_sim,
sum(a.total_revenue_usd) as total_revenue_usd,
sum(a.total_voice_revenue_usd) as total_voice_revenue_usd,
sum(a.total_data_revenue_usd) as total_data_revenue_usd,
sum(a.total_sms_revenue_usd) as total_sms_revenue_usd,
sum(a.days_of_usage) as days_of_usage,
ROUND(DATEDIFF(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('20230713', 'yyyyMMdd'))), TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(b.dob, 'yyyyMMdd')))) / 365) AS age,
ROUND(DATEDIFF(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('20230713', 'yyyyMMdd'))), TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(b.activation_date, 'yyyyMMdd')))) / 365) AS Aon
FROM
(
SELECT
calling_nbr,
days_of_usage,
total_revenue_segment,
last_active_date,
total_revenue_usd,
total_voice_revenue_usd,
total_data_revenue_usd,
total_sms_revenue_usd

FROM
rocai_analysis2.tmp_cltv_base_v1_conv_usd
) a
LEFT JOIN rocai_analysis2.prof_kyc_msisdn_latest b ON a.calling_nbr = b.calling_nbr_norm
LEFT JOIN rocai_analysis2.prof_ggsn_msc_imei_rolling5months c ON a.calling_nbr = c.calling_nbr
GROUP BY
a.calling_nbr,
a.total_revenue_segment,
a.last_active_date,
b.dob,
b.activation_date,
b.gender,
c.device_type,
c.dual_sim;