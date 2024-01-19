

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
=================================================================model base prep===========================================================================================================

---0 zwl customer
---1 usd customer
--483528 customer common base

----464994


DROP TABLE rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1;

CREATE TABLE rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1 stored as orc as
select 
msisdn,
case when bundle_code='58' then 1 else 0 end as sms_5_cust,
case when bundle_code='59' then 1 else 0 end as sms_10_cust,
case when bundle_code='60' then 1 else 0 end as sms_25_cust,
case when bundle_code='61' then 1 else 0 end as sms_125_cust,
case when bundle_code='63' then 1 else 0 end as sms_200_cust,
case when bundle_code='342' then 1 else 0 end as sms_300_cust,
case when aug_chk_flag='zwl' or aug_chk_flag='NRGS' then 0 else 1 end as flag_final
from 
rocai_analysis2.20230927_01_H_sms_lrm_base_aug_jul_basewchkkk
group by 
msisdn,
case when bundle_code='58' then 1 else 0 end ,
case when bundle_code='59' then 1 else 0 end ,
case when bundle_code='60' then 1 else 0 end ,
case when bundle_code='61' then 1 else 0 end ,
case when bundle_code='63' then 1 else 0 end ,
case when bundle_code='342' then 1 else 0 end ,
case when aug_chk_flag='zwl' or aug_chk_flag='NRGS' then 0 else 1 end ;


============================================================================================================================
DROP TABLE rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1_up;

CREATE TABLE rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1_up stored as orc as
select msisdn,
max(sms_5_cust) as sms_5_cust,
max(sms_10_cust) as sms_10_cust,
max(sms_25_cust) as sms_25_cust,
max(sms_125_cust)  as sms_125_cust,
max(sms_200_cust) as sms_200_cust,
max(sms_300_cust) as sms_300_cust,
max(flag_final) as flag_final
from 
rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1
group by 
msisdn;

select count(1) , count(distinct msisdn) as cc from rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1_up;


/*----------------------------------------------------------------
sms_5_cust,
sms_10_cust,
sms_25_cust,
sms_125_cust,
sms_200_cust,
sms_300_cust,

select 
sms_5_cust,
sms_10_cust,
sms_25_cust,
sms_125_cust,
sms_200_cust,
sms_300_cust,
flag_final,
count(distinct msisdn) as customers
FROM
rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1
group by 
sms_5_cust,
sms_10_cust,
sms_25_cust,
sms_125_cust,
sms_200_cust,
sms_300_cust,
flag_final;


select count(1) , count(distinct msisdn) as cc from rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1;*/

==========================================extra columns====================================


DROP TABLE IF EXISTS rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1_extra_cols;
CREATE TABLE rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1_extra_cols STORED AS ORC AS
SELECT
a.msisdn,
a.sms_5_cust,
a.sms_10_cust,
a.sms_25_cust,
a.sms_125_cust,
a.sms_200_cust,
a.sms_300_cust,
a.flag_final,
NVL(b.homing_bsc, 'Unmapped') AS region,
NVL(b.overall_cust_segment, 'Unmapped') AS overall_cust_segment,
b.sms_distinct_days,
SUM(NVL(b.data_volume_gb, 0)) AS data_usage,
SUM(NVL(b.voice_usage_mins, 0)) AS voice_usage,
SUM(NVL(b.sms_total_msgs, 0)) AS sms_usage,
SUM(NVL(b.sms_acct_main_balance_chg, 0) + NVL(sms_acct_credit_balance_chg, 0) + NVL(sms_bundle_revenue, 0)) AS sms_revenue,
SUM(NVL(b.total_revenue, 0)) AS total_revenue
FROM rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1_up a
LEFT JOIN rocai_analysis3.cust_360_dashboard_backend_device_cco b
ON a.msisdn = b.calling_nbr
WHERE b.part_month = '202307' and lower(b.plan_code) like '%prepaid%'
group by 
a.msisdn,
a.sms_5_cust,
a.sms_10_cust,
a.sms_25_cust,
a.sms_125_cust,
a.sms_200_cust,
a.sms_300_cust,
a.flag_final,
NVL(b.homing_bsc, 'Unmapped'),
b.sms_distinct_days,
NVL(b.overall_cust_segment, 'Unmapped');

select count(1) as cnt, count(distinct msisdn) as cccnt from rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1_extra_cols;


--------------------------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS rocai_analysis2.tmp_sms_rf_model_final_base;

CREATE TABLE rocai_analysis2.tmp_sms_rf_model_final_base STORED AS ORC AS
SELECT
a.msisdn,
a.sms_5_cust,
a.sms_10_cust,
a.sms_25_cust,
a.sms_125_cust,
a.sms_200_cust,
a.sms_300_cust,
a.flag_final,
a.region,
a.sms_distinct_days,
a.overall_cust_segment,
sum(a.data_usage) as data_usage,
sum(a.voice_usage) as voice_usage,
sum(a.sms_usage) as sms_usage,
sum(a.sms_revenue) as sms_revenue,
sum(a.total_revenue) as total_revenue,

NVL(b.gender, 'Unmapped') AS gender,
NVL(c.device_type, 'Unmapped') AS device_type,
NVL(c.dual_sim, 'Unmapped') AS dual_sim,
ROUND(DATEDIFF(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('20231004', 'yyyyMMdd'))), TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(b.dob, 'yyyyMMdd')))) / 365) AS age,
ROUND(DATEDIFF(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('20231004', 'yyyyMMdd'))), TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(b.activation_date, 'yyyyMMdd')))) / 365) AS Aon
FROM
(
SELECT
msisdn,
sms_5_cust,
sms_10_cust,
sms_25_cust,
sms_125_cust,
sms_200_cust,
sms_300_cust,
flag_final,
region,
sms_distinct_days,
overall_cust_segment,
data_usage,
voice_usage,
sms_usage,
sms_revenue,
total_revenue
FROM
rocai_analysis2.20230927_01_H_sms_lrm_model_base_v1_extra_cols
) a
LEFT JOIN rocai_analysis2.prof_kyc_msisdn_latest b ON a.msisdn = b.calling_nbr_norm
LEFT JOIN rocai_analysis2.prof_ggsn_msc_imei_rolling5months c ON a.msisdn = c.calling_nbr
GROUP BY
a.msisdn,
a.sms_5_cust,
a.sms_10_cust,
a.sms_25_cust,
a.sms_125_cust,
a.sms_200_cust,
a.sms_300_cust,
a.flag_final,
a.region,
a.sms_distinct_days,
a.overall_cust_segment,
b.dob,
b.activation_date,
NVL(b.gender, 'Unmapped'),
NVL(c.device_type, 'Unmapped'),
NVL(c.dual_sim, 'Unmapped');



select count(1) as cnt, count(distinct msisdn) as cccnt from rocai_analysis2.tmp_sms_rf_model_final_base;


select 
region,
flag_final,
count(distinct msisdn) as customer
from 
rocai_analysis2.tmp_sms_rf_model_final_base
group by 
region,
flag_final;




hive -e "set tez.queue.name=rocai_7;
set hive.cli.print.header=true;
set hive.cli.print.current.db=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET yarn.app.mapreduce.am.log.level=INFO;
SET mapreduce.map.log.level=INFO;
set hive.resultset.use.unique.column.names=false;
SET mapreduce.reduce.log.level=INFO;
select * from rocai_analysis2.tmp_sms_rf_model_final_base;" | sed 's/[\t]/,/g' > /landing_dir/rocai/subex_working_area/analyst/Hari_Prakash/20230929_tmp_sms_rf_model_final_base.csv
 
cd /landing_dir/rocai/subex_working_area/analyst/Hari_Prakash
gzip 20230929_tmp_sms_rf_model_final_base.csv



























------------------------------------------------------------------------ref-----------------------------------
https://www.kaggle.com/code/prashant111/random-forest-classifier-feature-importance vvgood one

https://www.kaggle.com/code/prashant111/random-forest-classifier-feature-importance




select 
substr(part_date,1,6) as pm,
count(distinct msisdn) as customers,
sum(nvl(vassdp_crbt_rev,0)) as revenue
from rocai_analysis2.prof_new_business_msisdn_daily_comp_bundle_inclusive
where substr(part_date,1,6)>='202307'
and (nvl(vassdp_crbt_cnt,0)>0 or nvl(vassdp_crbt_rev,0)>0)
and LENGTH(msisdn) = 12
and substr(msisdn, 1, 5) in ('26377', '26378')
group by 
substr(part_date,1,6)
;

select 
part_date,
count(distinct msisdn) as customers,
sum(nvl(vassdp_crbt_rev,0)) as revenue
from rocai_analysis2.prof_new_business_msisdn_daily_comp_bundle_inclusive
where part_date>='20230815'
and (nvl(vassdp_crbt_cnt,0)>0 or nvl(vassdp_crbt_rev,0)>0)
and LENGTH(msisdn) = 12
and substr(msisdn, 1, 5) in ('26377', '26378')
group by 
part_date;


DROP TABLE IF EXISTS rocai_analysis2.tmp_sms_rf_model_final_base;

CREATE TABLE rocai_analysis2.tmp_sms_rf_model_final_base STORED AS ORC AS
select 
count(distinct msisdn) as cc
from rocai_analysis2.prof_new_business_msisdn_daily_comp_bundle_inclusive
where part_date>='20230827' and part_date<='20230927'
and (nvl(vassdp_crbt_cnt,0)>0 or nvl(vassdp_crbt_rev,0)>0)
and LENGTH(msisdn) = 12
and substr(msisdn, 1, 5) in ('26377', '26378')
;


/*

sms_revenue           
sms_usage             
total_revenue         
sms_distinct_days     
voice_usage           
msisdn                
data_usage            
age                   
aon                   
region_2              
dual_sim_1            
dual_sim_2            
overall_cust_segment_2
gender_2              
region_7              
gender_1






region_1               
region_6               
device_type_2          
region_5               
sms_5_cust             
region_8               
device_type_1          
region_3               
sms_10_cust            
overall_cust_segment_1 
sms_125_cust           
region_4               
sms_25_cust            
region_9               
sms_200_cust           
dual_sim_3             
device_type_3          
overall_cust_segment_3 
device_type_4          
device_type_5          
sms_300_cust           
overall_cust_segment_4 
device_type_10         
device_type_7          
device_type_9          
device_type_8          
device_type_12         
device_type_13         
device_type_11         
device_type_6          
device_type_14         

sms_5_cust
sms_10_cust
sms_25_cust
sms_125_cust
sms_200_cust
sms_300_cust
flag_final
region
sms_distinct_days
overall_cust_segment
data_usage
voice_usage
sms_usage
sms_revenue
total_revenue
gender
device_type
dual_sim
age
aon


sms_5_cust, sms_10_cust, sms_25_cust, sms_125_cust, sms_200_cust, sms_300_cust:
These variables likely represent the number of SMS (text messages) sent by a customer in different categories. For example, sms_5_cust might represent the number of SMS sent by a customer within 5 days, and similarly for the other variables with different time frames.

flag_final: This variable could be a binary flag indicating some specific condition or event. For example, it might represent whether a customer has completed a particular action or met a certain criteria (1 for yes, 0 for no).

region: This variable may represent the geographic region or location associated with a customer. It could be a categorical variable specifying where the customer is located.

sms_distinct_days: This variable may represent the number of distinct days on which a customer sent SMS messages. It provides information about the spread of SMS activity over time.

overall_cust_segment: This could be a categorical variable that classifies customers into different segments or groups based on certain criteria. It's used for customer segmentation.

data_usage: Data usage typically represents the amount of mobile data consumed by a customer, often measured in megabytes (MB) or gigabytes (GB).

voice_usage: Voice usage represents the amount of voice calls made by a customer, typically measured in minutes.

sms_usage: SMS usage represents the total number of SMS messages sent by a customer, regardless of the time frame.

sms_revenue: This variable may represent the revenue generated by the telecom provider from SMS services used by the customer.

total_revenue: Total revenue could represent the overall revenue generated by the telecom provider from a customer, including all services (voice, SMS, data, etc.).

gender: Gender is a categorical variable that specifies the gender of the customer (e.g., Male, Female).

device_type: Device type may represent the type of mobile device used by the customer (e.g., smartphone, feature phone).

dual_sim: Dual SIM is a binary variable that indicates whether the customer's mobile device has dual SIM card slots (1 for yes, 0 for no).

age: Age represents the age of the customer. It's a continuous variable indicating the customer's age in years.

aon (Age on Network): AON represents the length of time a customer has been using the network or telecom services provided by the company. It's often measured in months or years.



Diagonal Values (1.0): The diagonal values represent the correlation of each variable with itself, which is always 1.0 since a variable perfectly correlates with itself.

Positive Correlations:

    sms_10_cust and sms_25_cust have a moderate positive correlation of 0.2257.
    sms_125_cust and data_usage have a relatively strong positive correlation of 0.192.
    sms_distinct_days and total_revenue have a strong positive correlation of 0.644.

Negative Correlations:

    sms_5_cust and sms_10_cust have a strong negative correlation of -0.6024.
    sms_200_cust and sms_revenue have a moderate negative correlation of -0.1755.
    age and aon (Age on Network) have a strong negative correlation of -0.5024, which suggests that as the age on the network increases, the age of the user tends to decrease.

Weak Correlations: There are several weak correlations (close to 0) in the matrix, indicating little to no linear relationship between those variables.

Flag_final: This variable shows moderate positive correlations with sms_distinct_days (0.3579) and total_revenue (0.3616). This suggests that the flag_final variable is somewhat related to these two variables.

Voice Usage: voice_usage has a positive correlation with total_revenue (0.6473), suggesting that higher voice usage tends to be associated with higher total revenue.

Highly Correlated Variables: Some variables are highly correlated, which might indicate multicollinearity in a regression analysis. For example, sms_usage and sms_revenue have a strong positive correlation of 0.8778, which means they provide similar information in a model.

Age and AON: The age of the user (age) and the age on the network (aon) have a negative correlation of -0.342, which suggests that as the age of the user increases, the age on the network tends to decrease
*/

=======================================================extra 8 columns=================================









=================time diff calculation for same bundle purchase , diff bundle purchase==================

DROP TABLE IF EXISTS rocai_analysis2.20231003_same_bundle_pur_cal_basev1;

CREATE TABLE rocai_analysis2.20231003_same_bundle_pur_cal_basev1 STORED AS ORC AS
select 
msisdn ,
bundle_code,
part_date
from 
rocai_analysis2.prof_prod_msisdn_daily
where part_date>='20230701' and part_date<='20230731' 
and lower(service_type)='sms'
and bundle_code in ('58','59','60','61','63','342')
AND (txn_cnt > 0 or txn_amt > 0)
and LENGTH(msisdn) = 12
and substr(msisdn, 1, 5) in ('26377', '26378')
group by 
msisdn ,
bundle_code,
part_date
order by 
bundle_code,
part_date;





DROP TABLE IF EXISTS rocai_analysis2.20231003_same_bundle_pur_cal_basev3;

CREATE TABLE rocai_analysis2.20231003_same_bundle_pur_cal_basev3 STORED AS ORC AS
/*select 
msisdn ,
bundle_code,
part_date,
from 
rocai_analysis2.20231003_same_bundle_pur_cal_basev1
group by 
msisdn ,
bundle_code,
part_date
order by 
bundle_code,
part_date;*/



DROP TABLE IF EXISTS rocai_analysis2.20231003_same_bundle_pur_cal_basev3;

CREATE TABLE rocai_analysis2.20231003_same_bundle_pur_cal_basev3 STORED AS ORC AS
WITH RankedData AS (
SELECT
msisdn,
bundle_code,
part_date,
LEAD(part_date) OVER (PARTITION BY msisdn, bundle_code ORDER BY part_date) AS next_purchase_date
FROM
rocai_analysis2.20231003_same_bundle_pur_cal_basev1
)
SELECT
msisdn,
bundle_code,
part_date,
next_purchase_date
FROM
RankedData
ORDER BY
bundle_code,
part_date
;



/*SELECT
msisdn,
bundle_code,
part_date,
next_purchase_date,
ROUND(DATEDIFF(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(next_purchase_date, 'yyyyMMdd'))), TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(part_date, 'yyyyMMdd')))) ) AS days_diff
from
rocai_analysis2.20231003_same_bundle_pur_cal_basev3;*/

DROP TABLE IF EXISTS rocai_analysis2.20231003_same_bundle_pur_cal_onetimebuycal;

CREATE TABLE rocai_analysis2.20231003_same_bundle_pur_cal_onetimebuycal STORED AS ORC AS
SELECT 
msisdn,
bundle_code,
part_date,
next_purchase_date,
CASE
WHEN COUNT(1) = 1 AND next_purchase_date IS NULL THEN 'one_time_buyer'
END AS one_time_buyer
FROM 
rocai_analysis2.20231003_same_bundle_pur_cal_basev3
group by
msisdn,
bundle_code,
part_date,
next_purchase_date;





DROP TABLE IF EXISTS rocai_analysis2.20231003_same_bundle_pur_cal_basev4;

CREATE TABLE rocai_analysis2.20231003_same_bundle_pur_cal_basev4 STORED AS ORC AS
SELECT
msisdn,
bundle_code,
part_date,
next_purchase_date,
one_time_buyer,
CASE
WHEN a.one_time_buyer = 'one_time_buyer' THEN 99999
WHEN next_purchase_date IS NOT NULL THEN ROUND(DATEDIFF(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(next_purchase_date, 'yyyyMMdd'))), TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(part_date, 'yyyyMMdd')))))
END AS days_diff
FROM
rocai_analysis2.20231003_same_bundle_pur_cal_onetimebuycal a
ORDER BY
msisdn,
bundle_code,
part_date,
one_time_buyer;

DROP TABLE IF EXISTS rocai_analysis2.20231003_same_bundle_days_diff_final_table;

CREATE TABLE rocai_analysis2.20231003_same_bundle_days_diff_final_table STORED AS ORC AS
select 
msisdn,
bundle_code,
min(days_diff) as smb_min_value,
max(days_diff) as smb_max_value,
avg(days_diff) as smb_avg_value
from 
rocai_analysis2.20231003_same_bundle_pur_cal_basev4
where days_diff is not null
group by 
msisdn,
bundle_code
order by 
bundle_code;











===========================for diff bundles================================================
DROP TABLE IF EXISTS rocai_analysis2.20231003_same_bundle_pur_cal_basev5;

CREATE TABLE rocai_analysis2.20231003_same_bundle_pur_cal_basev5 STORED AS ORC AS
select 
msisdn,
bundle_code,
min(part_date) as min_pd,
LEAD(part_date) OVER (PARTITION BY msisdn, bundle_code ORDER BY part_date) AS next_purchase_date
from
rocai_analysis2.20231003_same_bundle_pur_cal_basev1
group by 
msisdn,
bundle_code,
part_date
order by 
bundle_code,
min_pd;


DROP TABLE IF EXISTS rocai_analysis2.20231003_same_bundle_pur_cal_basev6;

CREATE TABLE rocai_analysis2.20231003_same_bundle_pur_cal_basev6 STORED AS ORC AS
select 
msisdn,
bundle_code,
min_pd,
next_purchase_date,
CASE
WHEN COUNT(1) = 1 AND next_purchase_date IS NULL THEN 'one_time_buyer'
END AS one_time_buyer
from
rocai_analysis2.20231003_same_bundle_pur_cal_basev5
group by 
msisdn,
bundle_code,
min_pd,
next_purchase_date
ORDER BY
msisdn,
bundle_code;


DROP TABLE IF EXISTS rocai_analysis2.20231003_diff_bundle_pur_cal_basev6;

CREATE TABLE rocai_analysis2.20231003_diff_bundle_pur_cal_basev6 STORED AS ORC AS

SELECT
msisdn,
bundle_code,
min_pd,
next_purchase_date,
one_time_buyer,
CASE
WHEN a.one_time_buyer = 'one_time_buyer' THEN 99999
WHEN next_purchase_date IS NOT NULL THEN ROUND(DATEDIFF(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(next_purchase_date, 'yyyyMMdd'))), TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(min_pd, 'yyyyMMdd')))))
END AS days_diff
FROM
rocai_analysis2.20231003_same_bundle_pur_cal_basev6 a
ORDER BY
msisdn,
bundle_code,
min_pd,
one_time_buyer;



DROP TABLE IF EXISTS rocai_analysis2.20231003_diff_bundle_final_table;

CREATE TABLE rocai_analysis2.20231003_diff_bundle_final_table STORED AS ORC AS
select 
msisdn,
bundle_code,
min(days_diff) as dfb_min_value,
max(days_diff) as dfb_max_value,
avg(days_diff) as dfb_avg_value
from 
rocai_analysis2.20231003_diff_bundle_pur_cal_basev6
where days_diff is not null
group by 
msisdn,
bundle_code
order by 
bundle_code;


----------------validation------------



DROP TABLE IF EXISTS rocai_analysis2.20231003_diff_bundle_final_table_validation;

CREATE TABLE rocai_analysis2.20231003_diff_bundle_final_table_validation STORED AS ORC AS
select 
msisdn,
bundle_code,
min_pd,
next_purchase_date,
one_time_buyer,
min(days_diff) as dfb_min_value,
max(days_diff) as dfb_max_value,
avg(days_diff) as dfb_avg_value
from 
rocai_analysis2.20231003_diff_bundle_pur_cal_basev6
where days_diff is not null
group by 
msisdn,
bundle_code,
min_pd,
next_purchase_date,
one_time_buyer
order by 
bundle_code,
min_pd
;

==================================utilization nd effective rate-================


58  5
59   10
60    25
61    125
63   200
342  300



DROP TABLE IF EXISTS rocai_analysis2.20231004_01_H_utilization_eff_rates_cal;

CREATE TABLE rocai_analysis2.20231004_01_H_utilization_eff_rates_cal STORED AS ORC AS
SELECT
a.msisdn,
a.acct_res_id,
SUM(NVL(a.units_used_sms, 0)) / SUM(NVL(a.utilization_cnt, 0)) AS utilization_rate,
SUM(NVL(a.units_used_sms, 0)) / SUM(NVL(a.txn_amt, 0)) AS effective_rate,
(SUM(NVL(a.units_used_sms, 0)) / SUM(NVL(a.utilization_cnt, 0))) * 100 AS utilization_rate_percentage,
(SUM(NVL(a.units_used_sms, 0)) / SUM(NVL(a.txn_amt, 0))) * 100 AS effective_rate_percentage
FROM (
SELECT
msisdn,
acct_res_id,
units_used_sms,
txn_cnt,
txn_amt,
CASE
WHEN acct_res_id = '58' THEN txn_cnt * 5
WHEN acct_res_id = '59' THEN txn_cnt * 10
WHEN acct_res_id = '60' THEN txn_cnt * 25
WHEN acct_res_id = '61' THEN txn_cnt * 125
WHEN acct_res_id = '62' THEN txn_cnt * 200
WHEN acct_res_id = '342' THEN txn_cnt * 300
ELSE 0
END AS utilization_cnt
FROM
rocai_analysis2.prof_msisdn_daily_sms_bundle_purchase_usage_details_dimensions
WHERE
SUBSTR(part_date, 1, 6) = '202307'
AND acct_res_id IN ('58', '59', '60', '61', '63', '342')
AND (txn_cnt > 0 OR txn_amt > 0)
AND LENGTH(msisdn) = 12
AND SUBSTR(msisdn, 1, 5) IN ('26377', '26378')
) a
GROUP BY
a.msisdn,
a.acct_res_id;

0.0

select min(utilization_rate_percentage) from rocai_analysis2.20231004_01_H_utilization_eff_rates_cal;


900
select max(utilization_rate_percentage) from rocai_analysis2.20231004_01_H_utilization_eff_rates_cal;

select * from rocai_analysis2.20231004_01_H_utilization_eff_rates_cal where utilization_rate_percentage=900;




0.0
select min(effective_rate_percentage) from rocai_analysis2.20231004_01_H_utilization_eff_rates_cal;

47.2
select max(effective_rate_percentage) from rocai_analysis2.20231004_01_H_utilization_eff_rates_cal;



SELECT
msisdn,
acct_res_id,
units_used_sms,
txn_cnt,
txn_amt,
CASE
WHEN acct_res_id = '58' THEN txn_cnt * 5
WHEN acct_res_id = '59' THEN txn_cnt * 10
WHEN acct_res_id = '60' THEN txn_cnt * 25
WHEN acct_res_id = '61' THEN txn_cnt * 125
WHEN acct_res_id = '62' THEN txn_cnt * 200
WHEN acct_res_id = '342' THEN txn_cnt * 300
ELSE 0
END AS utilization_cnt
FROM
rocai_analysis2.prof_msisdn_daily_sms_bundle_purchase_usage_details_dimensions
WHERE
SUBSTR(part_date, 1, 6) = '202307'
AND acct_res_id IN ('58', '59', '60', '61', '63', '342')
AND (txn_cnt > 0 OR txn_amt > 0)
AND LENGTH(msisdn) = 12
AND SUBSTR(msisdn, 1, 5) IN ('26377', '26378') and  msisdn = '263786996322';
















=================================================creating final table===============================================



DROP TABLE IF EXISTS rocai_analysis2.temp_HP_sms_migration_model_final_base;

CREATE TABLE rocai_analysis2.temp_HP_sms_migration_model_final_base STORED AS ORC AS
select a.*,
b.smb_min_value,
b.smb_max_value,
b.smb_avg_value,
c.dfb_min_value,
c.dfb_max_value,
c.dfb_avg_value,
d.utilization_rate_percentage,
d.effective_rate_percentage,
case when a.region='Harare North' then 19 
when a.region='Harare South' then 19 
when a.region='Manicaland' then 20	 
when a.region='Mashonaland C + W' then 21
when a.region='Mashonaland East + CZA' then 20
when a.region='Masvingo' then 20
when a.region='Matebeleland' then 19 
when a.region='Midlands' then 20
when a.region='Unmapped' then 24 else 0 end as region_conv_rate
from  
rocai_analysis2.tmp_sms_rf_model_final_base a 
left join
----same bundle purchase days diff
rocai_analysis2.20231003_same_bundle_days_diff_final_table b
on a.msisdn=b.msisdn 
left join
---------diff bundles days diff-------------
rocai_analysis2.20231003_diff_bundle_final_table c
on a.msisdn=c.msisdn
left join 
------------utilization and eff rates--------------
rocai_analysis2.20231004_01_H_utilization_eff_rates_cal d 
on a.msisdn=d.msisdn;




select count(1) as cnt, count(distinct msisdn) as cccnt from rocai_analysis2.temp_HP_sms_migration_model_final_base;


select a.*,
b.smb_min_value,
b.smb_max_value,
b.smb_avg_value,
c.dfb_min_value,
c.dfb_max_value,
c.dfb_avg_value,
d.utilization_rate_percentage,
d.effective_rate_percentage,
case when a.region='Harare North' then 19 
when a.region='Harare South' then 19 
when a.region='Manicaland' then 20	 
when a.region='Mashonaland C + W' then 21
when a.region='Mashonaland East + CZA' then 20
when a.region='Masvingo' then 20
when a.region='Matebeleland' then 19 
when a.region='Midlands' then 20
when a.region='Unmapped' then 24 else 0 end as region_conv_rate
from  
rocai_analysis2.tmp_sms_rf_model_final_base a 
left join
----same bundle purchase days diff
rocai_analysis2.20231003_same_bundle_days_diff_final_table b
on a.msisdn=b.msisdn 
left join
---------diff bundles days diff-------------
rocai_analysis2.20231003_diff_bundle_final_table c
on a.msisdn=c.msisdn
left join 
------------utilization and eff rates--------------
rocai_analysis2.20231004_01_H_utilization_eff_rates_cal d 
on a.msisdn=d.msisdn;
