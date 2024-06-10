-- Databricks notebook source
--clients receiving notify campaigns by delivery 
select a.treatment_description, count(distinct a.contact_key) as nb_client
from rdm.f_camp_contact a
inner join rdm.d_camp_offer b on a.camp_offer_key = b.camp_offer_key
where a.is_control = 'N' 
and b.campaign_description in ('2306_W25_MED_EML_SLD_NEWBRANDS (aswOP56156)','2306_W26_MPR_EML_SLD_LANCEMENT (aswOP56289)','2306_W26_MPR_EML_SLD_PROMO_PARFUM (aswOP56315)','2307_W27_MPR_EML_SLD_PROMO_PARFUM_LAST_CALL (aswOP56416)','2307_W28_MPR_EML_SLD_BEAUTYDEALS (aswOP56510)','2307_W29_MED_EML_SLD_CAPILLAIRE (aswOP56627)','2307_W30_MED_EML_SLD_SOLAIRES (aswOP56768)','2308_W31_MED_EML_ETE_TROUSSE (aswOP56866)','2308_W32_MPR_EML_ETE_GOLDENDAYS (aswOP57072)','2308_W33_MED_EML_ETE_SKINCARE (aswOP57077)','2308_W34_MED_EML_ETE_COUPDECOEURACCESSIBLE (aswOP57099)','2308_W35_MED_EML_LYS_FRAGRANCES_ICONIC (aswOP57100)','2309_W36_MED_EML_LYS_FRAGRANCES_NEW (aswOP57372)','2309_W37_MPR_EML_LYS_UNIQUE_DAYS (aswOP57531)','2309_W38_MPR_EML_LYS_LOREAL_PARA (aswOP57734)','2310_W40_MED_EML_LYS_FRAGRANCE_PREMIUM (aswOP58086)','2310_W41_MED_EML_LYS_FRAGRANCE_TOPSELLER (aswOP58144)','2310_W42_MED_EML_XMS_CALENDAR (aswOP58378)','2311_W44_MED_EML_BFR_NEWSKINCAREBRANDS (aswOP58661)','2311_W45_MPR_EML_BFR_FLASH2 (aswOP58728)','2311_W47_MPR_EML_BFR_FLASH4 (aswOP59023)','2312_W48_MPR_EML_XMS_GIFTSET (aswOP59163)','2312_W51_MPR_EML_XMS_LASTCALL (aswOP59615)','2312_W52_MED_EML_XMS_WALLET (aswOP59744)','2401_W03_MPR_EML_SAL_SECOND (aswOP60062)','2402_W04_MED_EML_VLD_GIFTSET (aswOP60194)','2402_W05_MPR_EML_SAL_LASTCALL (aswOP60411)','2402_W06_MED_EML_VLD_BUDGET (aswOP60583)','2402_W07_MPR_EML_MSM_PRIX_BAS_LAUNCH (aswOP60750)','2402_W08_MED_EML_MSM_BEAUTY_FAVORITES (aswOP60889)','2403_W10_MED_EML_MSM_PREVIEW (aswOP61049)','2403_W12_MED_EML_MSM_SPRING_FRAGRANCES (aswOP61398)'
)
group by a.treatment_description 
order by a.treatment_description asc

-- COMMAND ----------

-- get notify inactive opener and first open date
drop table if exists mfr_custom.ct_notify;

create table mfr_custom.ct_notify as 
select * 
from(
  select *, row_number() over (partition by a.contact_key order by a.start_date asc) as rankPurchase
from (
  select DISTINCT a.contact_key, a.treatment_description, 
to_date(a.campaign_contact_created_date, 'yyyy-MM-dd') as start_date, 
to_date(a.campaign_contact_created_date, 'yyyy-MM-dd')+7 as end_date, 
case 
  when a.treatment_description like '%NOTIFY_INACTIVE%' then 'notify_inactive'
  when a.treatment_description like '%NOTIFY_ACTIVE%' then 'notify_active'
  when a.treatment_description like '%_TEMOIN%' then 'temoin'
  when (a.treatment_description like '%NO_NOTIFY%') or (a.treatment_description like '%NON_NOTIFY') then 'non_notify'
  else 'other'
end as segment,
max(case when c.reason_code in ('Sent','Received on mobile') then 1 else 0 end) as email_sent,
max(case when c.reason_code in ('Open','Email click', 'Mirror page','Click on mobile notification') then 1 else 0 end) as email_open,
max(case when c.reason_code in ('Email click', 'Mirror page','Click on mobile notification') then 1 else 0 end) as email_click,
max(case when c.reason_code in ('Opt-out') then 1 else 0 end) as unsub
from rdm.f_camp_contact a
  inner join rdm.d_camp_offer b on a.camp_offer_key = b.camp_offer_key
  inner join rdm.f_camp_resp c on a.camp_offer_key = c.camp_offer_key and a.contact_key = c.contact_key and a.treatment_id=c.treatment_id -- treatment id permet d avoir la communication exacte
where a.is_control = 'N'
and a.treatment_description not like '%BAT%'
and a.treatment_description not like '%NONMEMBRE%'
and a.treatment_description not like '%OFFRE%'
and a.treatment_description not like '%OFFER%'
and a.treatment_description not like '%ALLO%'
and b.campaign_description in ('2306_W25_MED_EML_SLD_NEWBRANDS (aswOP56156)','2306_W26_MPR_EML_SLD_LANCEMENT (aswOP56289)','2306_W26_MPR_EML_SLD_PROMO_PARFUM (aswOP56315)','2307_W27_MPR_EML_SLD_PROMO_PARFUM_LAST_CALL (aswOP56416)','2307_W28_MPR_EML_SLD_BEAUTYDEALS (aswOP56510)','2307_W29_MED_EML_SLD_CAPILLAIRE (aswOP56627)','2307_W30_MED_EML_SLD_SOLAIRES (aswOP56768)','2308_W31_MED_EML_ETE_TROUSSE (aswOP56866)','2308_W32_MPR_EML_ETE_GOLDENDAYS (aswOP57072)','2308_W33_MED_EML_ETE_SKINCARE (aswOP57077)','2308_W34_MED_EML_ETE_COUPDECOEURACCESSIBLE (aswOP57099)','2308_W35_MED_EML_LYS_FRAGRANCES_ICONIC (aswOP57100)','2309_W36_MED_EML_LYS_FRAGRANCES_NEW (aswOP57372)','2309_W37_MPR_EML_LYS_UNIQUE_DAYS (aswOP57531)','2309_W38_MPR_EML_LYS_LOREAL_PARA (aswOP57734)','2310_W40_MED_EML_LYS_FRAGRANCE_PREMIUM (aswOP58086)','2310_W41_MED_EML_LYS_FRAGRANCE_TOPSELLER (aswOP58144)','2310_W42_MED_EML_XMS_CALENDAR (aswOP58378)','2311_W44_MED_EML_BFR_NEWSKINCAREBRANDS (aswOP58661)','2311_W45_MPR_EML_BFR_FLASH2 (aswOP58728)','2311_W47_MPR_EML_BFR_FLASH4 (aswOP59023)','2312_W48_MPR_EML_XMS_GIFTSET (aswOP59163)','2312_W51_MPR_EML_XMS_LASTCALL (aswOP59615)','2312_W52_MED_EML_XMS_WALLET (aswOP59744)','2401_W03_MPR_EML_SAL_SECOND (aswOP60062)','2402_W04_MED_EML_VLD_GIFTSET (aswOP60194)','2402_W05_MPR_EML_SAL_LASTCALL (aswOP60411)','2402_W06_MED_EML_VLD_BUDGET (aswOP60583)','2402_W07_MPR_EML_MSM_PRIX_BAS_LAUNCH (aswOP60750)','2402_W08_MED_EML_MSM_BEAUTY_FAVORITES (aswOP60889)','2403_W10_MED_EML_MSM_PREVIEW (aswOP61049)','2403_W12_MED_EML_MSM_SPRING_FRAGRANCES (aswOP61398)'
)
group by a.contact_key, 
a.treatment_description,
to_date(a.campaign_contact_created_date, 'yyyy-MM-dd'), 
to_date(a.campaign_contact_created_date, 'yyyy-MM-dd')+7
) a 
where a.segment = 'notify_inactive'
-- and a.email_open = 1 on prend tout le monde
) a 
--where a.rankPurchase = 1 
;

-- COMMAND ----------

--get unique notify inactive client opener and buer business KPI
select cast(DATE_PART('year', a.transaction_date_time) as varchar(4)) as yr, 
cast(DATE_PART('week', a.transaction_date_time) as varchar(2)) as wk,
count(distinct a.contact_key) as nb_client,
count(distinct a.transaction_key) as nb_ticket, 
sum(a.item_total_regular_unit_price) as ca, 
sum(a.quantity) as nb_item
from rdm.f_transaction_detail a
  INNER JOIN (SELECT * FROM mfr_custom.mfr_product 
              WHERE KPI_EXCLUSION_FLAG='N' 
              AND product_hier_1_L1_name!='GNFR'
              AND product_hier_1_l2_name!='LIVRAISON INTERNET'
              ) b on a.product_key=b.product_key
  inner join (select distinct a.contact_key
              from mfr_custom.ct_notify a 
              --- where a.segment = 'notify_inactive'
              --- and a.email_open = 1
                ) c on a.contact_key = c.contact_key
where to_date(a.transaction_date_time,'yyyy-MM-dd') >= c.start_date
group by cast(DATE_PART('year', a.transaction_date_time) as varchar(4)), cast(DATE_PART('week', a.transaction_date_time) as varchar(2))
order by yr, wk ; 

-- COMMAND ----------

--calculate incremental revenu, by month
with notify_inactive_client_purchase as(
  select LEFT(a.transaction_date_key, 6) as yr_mo, 
    count(distinct a.contact_key) as nb_client,
    count(distinct a.transaction_key) as nb_ticket, 
    sum(a.item_total_regular_unit_price) as ca, 
    sum(a.quantity) as nb_item
  from rdm.f_transaction_detail a
      INNER JOIN (SELECT * FROM mfr_custom.mfr_product 
                  WHERE KPI_EXCLUSION_FLAG='N' 
                  AND product_hier_1_L1_name!='GNFR'
                  AND product_hier_1_l2_name!='LIVRAISON INTERNET') b on a.product_key=b.product_key
  inner join (select distinct a.contact_key
              from mfr_custom.ct_notify a 
              --- where a.segment = 'notify_inactive'
              --- and a.email_open = 1
                ) c on a.contact_key = c.contact_key
  where to_date(a.transaction_date_time,'yyyy-MM-dd') >= c.start_date
  and a.item_total_regular_unit_price > 0
  and a.quantity > 0 
  group by LEFT(a.transaction_date_key, 6)
  order by yr_mo
)

select a.yr_mo, a.ca,
sum(a.ca) over (order by a.yr_mo asc) as incremental_ca -- ca cumulé
from notify_inactive_client_purchase a 
order by a.yr_mo asc;


-- COMMAND ----------

  SELECT *, cast(year(a.start_date) as int)*10000 + cast(month(a.start_date) as int)*100 + cast(day(a.start_date) as int) as start_date_int
  from mfr_custom.ct_notify a 

-- COMMAND ----------

--nb purchase for notify inactive vs non buyer
with notify_inactive_initial_rfm as(
SELECT a.contact_key, 
min(CASE WHEN b.segment_name ='VIP' THEN '01.VIP'
          WHEN b.segment_name ='LOYAL' THEN '02.LOYAL'
          WHEN b.segment_name ='REGULAR' THEN '03.REGULAR'
          WHEN b.segment_name ='NEW' THEN '05.NEW'
          WHEN b.segment_name ='LAPSED' THEN '06.LAPSING'
          WHEN b.segment_name ='ONE-OFFS' THEN '04.ONE-OFFS'
          WHEN b.segment_name IN ('INACTIVE','GONE AWAY') THEN '07.INACTIVE'
          WHEN b.segment_name IS NULL THEN '05.NEW' ELSE b.segment_name 
      END) as rfm_initial
FROM rdm.flf_segment a
INNER JOIN rdm.d_segment_detail b ON a.segment_detail_key=b.segment_detail_key
INNER JOIN (
  SELECT *, cast(year(a.start_date) as int)*10000 + cast(month(a.start_date) as int)*100 + cast(day(a.start_date) as int) as start_date_int
  from mfr_custom.ct_notify a 
) c on a.contact_key = c.contact_key
WHERE b.segment_type='RFM'
AND a.start_date_key <= c.start_date_int and (a.end_date_key >= c.start_date_int OR a.end_date_key IS NULL)
GROUP BY a.contact_key
),

current_rfm (
SELECT a.contact_key, 
min(CASE WHEN b.segment_name ='VIP' THEN '01.VIP'
          WHEN b.segment_name ='LOYAL' THEN '02.LOYAL'
          WHEN b.segment_name ='REGULAR' THEN '03.REGULAR'
          WHEN b.segment_name ='NEW' THEN '05.NEW'
          WHEN b.segment_name ='LAPSED' THEN '06.LAPSING'
          WHEN b.segment_name ='ONE-OFFS' THEN '04.ONE-OFFS'
          WHEN b.segment_name IN ('INACTIVE','GONE AWAY') THEN '07.INACTIVE'
          WHEN b.segment_name IS NULL THEN '05.NEW' ELSE b.segment_name 
      END) as rfm_current
FROM rdm.flf_segment a
INNER JOIN rdm.d_segment_detail b ON a.segment_detail_key=b.segment_detail_key
WHERE b.segment_type='RFM'
AND a.start_date_key <= cast(year(current_date()) as int)*10000 + cast(month(current_date()) as int)*100 + cast(day(current_date()) as int)
and a.end_date_key IS NULL
group by a.contact_key
)

select d.rfm_initial, e.rfm_current, 
case 
  when b.ca > 0 then 'buyer' 
  else 'non_buyer' 
end as if_buyer, 
count(distinct a.contact_key) as nb_client, 
sum(b.ca) as ca, 
sum(b.nb_item) as nb_item, 
sum(b.nb_ticket) as nb_ticket
from mfr_custom.ct_notify a
left join 

(select a.contact_key,
LEFT(a.transaction_date_key, 6) as yr_mo, --- pr le mois prendre le mois fiscal de la table rdm.d_date
count(distinct a.transaction_key) as nb_ticket, 
sum(a.item_total_regular_unit_price) as ca, 
sum(a.quantity) as nb_item
from rdm.f_transaction_detail a
  INNER JOIN (SELECT * FROM mfr_custom.mfr_product 
              WHERE KPI_EXCLUSION_FLAG='N' 
              AND product_hier_1_L1_name!='GNFR'
              AND product_hier_1_l2_name!='LIVRAISON INTERNET'
              ) b on a.product_key=b.product_key
  inner join (select *
              from mfr_custom.ct_notify a 
              where a.segment = 'notify_inactive'
              and a.email_open = 1
                ) c on a.contact_key = c.contact_key
where to_date(a.transaction_date_time,'yyyy-MM-dd') >= c.start_date and to_date(a.transaction_date_time,'yyyy-MM-dd') <= '2024-04-30'
  and a.item_total_regular_unit_price > 0
  and a.quantity > 0
group by a.contact_key,
LEFT(a.transaction_date_key, 6)
) b on a.contact_key = b.contact_key


left join notify_inactive_initial_rfm d on a.contact_key = d.contact_key
left join current_rfm e on a.contact_key = e.contact_key 
group by d.rfm_initial, e.rfm_current, if_buyer

-- COMMAND ----------

select distinct *
from mfr_custom.ct_notify_all_inactive 
limit 10;

-- COMMAND ----------

SELECT count(DISTINCT contact_key)
from mfr_custom.ct_notify_all_inactive 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Compraison vs echatillon représentatif

-- COMMAND ----------

--all clients inactive
drop table if exists mfr_custom.ct_notify_all_inactive;

create table mfr_custom.ct_notify_all_inactive as 

select *
from (select DISTINCT a.contact_key, c.member_key,
case 
  when treatment_description like '%NOTIFY_INACTIVE%' or treatment_description like '%INACTIF%' then 'notify_inactive' 
  when treatment_description like '%NOTIFY_ACTIVE%' or treatment_description like '%_ACTIF%' then 'notify_active'
  when treatment_description like '%_TEMOIN%' then 'temoin'
  when (treatment_description like '%NO_NOTIFY%') or (treatment_description like '%NON_NOTIFY') then 'non_notify'
  else 'other'
end as segment
from rdm.f_camp_contact a
  inner join rdm.d_camp_offer b on a.camp_offer_key = b.camp_offer_key
  inner join rdm.d_member c on a.contact_key = c.contact_key
where a.is_control = 'N'
and a.treatment_description not like '%BAT%'
and a.treatment_description not like '%NONMEMBRE%'
and a.treatment_description not like '%OFFRE%'
and a.treatment_description not like '%OFFER%'
and a.treatment_description not like '%ALLO%'
and b.campaign_description in ('2306_W25_MED_EML_SLD_NEWBRANDS (aswOP56156)','2306_W26_MPR_EML_SLD_LANCEMENT (aswOP56289)','2306_W26_MPR_EML_SLD_PROMO_PARFUM (aswOP56315)','2307_W27_MPR_EML_SLD_PROMO_PARFUM_LAST_CALL (aswOP56416)','2307_W28_MPR_EML_SLD_BEAUTYDEALS (aswOP56510)','2307_W29_MED_EML_SLD_CAPILLAIRE (aswOP56627)','2307_W30_MED_EML_SLD_SOLAIRES (aswOP56768)','2308_W31_MED_EML_ETE_TROUSSE (aswOP56866)','2308_W32_MPR_EML_ETE_GOLDENDAYS (aswOP57072)','2308_W33_MED_EML_ETE_SKINCARE (aswOP57077)','2308_W34_MED_EML_ETE_COUPDECOEURACCESSIBLE (aswOP57099)','2308_W35_MED_EML_LYS_FRAGRANCES_ICONIC (aswOP57100)','2309_W36_MED_EML_LYS_FRAGRANCES_NEW (aswOP57372)','2309_W37_MPR_EML_LYS_UNIQUE_DAYS (aswOP57531)','2309_W38_MPR_EML_LYS_LOREAL_PARA (aswOP57734)','2310_W40_MED_EML_LYS_FRAGRANCE_PREMIUM (aswOP58086)','2310_W41_MED_EML_LYS_FRAGRANCE_TOPSELLER (aswOP58144)','2310_W42_MED_EML_XMS_CALENDAR (aswOP58378)','2311_W44_MED_EML_BFR_NEWSKINCAREBRANDS (aswOP58661)','2311_W45_MPR_EML_BFR_FLASH2 (aswOP58728)','2311_W47_MPR_EML_BFR_FLASH4 (aswOP59023)','2312_W48_MPR_EML_XMS_GIFTSET (aswOP59163)','2312_W51_MPR_EML_XMS_LASTCALL (aswOP59615)','2312_W52_MED_EML_XMS_WALLET (aswOP59744)','2401_W03_MPR_EML_SAL_SECOND (aswOP60062)','2402_W04_MED_EML_VLD_GIFTSET (aswOP60194)','2402_W05_MPR_EML_SAL_LASTCALL (aswOP60411)','2402_W06_MED_EML_VLD_BUDGET (aswOP60583)','2402_W07_MPR_EML_MSM_PRIX_BAS_LAUNCH (aswOP60750)','2402_W08_MED_EML_MSM_BEAUTY_FAVORITES (aswOP60889)','2403_W10_MED_EML_MSM_PREVIEW (aswOP61049)','2403_W12_MED_EML_MSM_SPRING_FRAGRANCES (aswOP61398)')
) a 
where a.segment = 'notify_inactive'

-- COMMAND ----------

--creation notify inactive client profil table
--start date '2023-06-21' & end date '2024-05-10'

--execute this vv

drop table if exists mfr_custom.ct_notify_inactive_civ;

create table mfr_custom.ct_notify_inactive_civ as 

select a.*,
case when age >= 16 and age <= 25 then  '01.Entre 16 et 25 ans'        
    when age >= 26 and age <= 35 then  '02.Entre 26 et 35 ans'    
    when age >= 36 and age <= 45 then  '03.Entre 36 et 45 ans'    
    when age >= 46 and age <= 55 then  '04.Entre 46 et 55 ans'    
    when age >= 56 and age <= 65 then  '05.Entre 56 et 65 ans'    
    when age >= 66 and age <= 100 then  '06.Entre 66 et 100 ans'    
    else '07.AUTRES' 
end as tr_age, 
case when anciennete < 12 then '01. Moins 1 an'
    when anciennete >= 12 and anciennete < 36 then '02. Entre 1 et 3 ans'
    when anciennete >= 36 and anciennete < 60 then '03. Entre 3 et 5 ans'
    when anciennete >= 60 then '04. Plus 5 ans'
end as tr_anciennte
    -- rajouter tranche d anciennete difference premier achat et 21 juin 2023
    -- moins d un an - 1 a 3 - 3 a 5 - Plus de 5 ans
FROM (
    select a.*
        , case when int(DATEDIFF('2023-06-21', birth_date)/365.25)<16 or int(DATEDIFF('2023-06-21', birth_date)/365.25)>100 then null 
                else int(DATEDIFF('2023-06-21', birth_date)/365.25) end as age
        , gender
        , case when  is_valid_email = 'Y' and is_suppress_email = 'N' then 1 ELSE 0 END as EMAIL_OPT_IN_MRND 
        , case when  ch_sms.optin_sms is not null then 1 ELSE 0 END as SMS_OPT_IN_MRND --use a fake optin sms instead -> receive sms since march
        , case when  is_valid_address = 'Y' and is_suppress_mail = 'N' then 1 ELSE 0 END as DM_OPT_IN_MRND

        , coalesce(member_tier_name,'Privilege Member') as member_tier_name
        , coalesce(segment_rfm, '05.NEW') as segment_rfm
        , ls.segment_ls
    from mfr_custom.ct_notify_all_inactive a 
INNER JOIN rdm.d_contact b  on a.contact_key=b.contact_key
INNER JOIN (select distinct member_key, contact_key, enrol_date 
                from rdm.d_member) m on a.contact_key=m.contact_key

INNER JOIN (SELECT DISTINCT contact_key
            FROM rdm.f_transaction_detail a  
                INNER JOIN (SELECT * FROM mfr_custom.mfr_product 
                WHERE KPI_EXCLUSION_FLAG='N' 
                AND product_hier_1_L1_name!='GNFR'
                AND product_hier_1_l2_name!='LIVRAISON INTERNET') b on a.product_key=b.product_key --product filer added
            WHERE business_date_key between 20200621 AND 20230621   --DONE. instead of 20230621 AND 20240430, need to take buyers before the test
            and BU_KEY='MFR'			
            and CONTACT_KEY!='0'
            and quantity<>0		
            GROUP BY contact_key) tr on a.contact_key=tr.contact_key 

LEFT JOIN (select member_key, min(member_tier_name) as member_tier_name
                from rdm.d_member_tier_history
                where ( to_date(member_tier_start_date, 'yyyy-MM-dd')< to_date('2023-06-21', 'yyyy-MM-dd') 
                and (to_date(member_tier_end_date, 'yyyy-MM-dd') >=to_date('2023-06-21', 'yyyy-MM-dd') or member_tier_end_date is null))
                and member_tier_name in ('Privilege Diamond','Privilege Member','Privilege VIP')
                group by member_key) c on a.member_key=c.member_key

LEFT JOIN (select contact_key
                , min (case when segment_name ='VIP' then '01.VIP'
                                when segment_name ='LOYAL' then '02.LOYAL'
                                when segment_name ='REGULAR' then '03.REGULAR'
                                when segment_name ='NEW' then '05.NEW'
                                when segment_name ='LAPSED' then '06.LAPSING'
                                when segment_name ='ONE-OFFS' then '04.ONE-OFFS'
                                when segment_name in ('INACTIVE','GONE AWAY') then '07.INACTIVE'
                                when segment_name is null then '05.NEW' else segment_name end) as SEGMENT_RFM
            from rdm.flf_segment a
            INNER JOIN rdm.d_segment_detail b on a.segment_detail_key=b.segment_detail_key
            where b.segment_type='RFM'
            and a.start_date_key <= 20230621
            and (a.end_date_key >= 20230621 or a.end_date_key is null)
            group by contact_key) rfm on a.contact_key=rfm.contact_key --RFM at sending date
                
LEFT JOIN (select contact_key
                , min(sub_segment_name) as SEGMENT_LS
            from rdm.flf_segment a
            INNER JOIN rdm.d_segment_detail b on a.segment_detail_key=b.segment_detail_key
            where b.segment_type='Lifestyle'
            and a.start_date_key <= 20230621
            and (a.end_date_key >= 20230621 or a.end_date_key is null)
            group by contact_key) LS on a.contact_key=LS.contact_key  --lifeStyle at sending date
left join (select distinct contact_key as optin_sms
            from rdm.f_camp_resp 
            where to_date(campaign_response_date_time,'yyyy-MM-dd') between '2024-03-10' and '2024-05-10'
            and channel = 'Mobile' --remove sms reason code filter
            ) ch_sms on a.contact_key = ch_sms.optin_sms --use a fake optin sms instead -> received sms or not in the past 2 months 
            ) a 
            
left join (select distinct a.contact_key, datediff(month, to_date(min(a.transaction_date_time),'yyyy-MM-dd'), to_date('2023-06-21','yyyy-MM-dd')) as anciennete
            from rdm.f_transaction_detail a
            where a.bu_key='MFR'
            and a.quantity<>0 
            group by a.contact_key
            HAVING anciennete > 0
            ) AN on a.contact_key = an.contact_key --get anciennete by month

-- COMMAND ----------

select *
from mfr_custom.ct_notify_inactive_civ
limit 10;

-- COMMAND ----------

--anciennté (ecart 1er achat)
select distinct a.contact_key, datediff(month, to_date(min(a.transaction_date_time),'yyyy-MM-dd'), to_date('2023-06-21','yyyy-MM-dd')) as anciennete
from rdm.f_transaction_detail a
INNER JOIN (select distinct contact_key from mfr_custom.ct_notify_all_inactive) b on a.contact_key=b.contact_key
where a.bu_key='MFR'
and a.quantity<>0 
group by a.contact_key
limit 10;


-- COMMAND ----------

--creation control group profil table
--start date '2023-06-21'
drop table if exists mfr_custom.ct_notify_temoin;

create table  mfr_custom.ct_notify_temoin as 
select a.*, 
    case when age >= 16 and age <= 25 then  '01.Entre 16 et 25 ans'        
        when age >= 26 and age <= 35 then  '02.Entre 26 et 35 ans'    
        when age >= 36 and age <= 45 then  '03.Entre 36 et 45 ans'    
        when age >= 46 and age <= 55 then  '04.Entre 46 et 55 ans'    
        when age >= 56 and age <= 65 then  '05.Entre 56 et 65 ans'    
        when age >= 66 and age <= 100 then  '06.Entre 66 et 100 ans'    
        else '07.AUTRES' 
    end as tr_age,
    case when anciennete < 12 then '01. Moins 1 an'
        when anciennete >= 12 and anciennete < 36 then '02. Entre 1 et 3 ans'
        when anciennete >= 36 and anciennete < 60 then '03. Entre 3 et 5 ans'
        when anciennete >= 60 then '04. Plus 5 ans'
    end as tr_anciennte
    -- rajouter tranche d anciennete difference premier achat et 21 juin 2023
    -- moins d un an - 1 a 3 - 3 a 5 - Plus de 5 ans
FROM
(
select a.contact_key, m.member_key
, case when int(DATEDIFF('2023-06-21', birth_date)/365.25)<16 or int(DATEDIFF('2023-06-21', birth_date)/365.25)>100 then null 
        else int(DATEDIFF('2023-06-21', birth_date)/365.25) end as age
    , gender
    , case when  is_valid_email = 'Y' and is_suppress_email = 'N' then 1 ELSE 0 END as EMAIL_OPT_IN_MRND 
    , case when  ch_sms.optin_sms is not null then 1 ELSE 0 END as SMS_OPT_IN_MRND --use a fake optin sms instead -> receive sms since march
    , case when  is_valid_address = 'Y' and is_suppress_mail = 'N' then 1 ELSE 0 END as DM_OPT_IN_MRND
            
    , coalesce(member_tier_name,'Privilege Member') as member_tier_name
    , coalesce(segment_rfm, '05.NEW') as segment_rfm
    , ls.segment_ls
    , an.anciennete

from rdm.d_contact a 
INNER JOIN (select distinct member_key, contact_key, enrol_date 
                from rdm.d_member) m on a.contact_key=m.contact_key

INNER JOIN (SELECT DISTINCT contact_key
            FROM  rdm.f_transaction_detail a 
            INNER JOIN (SELECT * FROM mfr_custom.mfr_product 
                WHERE KPI_EXCLUSION_FLAG='N' 
                AND product_hier_1_L1_name!='GNFR'
                AND product_hier_1_l2_name!='LIVRAISON INTERNET') b on a.product_key=b.product_key --product filer added
            WHERE business_date_key between 20200621 AND 20230621   --DONE. instead of 20230621 AND 20240430, need to take buyers before the test
            and BU_KEY='MFR'			
            and CONTACT_KEY!='0'
            and quantity<>0		
            GROUP BY contact_key) tr on a.contact_key=tr.contact_key 

LEFT JOIN (select member_key, min(member_tier_name) as member_tier_name
                from rdm.d_member_tier_history
                where ( to_date(member_tier_start_date, 'yyyy-MM-dd')< to_date('2023-06-21', 'yyyy-MM-dd') 
                and (to_date(member_tier_end_date, 'yyyy-MM-dd') >=to_date('2023-06-21', 'yyyy-MM-dd') or member_tier_end_date is null))
                and member_tier_name in ('Privilege Diamond','Privilege Member','Privilege VIP')
                group by member_key) c on m.member_key=c.member_key

LEFT JOIN (select contact_key
                    , min (case when segment_name ='VIP' then '01.VIP'
                                 when segment_name ='LOYAL' then '02.LOYAL'
                                 when segment_name ='REGULAR' then '03.REGULAR'
                                 when segment_name ='NEW' then '05.NEW'
                                 when segment_name ='LAPSED' then '06.LAPSING'
                                 when segment_name ='ONE-OFFS' then '04.ONE-OFFS'
                                 when segment_name in ('INACTIVE','GONE AWAY') then '07.INACTIVE'
                                 when segment_name is null then '05.NEW' else segment_name end) as SEGMENT_RFM
                from rdm.flf_segment a
                INNER JOIN rdm.d_segment_detail b on a.segment_detail_key=b.segment_detail_key
                where b.segment_type='RFM'
                and a.start_date_key <= 20230621
                and (a.end_date_key >= 20230621 or a.end_date_key is null)
                group by contact_key) rfm on a.contact_key=rfm.contact_key
                
LEFT JOIN (select contact_key
                    , min(sub_segment_name) as SEGMENT_LS
                from rdm.flf_segment a
                INNER JOIN rdm.d_segment_detail b on a.segment_detail_key=b.segment_detail_key
                where b.segment_type='Lifestyle'
                and a.start_date_key <= 20230621
                and (a.end_date_key >= 20230621 or a.end_date_key is null)
                group by contact_key) LS on a.contact_key=LS.contact_key
left join (select distinct a.contact_key, datediff(month, to_date(min(a.transaction_date_time),'yyyy-MM-dd'), to_date('2023-06-21','yyyy-MM-dd')) as anciennete
            from rdm.f_transaction_detail a
            where a.bu_key='MFR'
            and a.quantity<>0 
            group by a.contact_key
            HAVING anciennete > 0
            ) an on a.contact_key = an.contact_key --get anciennete in month
left join (select distinct contact_key as optin_sms
            from rdm.f_camp_resp 
            where to_date(campaign_response_date_time,'yyyy-MM-dd') between '2024-03-10' and '2024-05-10'
            and channel = 'Mobile'--remove reason code filter
            ) ch_sms on a.contact_key = ch_sms.optin_sms --use a fake optin sms instead -> received sms or not in the past 2 months 

where a.bu_key='MFR'
and a.contact_key not in (select distinct contact_key from mfr_custom.ct_notify_all_inactive)
and a.contact_key not in(select distinct contact_key 
                        from rdm.f_camp_resp where to_date(campaign_response_date_time,'yyyy-MM-dd') >= '2023-06-21'
                        and channel = 'Email'
                        and reason_code not in ('Failed', 'Pending')) --- virer tous les clients qui ont recu un email depuis le 21 juin
        ) a
where EMAIL_OPT_IN_MRND = 0 --take EMAIL_OPT_IN_MRND  = 0 

-- COMMAND ----------

select * 
from mfr_custom.ct_notify_temoin
limit 10

-- COMMAND ----------

--- Pour savoir le nombre de clients a prendre
select distinct var_ech, 0.5*poids, max(rang)

from
(
select a.*, poids, 
  ROW_NUMBER() OVER (PARTITION BY a.var_ech ORDER BY RANDOM()) AS rang

from

(select a.* 
  , concat(coalesce(gender,'N'),' ',  dm_opt_in_mrnd,' ',  sms_opt_in_mrnd,' ',member_tier_name,' ',segment_rfm, ' ',tr_age,' ', tr_anciennte) as var_ech
from  mfr_custom.ct_notify_temoin a) a --removing opt-in email, add tr_anciennte, SMS_OPT_IN_MRND

LEFT JOIN 

    (select var_ech, count(distinct contact_key) as poids
      from
        (select *, 
        concat(coalesce(gender,'N'),' ',  dm_opt_in_mrnd,' ',  sms_opt_in_mrnd,' ',member_tier_name,' ',segment_rfm, ' ',tr_age,' ', tr_anciennte) as var_ech
        from mfr_custom.ct_notify_inactive_civ) --removing opt-in email, add tr_anciennte, SMS_OPT_IN_MRND
        group by 1
        order by 1) b on a.var_ech=b.var_ech)

group by 1,2
having max(rang)<=0.5*poids



-- COMMAND ----------

-- random sample comparaison 60%
drop table if exists mfr_custom.ct_notify_base_comparaison;

create table  mfr_custom.ct_notify_base_comparaison as

select *
from
(
select a.*, poids, 
  ROW_NUMBER() OVER (PARTITION BY a.var_ech ORDER BY RANDOM()) AS rang

from

(select a.* 
  , concat(coalesce(gender,'N'),' ', dm_opt_in_mrnd, ' ',member_tier_name,' ',segment_rfm,' ',tr_age,' ', tr_anciennte) as var_ech
from  mfr_custom.ct_notify_temoin a) a --- enlever contactabilité dm 

LEFT JOIN 

    (select var_ech, count(distinct contact_key) as poids
      from
        (select *, 
        concat(coalesce(gender,'N'),' ', dm_opt_in_mrnd, ' ', member_tier_name,' ',segment_rfm,' ',tr_age,' ', tr_anciennte) as var_ech
        from mfr_custom.ct_notify_inactive_civ) --- enlever contactabilité dm 
        group by 1
        order by 1) b on a.var_ech=b.var_ech)
  
  where rang <= 0.6 * poids

-- COMMAND ----------

-- random sample comparaison 50%
drop table if exists mfr_custom.ct_notify_base_comparaison;

create table  mfr_custom.ct_notify_base_comparaison as

select *
from
(
select a.*, poids, 
  ROW_NUMBER() OVER (PARTITION BY a.var_ech ORDER BY RANDOM()) AS rang

from

(select a.* 
  , concat(coalesce(gender,'N'),' ',  sms_opt_in_mrnd,' ',member_tier_name,' ',segment_rfm,' ',tr_age,' ', tr_anciennte) as var_ech
from  mfr_custom.ct_notify_temoin a) a --- enlever contactabilité dm 

LEFT JOIN 

    (select var_ech, count(distinct contact_key) as poids
      from
        (select *, 
        concat(coalesce(gender,'N'),' ',  sms_opt_in_mrnd,' ', member_tier_name,' ',segment_rfm,' ',tr_age,' ', tr_anciennte) as var_ech
        from mfr_custom.ct_notify_inactive_civ) --- enlever contactabilité dm 
        group by 1
        order by 1) b on a.var_ech=b.var_ech)
  
  where rang <= 0.5 * poids

-- COMMAND ----------

select * 
from mfr_custom.ct_notify_base_comparaison
limit 10

-- COMMAND ----------

DROP TABLE IF EXISTS mfr_custom.ct_notify_base_impact;

create table mfr_custom.ct_notify_base_impact as 
select *
FROM

(select 1 as notify_inactive_listed, contact_key, gender, tr_age, DM_OPT_IN_MRND, EMAIL_OPT_IN_MRND, SMS_OPT_IN_MRND, segment_rfm, segment_ls, member_tier_name
from mfr_custom.ct_notify_inactive_civ

UNION ALL

select 0 as notify_inactive_listed, contact_key, gender, tr_age, DM_OPT_IN_MRND, EMAIL_OPT_IN_MRND, SMS_OPT_IN_MRND, segment_rfm, segment_ls, member_tier_name
from mfr_custom.ct_notify_base_comparaison)

-- COMMAND ----------


with sales as 
(SELECT contact_key,       			
        count(distinct transaction_key) as TICKET,			
        sum(item_total_regular_unit_price) as SALE,            			
        sum(quantity) as PRODUIT			
  FROM  rdm.f_transaction_detail a 
  INNER JOIN mfr_custom.mfr_product b on a.product_key=b.product_key
                WHERE business_date_key between 20230621 AND 20240510
                and BU_KEY='MFR'			
                and CONTACT_KEY!='0'
                and quantity<>0		
            GROUP BY contact_key)
            
 select a.notify_inactive_listed,
        count(DISTINCT A.contact_key) AS CLIENTS_CIBLES,
        count(DISTINCT b.contact_key)AS CLIENTS_ACHETEURS,
        count(DISTINCT b.contact_key)/count(DISTINCT A.contact_key)  as TAUX_RETOUR,
        sum(SALE) AS SALES,
        sum(SALE)/ sum(TICKET) as ATV,
        sum(SALE)/ count(DISTINCT b.contact_key) as ACV,
        sum(PRODUIT)/count(DISTINCT b.contact_key) as QTY_CLIENT
        
FROM mfr_custom.ct_notify_base_impact a 

LEFT JOIN sales b on a.contact_key=b.contact_key

group by 1

-- COMMAND ----------

--by status
--50% CG
with sales as 
(SELECT contact_key,       			
        count(distinct transaction_key) as TICKET,			
        sum(item_total_regular_unit_price) as SALE,            			
        sum(quantity) as PRODUIT			
  FROM  rdm.f_transaction_detail a 
  INNER JOIN mfr_custom.mfr_product b on a.product_key=b.product_key
                WHERE business_date_key between 20230621 AND 20240510
                and BU_KEY='MFR'			
                and CONTACT_KEY!='0'
                and quantity<>0		
            GROUP BY contact_key)
            
 select a.notify_inactive_listed,
        a.member_tier_name,
        count(DISTINCT A.contact_key) AS CLIENTS_CIBLES,
        count(DISTINCT b.contact_key)AS CLIENTS_ACHETEURS,
        count(DISTINCT b.contact_key)/count(DISTINCT A.contact_key)  as TAUX_RETOUR,
        sum(TICKET) as nb_ticket,
        sum(SALE) AS SALES_MARQUES,

        sum(SALE)/ sum(TICKET) as ATV,
        sum(SALE)/ count(DISTINCT b.contact_key) as ACV,
        sum(PRODUIT)/count(DISTINCT b.contact_key) as QTY_CLIENT
        
FROM mfr_custom.ct_notify_base_impact a 

LEFT JOIN sales b on a.contact_key=b.contact_key

group by 1,2
order by 2,1 desc

-- COMMAND ----------

--by rfm, ls, status, age, gender
--50% CG
with sales as 
(SELECT contact_key,       			
        count(distinct transaction_key) as TICKET,			
        sum(item_total_regular_unit_price) as SALE,            			
        sum(quantity) as PRODUIT			
  FROM  rdm.f_transaction_detail a 
  INNER JOIN (SELECT * FROM mfr_custom.mfr_product 
              WHERE KPI_EXCLUSION_FLAG='N' 
              AND product_hier_1_L1_name!='GNFR'
              AND product_hier_1_l2_name!='LIVRAISON INTERNET') b on a.product_key=b.product_key
                WHERE business_date_key between 20230621 AND 20240510
                and BU_KEY='MFR'			
                and CONTACT_KEY!='0'
                and quantity<>0		
            GROUP BY contact_key)
            
 select a.notify_inactive_listed,
        a.segment_rfm,
        a.segment_ls,
        a.member_tier_name,
        a.tr_age,
        a.gender,
        count(DISTINCT A.contact_key) AS CLIENTS_CIBLES,
        count(DISTINCT b.contact_key)AS CLIENTS_ACHETEURS,
        count(DISTINCT b.contact_key)/count(DISTINCT A.contact_key)  as TAUX_RETOUR,
        sum(TICKET) as nb_ticket,
        sum(SALE) AS SALES,

        sum(SALE)/ sum(TICKET) as ATV,
        sum(SALE)/ count(DISTINCT b.contact_key) as ACV,
        sum(PRODUIT)/count(DISTINCT b.contact_key) as QTY_CLIENT
        
FROM mfr_custom.ct_notify_base_impact a 

LEFT JOIN sales b on a.contact_key=b.contact_key

group by 1, 2, 3, 4, 5, 6
order by 1 desc

-- COMMAND ----------

select * 
from  mfr_custom.ct_notify_base_impact
limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Perf j-6m vs. période notify

-- COMMAND ----------

--performance 6 months before notify vs notify test period. notify inactive listes vs CG
with salesBefore as 
(SELECT contact_key,       			
    count(distinct transaction_key) as TICKET,			
    sum(item_total_regular_unit_price) as SALE,            			
    sum(quantity) as PRODUIT			
FROM  rdm.f_transaction_detail a 
INNER JOIN (SELECT * FROM mfr_custom.mfr_product 
              WHERE KPI_EXCLUSION_FLAG='N' 
              AND product_hier_1_L1_name!='GNFR'
              AND product_hier_1_l2_name!='LIVRAISON INTERNET') b on a.product_key=b.product_key --product filter added
WHERE business_date_key between 20221220 AND 20230620
and BU_KEY='MFR'			
and CONTACT_KEY!='0'
and quantity<>0		
GROUP BY contact_key), 

salesAfter as 
(SELECT contact_key,       			
  count(distinct transaction_key) as TICKET,			
  sum(item_total_regular_unit_price) as SALE,            			
  sum(quantity) as PRODUIT			
FROM  rdm.f_transaction_detail a 
INNER JOIN (SELECT * FROM mfr_custom.mfr_product 
              WHERE KPI_EXCLUSION_FLAG='N' 
              AND product_hier_1_L1_name!='GNFR'
              AND product_hier_1_l2_name!='LIVRAISON INTERNET') b on a.product_key=b.product_key --product filter added
WHERE business_date_key between 20230621 AND 20240510
and BU_KEY='MFR'			
and CONTACT_KEY!='0'
and quantity<>0		
GROUP BY contact_key) 
            
 select a.notify_inactive_listed,
        a.segment_rfm,
        a.segment_ls,
        a.member_tier_name,
        a.tr_age,
        a.gender,
        count(distinct a.contact_key) as CLIENT_CIBLE,
        count(DISTINCT b.contact_key)AS CLIENTS_ACHETEURS_AVANT,
        sum(b.TICKET) as nb_ticket_AVANT,
        sum(b.SALE) AS SALES_AVANT,
        sum(b.SALE)/ sum(b.TICKET) as ATV_AVANT,
        sum(b.SALE)/ count(DISTINCT b.contact_key) as ACV_AVANT,
        sum(b.PRODUIT)/count(DISTINCT b.contact_key) as QTY_CLIENT_AVANT, --sales before test notify 6 months

        count(DISTINCT c.contact_key)AS CLIENTS_ACHETEURS_APRES,
        sum(c.TICKET) as nb_ticket_APRES,
        sum(c.SALE) AS SALES_APRES,
        sum(c.SALE)/ sum(c.TICKET) as ATV_APRES,
        sum(c.SALE)/ count(DISTINCT c.contact_key) as ACV_APRES,
        sum(c.PRODUIT)/count(DISTINCT c.contact_key) as QTY_CLIENT_APRES --sales during test notify
        
FROM mfr_custom.ct_notify_base_impact a 

LEFT JOIN salesBefore b on a.contact_key=b.contact_key
left join salesAfter c on a.contact_key=c.contact_key

group by 1, 2, 3, 4, 5, 6
order by 1 desc



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###test d'égalité de moeyenne

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #test d'égalité de moeyenne AVANT
-- MAGIC from scipy import stats
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.ml.stat import Summarizer
-- MAGIC from pyspark.ml.linalg import Vectors
-- MAGIC from pyspark.ml.feature import VectorAssembler
-- MAGIC
-- MAGIC df1 = spark.sql(
-- MAGIC     '''with salesBefore as 
-- MAGIC     (SELECT contact_key,       			
-- MAGIC         count(distinct transaction_key) as TICKET,			
-- MAGIC         sum(item_total_regular_unit_price) as SALE,            			
-- MAGIC         sum(quantity) as PRODUIT			
-- MAGIC     FROM  rdm.f_transaction_detail a 
-- MAGIC     INNER JOIN (SELECT * FROM mfr_custom.mfr_product 
-- MAGIC                   WHERE KPI_EXCLUSION_FLAG='N' 
-- MAGIC                   AND product_hier_1_L1_name!='GNFR'
-- MAGIC                   AND product_hier_1_l2_name!='LIVRAISON INTERNET') b on a.product_key=b.product_key --product filter added
-- MAGIC     WHERE business_date_key between 20221220 AND 20230620
-- MAGIC     and BU_KEY='MFR'			
-- MAGIC     and CONTACT_KEY!='0'
-- MAGIC     and quantity<>0		
-- MAGIC     GROUP BY contact_key)
-- MAGIC               
-- MAGIC     select a.contact_key,
-- MAGIC     sum(b.SALE)/ count(DISTINCT b.contact_key) as acv_avant
-- MAGIC     FROM mfr_custom.ct_notify_base_impact a 
-- MAGIC     inner JOIN salesBefore b on a.contact_key=b.contact_key --inner join to remove null acv
-- MAGIC     where a.notify_inactive_listed = 1
-- MAGIC     group by 1'''
-- MAGIC ) #notify
-- MAGIC
-- MAGIC df2 = spark.sql(
-- MAGIC   '''with salesBefore as 
-- MAGIC     (SELECT contact_key,       			
-- MAGIC         count(distinct transaction_key) as TICKET,			
-- MAGIC         sum(item_total_regular_unit_price) as SALE,            			
-- MAGIC         sum(quantity) as PRODUIT			
-- MAGIC     FROM  rdm.f_transaction_detail a 
-- MAGIC     INNER JOIN (SELECT * FROM mfr_custom.mfr_product 
-- MAGIC                   WHERE KPI_EXCLUSION_FLAG='N' 
-- MAGIC                   AND product_hier_1_L1_name!='GNFR'
-- MAGIC                   AND product_hier_1_l2_name!='LIVRAISON INTERNET') b on a.product_key=b.product_key --product filter added
-- MAGIC     WHERE business_date_key between 20221220 AND 20230620
-- MAGIC     and BU_KEY='MFR'			
-- MAGIC     and CONTACT_KEY!='0'
-- MAGIC     and quantity<>0		
-- MAGIC     GROUP BY contact_key)
-- MAGIC               
-- MAGIC     select a.contact_key,
-- MAGIC     sum(b.SALE)/ count(DISTINCT b.contact_key) as acv_avant
-- MAGIC     FROM mfr_custom.ct_notify_base_impact a 
-- MAGIC     inner JOIN salesBefore b on a.contact_key=b.contact_key --inner join to remove null acv
-- MAGIC     where a.notify_inactive_listed = 0
-- MAGIC     group by 1'''
-- MAGIC ) #CG
-- MAGIC
-- MAGIC # Create a VectorAssembler
-- MAGIC assembler = VectorAssembler(inputCols=["acv_avant"], outputCol="acv_avant_vec")
-- MAGIC
-- MAGIC # Transform the DataFrames
-- MAGIC df1 = assembler.transform(df1)
-- MAGIC df2 = assembler.transform(df2)
-- MAGIC
-- MAGIC # Calculate the means
-- MAGIC mean1 = df1.select(Summarizer.mean(df1.acv_avant_vec)).collect()[0][0][0]
-- MAGIC mean2 = df2.select(Summarizer.mean(df2.acv_avant_vec)).collect()[0][0][0]
-- MAGIC
-- MAGIC # Print the means
-- MAGIC print("Mean of the first group:", mean1)
-- MAGIC print("Mean of the second group:", mean2)
-- MAGIC
-- MAGIC # Test the equality of the means
-- MAGIC # (For a confidence level of 95%)
-- MAGIC if abs(mean1 - mean2) < 1.96 * ((df1.count() - 1) * df1.select(Summarizer.variance(df1.acv_avant_vec)).collect()[0][0][0] / df1.count() + (df2.count() - 1) * df2.select(Summarizer.variance(df2.acv_avant_vec)).collect()[0][0][0] / df2.count()) ** 0.5:
-- MAGIC     print("The means of the two groups are not statistically different (do not reject the null hypothesis)")
-- MAGIC else:
-- MAGIC     print("The means of the two groups are statistically different (reject the null hypothesis)")
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #test d'égalité de moeyenne APRES
-- MAGIC from scipy import stats
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.ml.stat import Summarizer
-- MAGIC from pyspark.ml.linalg import Vectors
-- MAGIC from pyspark.ml.feature import VectorAssembler
-- MAGIC
-- MAGIC df1 = spark.sql(
-- MAGIC     '''with salesAfter as 
-- MAGIC     (SELECT contact_key,       			
-- MAGIC       count(distinct transaction_key) as TICKET,			
-- MAGIC       sum(item_total_regular_unit_price) as SALE,            			
-- MAGIC       sum(quantity) as PRODUIT			
-- MAGIC     FROM  rdm.f_transaction_detail a 
-- MAGIC     INNER JOIN (SELECT * FROM mfr_custom.mfr_product 
-- MAGIC                   WHERE KPI_EXCLUSION_FLAG='N' 
-- MAGIC                   AND product_hier_1_L1_name!='GNFR'
-- MAGIC                   AND product_hier_1_l2_name!='LIVRAISON INTERNET') b on a.product_key=b.product_key --product filter added
-- MAGIC     WHERE business_date_key between 20230621 AND 20240510
-- MAGIC     and BU_KEY='MFR'			
-- MAGIC     and CONTACT_KEY!='0'
-- MAGIC     and quantity<>0		
-- MAGIC     GROUP BY contact_key) 
-- MAGIC               
-- MAGIC     select a.contact_key,
-- MAGIC     sum(c.SALE)/ count(DISTINCT c.contact_key) as acv_apres
-- MAGIC     FROM mfr_custom.ct_notify_base_impact a 
-- MAGIC     inner JOIN salesAfter c on a.contact_key=c.contact_key --inner join to remove null acv
-- MAGIC     where a.notify_inactive_listed = 1
-- MAGIC     group by 1'''
-- MAGIC ) #notify
-- MAGIC
-- MAGIC df2 = spark.sql(
-- MAGIC   '''with salesAfter as 
-- MAGIC     (SELECT contact_key,       			
-- MAGIC       count(distinct transaction_key) as TICKET,			
-- MAGIC       sum(item_total_regular_unit_price) as SALE,            			
-- MAGIC       sum(quantity) as PRODUIT			
-- MAGIC     FROM  rdm.f_transaction_detail a 
-- MAGIC     INNER JOIN (SELECT * FROM mfr_custom.mfr_product 
-- MAGIC                   WHERE KPI_EXCLUSION_FLAG='N' 
-- MAGIC                   AND product_hier_1_L1_name!='GNFR'
-- MAGIC                   AND product_hier_1_l2_name!='LIVRAISON INTERNET') b on a.product_key=b.product_key --product filter added
-- MAGIC     WHERE business_date_key between 20230621 AND 20240510
-- MAGIC     and BU_KEY='MFR'			
-- MAGIC     and CONTACT_KEY!='0'
-- MAGIC     and quantity<>0		
-- MAGIC     GROUP BY contact_key) 
-- MAGIC               
-- MAGIC     select a.contact_key,
-- MAGIC     sum(c.SALE)/ count(DISTINCT c.contact_key) as acv_apres
-- MAGIC     FROM mfr_custom.ct_notify_base_impact a 
-- MAGIC     inner JOIN salesAfter c on a.contact_key=c.contact_key --inner join to remove null acv
-- MAGIC     where a.notify_inactive_listed = 0
-- MAGIC     group by 1'''
-- MAGIC ) #CG
-- MAGIC
-- MAGIC # Create a VectorAssembler
-- MAGIC assembler = VectorAssembler(inputCols=["acv_apres"], outputCol="acv_apres_vec")
-- MAGIC
-- MAGIC # Transform the DataFrames
-- MAGIC df1 = assembler.transform(df1)
-- MAGIC df2 = assembler.transform(df2)
-- MAGIC
-- MAGIC # Calculate the means
-- MAGIC mean1 = df1.select(Summarizer.mean(df1.acv_apres_vec)).collect()[0][0][0]
-- MAGIC mean2 = df2.select(Summarizer.mean(df2.acv_apres_vec)).collect()[0][0][0]
-- MAGIC
-- MAGIC # Print the means
-- MAGIC print("Mean of the first group:", mean1)
-- MAGIC print("Mean of the second group:", mean2)
-- MAGIC
-- MAGIC # Test the equality of the means
-- MAGIC # (For a confidence level of 95%)
-- MAGIC if abs(mean1 - mean2) < 1.96 * ((df1.count() - 1) * df1.select(Summarizer.variance(df1.acv_apres_vec)).collect()[0][0][0] / df1.count() + (df2.count() - 1) * df2.select(Summarizer.variance(df2.acv_apres_vec)).collect()[0][0][0] / df2.count()) ** 0.5:
-- MAGIC     print("The means of the two groups are not statistically different (do not reject the null hypothesis)")
-- MAGIC else:
-- MAGIC     print("The means of the two groups are statistically different (reject the null hypothesis)")
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #intial py code from chatgpt 
-- MAGIC
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.ml.stat import Summarizer
-- MAGIC   
-- MAGIC # Exemple de données (remplacez-les par vos propres données)
-- MAGIC data = [(1, 12.0), (2, 15.0), (3, 18.0), (4, 20.0), (5, 22.0)]
-- MAGIC data2 = [(6, 14.0), (7, 16.0), (8, 18.0), (9, 20.0), (10, 22.0)]
-- MAGIC  
-- MAGIC # Création des DataFrames Spark à partir des données
-- MAGIC df1 = spark.createDataFrame(data, ["id", "value"])
-- MAGIC df2 = spark.createDataFrame(data2, ["id", "value"])
-- MAGIC  
-- MAGIC #failed 
-- MAGIC #AttributeError: type object 'Summarizer' has no attribute 'summary'
-- MAGIC # Calcul des statistiques
-- MAGIC summary1 = df1.select(Summarizer.summary(df1.value).mean)
-- MAGIC summary2 = df2.select(Summarizer.summary(df2.value).mean)
-- MAGIC  
-- MAGIC # Extraction des moyennes
-- MAGIC mean1 = summary1.collect()[0]["mean"]
-- MAGIC mean2 = summary2.collect()[0]["mean"]
-- MAGIC  
-- MAGIC # Affichage des moyennes
-- MAGIC print("Moyenne du premier groupe :", mean1)
-- MAGIC print("Moyenne du deuxième groupe :", mean2)
-- MAGIC  
-- MAGIC # Test de l'égalité des moyennes
-- MAGIC # (Pour un niveau de confiance de 95%)
-- MAGIC if abs(mean1 - mean2) < 1.96 * ((summary1.count() - 1) * summary1.collect()[0]["variance"] / summary1.count() + (summary2.count() - 1) * summary2.collect()[0]["variance"] / summary2.count()) ** 0.5:
-- MAGIC     print("Les moyennes des deux groupes ne sont pas statistiquement différentes (ne pas rejeter l'hypothèse nulle)")
-- MAGIC else:
-- MAGIC     print("Les moyennes des deux groupes sont statistiquement différentes (rejeter l'hypothèse nulle)")
-- MAGIC  
-- MAGIC # Arrêt de la session Spark
-- MAGIC spark.stop()
