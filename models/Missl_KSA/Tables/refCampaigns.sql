select *,case when Type in ('Direct','Offline','Organic','Referral','others') then 'Non Paid'
              else 'Paid' end Paid_NonPaid,
              {{Country}} Halo_Country
from 
(
select *,case when Publisher in ('Direct') then 'Direct'
              when Publisher in ('BibaOffline','Facebook_Social','narvar','OfflineFB','Offline') then 'Offline'
              when Publisher in ('Organic') then 'Organic' 
              when Publisher in ('Referral') then 'Referral'
              when Publisher in ('Clickonik','Grabon','Admitad','omg','VCM','Vcommission','Affoy','Opicle','Catalyst','Adcanopus') then 'Affiliates'
              when Publisher in ('Icubes_Emailer','Icubes','Cartabandonement','MC','Netcore_SMS','Netcore_Email','Other_Email_SMS') then 'Emailer_SMS'
              when publisher in ('Facebook') then 'Facebook'
              when publisher in ('Google','bing') then 'Google'
              when publisher in ('Izooto' ) then 'Izooto'
              else 'others' end Type
from              
(              

select *,case when REGEXP_CONTAINS(source_medium,".*(?i)(direct).*") then 'Direct'   
              when REGEXP_CONTAINS(source_medium,".*(?i)bibaoffline.*") then 'BibaOffline'
              when REGEXP_CONTAINS(source_medium,".*facebook / social.*") then 'Facebook_Social'
              when REGEXP_CONTAINS(source_medium,".*(?i)narvar.*") then 'narvar'
              when REGEXP_CONTAINS(source_medium,".*(?i)offlinefb.*") then 'OfflineFB'
              when REGEXP_CONTAINS(source_medium,".*(?i)offline.*") then 'Offline'
              when REGEXP_CONTAINS(source_medium,".*(?i)organic.*") then 'Organic'
              when REGEXP_CONTAINS(source_medium,".*(?i)referral.*") then 'Referral'
              when REGEXP_CONTAINS(source_medium,".*(?i)clickonik.*") then 'Clickonik'
              when REGEXP_CONTAINS(source_medium,".*(?i)grabon.*") then 'Grabon'
              when REGEXP_CONTAINS(source_medium,".*(?i)admitad.*") then 'Admitad'
              when REGEXP_CONTAINS(source_medium,".*(?i)omg.*") then 'omg'
              when REGEXP_CONTAINS(source_medium,".*(?i)vcm.*") then 'VCM'
              when REGEXP_CONTAINS(source_medium,".*(?i)vcommission.*") then 'Vcommission'
              when REGEXP_CONTAINS(source_medium,".*(?i)affoy.*") then 'Affoy'
              when REGEXP_CONTAINS(source_medium,".*(?i)opicle.*") then 'Opicle'
              when REGEXP_CONTAINS(source_medium,".*(?i)catalyst.*") then 'Catalyst'
              when REGEXP_CONTAINS(source_medium,".*(?i)adcanopus.*") then 'Adcanopus'
              when REGEXP_CONTAINS(source_medium,".*(?i)icubes / email.*|.*(?i)icubeswire / email.*|.*(?i)icubespro / email.*") then 'Icubes_Emailer'
              when REGEXP_CONTAINS(source_medium,".*(?i)icubes.*") then 'Icubes'
              when REGEXP_CONTAINS(source_medium,".*(?i)cartabondonement.*") then 'Cartabandonement'
              when REGEXP_CONTAINS(source_medium,".*(?i)mc /.*") then 'MC'
              when REGEXP_CONTAINS(source_medium,".*(?i)netcore / sms.*|.*(?i)sms / netcore.*") then 'Netcore_SMS'
              when REGEXP_CONTAINS(source_medium,".*(?i)netcore / email*") then 'Netcore_Email'
              when REGEXP_CONTAINS(source_medium,".*(?i)email.*|.*(?i)sms.*") then 'Other_Email_SMS'
              when REGEXP_CONTAINS(source_medium,".*(?i)facebook.*") then 'Facebook'
              when REGEXP_CONTAINS(source_medium,".*(?i)google.*") then 'Google'
              when REGEXP_CONTAINS(source_medium,".*(?i)bing.*") then 'bing' 
              when REGEXP_CONTAINS(source_medium,".*(?i)izooto.*") then 'Izooto' 
              else 'Others'
              end Publisher
              
from
(

select row_number() over() campaign_id,* ,case when source_medium like '%google%' or source_medium like '%Google%' then 'Google'
                                               when source_medium like '%facebook%' or source_medium like '%Facebook%' then 'Facebook'
                                               else 'Others' end Source
from
(
select distinct campaign_name,source_medium
from `{{refKeywords}}`
)
)
)
)
