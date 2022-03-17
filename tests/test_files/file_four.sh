#!/bin/bash

#get script name
IFS='/'
read -a strarr <<< "$0"
SCRIPT_NAME=${strarr[@]: -1: 1}
unset IFS

echo Running at: `date`

set -e
(
  # Wait for lock on /var/lock/.${SCRIPT_NAME}.exclusivelock (fd 200) for 10 seconds
  flock -x -w 10 200 || exit 1

  ####################################################################################################
  # DB Connections
  ####################################################################################################
  source aa_redshift_creds.sh
  monthsback=2

  export PGPASSWORD=${AA_REDSHIFT_PASS}
  SQL="
    drop table if exists singular_base_mmp_table;
    create table if not exists singular_base_mmp_table
    (
      account_name varchar(512),
      dt date encode az64,
      app varchar(512),
      campaign_id integer encode az64,
      adn_affiliate_id varchar(512),
      publisher_site_id integer encode az64,
      publisher_site_name varchar(512),
      campaign_name varchar(512),
      platform varchar(512),
      installs bigint encode az64,
      cost numeric(12,2) encode az64,
      currency varchar(256),
      country varchar(256),
      store_id varchar(256)
    )
    diststyle even;
  "
  echo "${SQL}"
  time psql -h ${AA_REDSHIFT_HOST} -U ${AA_REDSHIFT_USER} -d ${AA_REDSHIFT_DB} -p 5439 -c "${SQL}"

  export PGPASSWORD=${AA_REDSHIFT_PASS}
  SQL="
  drop table if exists daily_rates;
  create table daily_rates as
    with dts as (select dt from ho_api.stats where dt >= date_trunc('month', current_date - interval '${monthsback} month') group by dt)
    , order_affiliates as (select affiliate_id, offer_id, goal_id from ho_api.stats where dt >= date_trunc('month', current_date - interval '${monthsback} month') group by affiliate_id, offer_id, goal_id)
    , all_combos as (select dt, affiliate_id, offer_id, goal_id from dts cross join order_affiliates)
    , rate1 as (
        select a.dt, a.affiliate_id, a.offer_id, a.goal_id, c.name, sum(revenue) rev, sum(conversions) conversions
        from all_combos a
        left join ho_api.stats b
          on a.dt = b.dt
            and a.offer_id = b.offer_id
            and a.goal_id = b.goal_id
            and a.affiliate_id = b.affiliate_id
            and b.dt >= date_trunc('month', current_date - interval '${monthsback} month')
        left join ho_api.goal c
          on a.goal_id = c.id
            and a.offer_id = c.offer_id
        group by a.dt, a.affiliate_id, a.offer_id, a.goal_id, name)
    , rate2 as (
        select dt, affiliate_id, offer_id, goal_id, name, rev, conversions,
          lag(rev,1) over (partition by affiliate_id, offer_id, goal_id order by dt) affiliate_prev_dt_rev,
          lag(conversions,1) over (partition by affiliate_id, offer_id, goal_id order by dt) affiliate_prev_dt_convs,
          sum(rev) over (partition by offer_id, goal_id, dt) goal_cur_dt_rev,
          sum(conversions) over (partition by offer_id, goal_id, dt) goal_cur_dt_convs,
          sum(rev) over (partition by offer_id, goal_id) goal_total_rev,
          sum(conversions) over (partition by offer_id, goal_id) goal_total_convs
      from rate1)
    , rate3 as (
        select dt, affiliate_id, offer_id, goal_id, name, rev, conversions,
          case when conversions > 0 then round(rev::float/conversions::float,2) end affiliate_current_dt_rate,
          case when affiliate_prev_dt_convs > 0 then round(affiliate_prev_dt_rev::float/affiliate_prev_dt_convs::float,2) end affiliate_previous_dt_rate,
          case when goal_cur_dt_convs > 0 then round(goal_cur_dt_rev::float/goal_cur_dt_convs::float,2) end goal_current_dt_rate,
          case when goal_total_convs > 0 then round(goal_total_rev::float/goal_total_convs::float,2) end goal_all_time_rate
        from rate2)
    select *, coalesce(affiliate_current_dt_rate, affiliate_previous_dt_rate, goal_current_dt_rate, goal_all_time_rate) as rate from rate3
  "
  echo "${SQL}"
  time psql -h ${AA_REDSHIFT_HOST} -U ${AA_REDSHIFT_USER} -d ${AA_REDSHIFT_DB} -p 5439 -c "${SQL}"

  export PGPASSWORD=${AA_REDSHIFT_PASS}
  SQL="
  drop table if exists singular_rates;
  create table singular_rates as
      with dts as (select dt from ho_api.stats where dt >= date_trunc('month', current_date - interval '${monthsback} month') and goal_id = '0' group by 1)
      , offer_affiliates as (select offer_id, affiliate_id from ho_api.stats where dt >= date_trunc('month', current_date - interval '${monthsback} month') and goal_id = '0' group by 1,2)
      , all_combos as (select dt, offer_id, affiliate_id from dts cross join offer_affiliates)
      , rate1 as (
          select a.dt, a.offer_id,
                 a.affiliate_id,
                 sum(revenue) rev,
                 sum(conversions) conversions
            from all_combos a left join ho_api.stats b
              on a.dt = b.dt
             and a.offer_id = b.offer_id
             and a.affiliate_id = b.affiliate_id
             and b.dt >= date_trunc('month', current_date - interval '${monthsback} month')
             and b.goal_id = '0'
          group by 1,2,3
      --     order by 2,3,1
      --     limit 10000
    )
    , rate2 as (
      select dt,
             offer_id,
             affiliate_id,
             rev,
             conversions,
             lag(rev,1) over (partition by affiliate_id, offer_id order by dt) prev_rev,
             lag(conversions,1) over (partition by affiliate_id, offer_id order by dt) prev_conversions,
             sum(rev) over (partition by offer_id, dt) offer_dt_rev,
             sum(conversions) over (partition by offer_id, dt) offer_dt_conversions,
             sum(rev) over (partition by offer_id) offer_rev,
             sum(conversions) over (partition by offer_id) offer_conversions
      from rate1
    ),
    rate3 as (
      select dt,
             offer_id as campaign_id,
             affiliate_id as publisher_site_id,
             rev, conversions,
             case when conversions > 0 then rev::float/conversions::float end r1,
             case when prev_conversions > 0 then prev_rev::float/prev_conversions::float end r2,
             case when offer_dt_conversions > 0 then offer_dt_rev::float/offer_dt_conversions::float end r3,
             case when offer_conversions > 0 then offer_rev::float/offer_conversions::float end r4
      from rate2
    )
    select *, coalesce(r1, r2, r3, r4) rate
    from rate3
  "
  echo "${SQL}"
  time psql -h ${AA_REDSHIFT_HOST} -U ${AA_REDSHIFT_USER} -d ${AA_REDSHIFT_DB} -p 5439 -c "${SQL}"

  export PGPASSWORD=${AA_REDSHIFT_PASS}
  SQL="
    -------------------------------
    --appsflyer cpe:
    drop table if exists singular_appsflyer_cpe;
    create table singular_appsflyer_cpe (like singular_base_mmp_table);

    --insert from appsflyer_postbacks
    insert into singular_appsflyer_cpe
    with singular_appsflyer_cpe_conversions as 
    (
        SELECT
            replace(replace(s.name, ' ', ''),',','_') as account_name
        ,   event_time::date as dt
        ,   a.app_name as app
        ,   left(sub_param_5, 5)::int as campaign_id
        ,   a.affiliate_id as publisher_site_id
        ,   source as publisher_site_name
        ,   c.name as campaign_name
        ,   case
                when lower(platform) like '%iphone%'
                    or lower(platform) like '%ipod%'
                    or lower(platform) like '%ipad%'
                    or lower(platform) like '%ios%'
                    then 'iOS'
                when lower(platform) like '%android%'
                    then 'Android'
                else 'Other'
            end as platform
        ,   country_code as country
        ,   count(1) as installs
        ,   sum(dr.rate) as cost
        FROM appsflyer_postbacks a
        INNER JOIN salesforce.campaign c
            ON left(a.sub_param_5, 5) = c.ho_offer_id__c
                and c.singular_cost__c = 'True'
                and c.advertiser_id__c != '1928'
                and c.advertiser_id__c != '713'
        INNER JOIN daily_rates dr
            ON a.affiliate_id = dr.affiliate_id 
                and a.campaign_id = dr.offer_id 
                and a.event_time::date = dr.dt 
                and LOWER(a.event_name) = LOWER(case when a.media_source = 'adaction2_int' then c.event_token__c else coalesce(dr.name, 'install') end)
        LEFT JOIN salesforce.account s
            ON c.advertiser_id__c = s.advertiser_id__c
        WHERE date(event_time) >= date_trunc('month', current_date - interval '${monthsback} month')
            AND (left(sub_param_5, 5) ~ '^[0-9\.]+$')
            AND upper(a.campaign_type) = 'CPE'
            AND postback_http_response_code like '200'
            AND dr.rate > 0 
        GROUP BY 1,2,3,4,5,6,7,8,9
    )
    SELECT 
        a.account_name
    ,   a.dt
    ,   a.app
    ,   a.campaign_id
    ,   a.publisher_site_id::varchar + '_' + publisher_site_name::varchar as adn_affiliate_id
    ,   a.publisher_site_id
    ,   a.publisher_site_name
    ,   a.campaign_name
    ,   a.platform
    ,   a.installs
    ,   a.cost
    ,   'USD' as currency
    ,   a.country
    ,   x.app_id as store_id
    FROM singular_appsflyer_cpe_conversions a
    LEFT JOIN ho_api.offer_apps x
        ON a.campaign_id = x.offer_id::int;

    --insert from appsflyer_postbacks_king
    insert into singular_appsflyer_cpe
    with singular_appsflyer_cpe_conversions as 
    (
        select 
            replace(replace(s.name, ' ', ''),',','_') as account_name
        ,   event_time::date as dt
        ,   a.app_name as app
        ,   left(sub_param_5, 5)::int as campaign_id
        ,   affiliate_id as publisher_site_id
        ,   source as publisher_site_name
        ,   c.name as campaign_name
        ,   case
                when lower(platform) like '%iphone%'
                    or lower(platform) like '%ipod%'
                    or lower(platform) like '%ipad%'
                    or lower(platform) like '%ios%'
                    then 'iOS'
                when lower(platform) like '%android%'
                    then 'Android'
                else 'Other'
            end as platform
        ,   country_code as country
        ,   count(1) as installs
        ,   sum(revenue) as revenue
        FROM appsflyer_postbacks_king a
        INNER JOIN salesforce.campaign c
            ON left(a.sub_param_5, 5) = c.ho_offer_id__c
                AND c.singular_cost__c = 'True'
                AND c.advertiser_id__c = '713'
                AND af_level = 20
                AND posted_conversion_to_tune = True
        LEFT JOIN salesforce.account s
            ON c.advertiser_id__c = s.advertiser_id__c
        WHERE date(event_time) >= date_trunc('month', current_date - interval '${monthsback} month')
            AND (left(sub_param_5, 5) ~ '^[0-9\.]+$')
            AND upper(a.campaign_type) = 'CPE'
            AND lower(a.event_name) = lower(c.event_token__c)
            AND postback_http_response_code like '200'
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    )
    SELECT 
        a.account_name
    ,   a.dt
    ,   a.app
    ,   a.campaign_id
    ,   a.publisher_site_id::varchar + '_' + publisher_site_name::varchar as adn_affiliate_id
    ,   a.publisher_site_id
    ,   a.publisher_site_name
    ,   a.campaign_name
    ,   a.platform
    ,   a.installs
    ,   a.revenue::numeric(12,2) as cost
    ,   'USD' as currency
    ,   a.country
    ,   x.app_id as store_id
    FROM singular_appsflyer_cpe_conversions a
    LEFT JOIN ho_api.offer_apps x
        ON a.campaign_id = x.offer_id::int;

    GRANT SELECT ON singular_appsflyer_cpe to public;
  "
  echo "${SQL}"
  time psql -h ${AA_REDSHIFT_HOST} -U ${AA_REDSHIFT_USER} -d ${AA_REDSHIFT_DB} -p 5439 -c "${SQL}"

  export PGPASSWORD=${AA_REDSHIFT_PASS}
  SQL="
    -------------------------------
    -- singular_appsflyer_cpi
    drop table if exists singular_appsflyer_cpi;
    create table singular_appsflyer_cpi (like singular_base_mmp_table);

    insert into  singular_appsflyer_cpi
    with singular_appsflyer_cpi_conversions as
    (
        SELECT 
            replace(replace(s.name, ' ', ''),',','_') as account_name
        ,   event_time::date as dt
        ,   a.app_name as app
        ,   left(sub_param_5, 5)::int as campaign_id
        ,   a.affiliate_id as publisher_site_id
        ,   source as publisher_site_name
        ,   c.name as campaign_name
        ,   case
                when lower(platform) like '%iphone%'
                    or lower(platform) like '%ipod%'
                    or lower(platform) like '%ipad%'
                    or lower(platform) like '%ios%'
                    then 'iOS'
                when lower(platform) like '%android%'
                    then 'Android'
                else 'Other'
            end as platform
        ,   country_code as country
        ,   count(1) as installs
        ,   sum(dr.rate) as cost
        FROM appsflyer_postbacks a
        INNER JOIN salesforce.campaign c
            ON left(a.sub_param_5, 5) = c.ho_offer_id__c
                and c.singular_cost__c = 'True'
                and c.advertiser_id__c != '1928'
        INNER JOIN daily_rates dr
            ON a.affiliate_id = dr.affiliate_id 
                and a.campaign_id = dr.offer_id 
                and a.event_time::date = dr.dt 
                and LOWER(a.event_name) = LOWER(case when a.media_source = 'adaction2_int' then c.event_token__c else coalesce(dr.name, 'install') end)
        LEFT JOIN salesforce.account s
            ON c.advertiser_id__c = s.advertiser_id__c
        WHERE date(event_time) >= date_trunc('month', current_date - interval '${monthsback} month')
            AND (left(sub_param_5, 5) ~ '^[0-9\.]+$')
            AND upper(a.campaign_type) = 'CPI'
            AND postback_http_response_code like '200'
            AND dr.rate > 0 
        GROUP BY 1,2,3,4,5,6,7,8,9
    )
    SELECT 
        a.account_name
    ,   a.dt
    ,   a.app
    ,   a.campaign_id
    ,   a.publisher_site_id::varchar + '_' + publisher_site_name::varchar as adn_affiliate_id
    ,   a.publisher_site_id
    ,   a.publisher_site_name
    ,   a.campaign_name
    ,   a.platform
    ,   a.installs
    ,   a.cost
    ,   'USD' as currency
    ,   a.country
    ,   x.app_id as store_id
    FROM singular_appsflyer_cpi_conversions a
    LEFT JOIN ho_api.offer_apps x
        ON a.campaign_id = x.offer_id::int;

    --insert from appsflyer_postbacks_king
    insert into  singular_appsflyer_cpi
    with singular_appsflyer_cpi_conversions as (
        select replace(replace(s.name, ' ', ''),',','_') account_name,
               event_time::date dt,
               a.app_name                               app,
               left(sub_param_5, 5)::int                campaign_id,
               affiliate_id                             publisher_site_id,
               source                                   publisher_site_name,
               c.name                                   campaign_name,
               case
                   when lower(platform) like '%iphone%'
                       or lower(platform) like '%ipod%'
                       or lower(platform) like '%ipad%'
                       or lower(platform) like '%ios%'
                       then 'iOS'
                   when lower(platform) like '%android%'
                       then 'Android'
                   else 'Other'
                   end                                  platform,
               country_code                             country,
               count(1)                                 installs,
               sum(revenue)                             revenue
        from appsflyer_postbacks_king a
        join salesforce.campaign c
          on left(a.sub_param_5, 5) = c.ho_offer_id__c
         and c.singular_cost__c = 'True'
         and c.advertiser_id__c = '713'
         and af_level = 20
         and posted_conversion_to_tune = True
        left join salesforce.account s
          on c.advertiser_id__c = s.advertiser_id__c
    where date(event_time) >= date_trunc('month', current_date - interval '${monthsback} month')
            and (left(sub_param_5, 5) ~ '^[0-9\.]+$')
            and upper(a.campaign_type) = 'CPI'
            and lower(a.event_name) = 'install'
            and postback_http_response_code like '200'
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9
    )
    select a.account_name, a.dt, a.app, a.campaign_id, a.publisher_site_id::varchar + '_' + publisher_site_name::varchar as adn_affiliate_id, a.publisher_site_id, a.publisher_site_name, a.campaign_name, a.platform, a.installs, (a.revenue)::numeric(12,2) cost, 'USD' currency, a.country, x.app_id store_id
      from singular_appsflyer_cpi_conversions a
      left join ho_api.offer_apps x on a.campaign_id = x.offer_id::int;
    grant select on singular_appsflyer_cpi to public;
  "
  echo "${SQL}"
  time psql -h ${AA_REDSHIFT_HOST} -U ${AA_REDSHIFT_USER} -d ${AA_REDSHIFT_DB} -p 5439 -c "${SQL}"

  export PGPASSWORD=${AA_REDSHIFT_PASS}
  SQL="
    --adjust cpe:
    drop table if exists singular_adjust_cpe;
    create table singular_adjust_cpe (like singular_base_mmp_table);

    insert into singular_adjust_cpe
      with singular_adjust_cpe_conversions as (
        select replace(replace(s.name, ' ', ''),',','_') account_name,
               date(a.dt) dt,
               a.app_name app,
               c.ho_offer_id__c::int                                                        campaign_id,
               case
                   when a.adgroups_name like 'unknown' then NULL
                   when a.adgroups_name ~ '^[0-9a-z]{4}' then left(a.adgroups_name, 4)
                   end publisher_site_id,
               replace(
                   case
                       when a.adgroups_name like 'unknown' then NULL
                       when a.adgroups_name ~ '^[0-9a-z]{4}' then SUBSTRING(adgroups_name FROM 5) end, '_','') publisher_site_name,
               a.camp_name campaign_name,
               case
                   when lower(os_name) like '%android%' then 'Android'
                   when lower(os_name) like '%ios%' then 'iOS'
                   when lower(os_name) in ('linux', 'macos', 'windows', 'unknown') then 'Desktop'
                   end                                                                      platform,
                   upper(country)                                                           country,
               sum(events)                                                                  installs
        from adjust_api.event_stats a
                 join salesforce.campaign c
                      on c.attribution_tracking_platform__c = 'Adjust'
                          and c.mmp_advertiser_id__c ~ '^[0-9a-z]{12}$'
                          and c.mmp_advertiser_id__c = a.app_token
                          and c.event_token__c = a.event_token
                          and
                         (lower(c.ho_offer_id__c) = lower(a.camp_name) or lower(c.ho_offer_name__c) = lower(a.camp_name))
                          and c.billing_type__c = 'CPE'
                          and c.singular_cost__c = 'True'
                          and dt >= date_trunc('month', current_date - interval '${monthsback} month')
                          and lower(a.network_name) not like '%cpi%'
                 left join salesforce.account s
                   on c.advertiser_id__c = s.advertiser_id__c
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9
    )
    select a.account_name, a.dt, a.app, a.campaign_id, a.publisher_site_id::varchar + '_' + publisher_site_name::varchar as adn_affiliate_id, a.publisher_site_id::int, a.publisher_site_name, a.campaign_name, a.platform, a.installs, (b.rate*a.installs)::numeric(12,2) cost, 'USD' currency, a.country, x.app_id store_id
      from singular_adjust_cpe_conversions a
      left join singular_rates b on a.campaign_id = b.campaign_id and a.publisher_site_id = b.publisher_site_id and a.dt = b.dt
      left join ho_api.offer_apps x on a.campaign_id = x.offer_id::int;
    grant select on singular_adjust_cpe to public;
  "
  echo "${SQL}"
  time psql -h ${AA_REDSHIFT_HOST} -U ${AA_REDSHIFT_USER} -d ${AA_REDSHIFT_DB} -p 5439 -c "${SQL}"

  export PGPASSWORD=${AA_REDSHIFT_PASS}
  SQL="
    drop table if exists singular_adjust_cpi;
    create table singular_adjust_cpi (like singular_base_mmp_table);

    insert into singular_adjust_cpi
        select replace(replace(s.name, ' ', ''),',','_') account_name,
               date(a.dt) dt,
               a.app_name app,
               c.ho_offer_id__c::int                                                        campaign_id,
               case when a.adgroups_name ~ '^[0-9a-z]{4}' then left(a.adgroups_name, 4) end + '_' +  replace(case when a.adgroups_name ~ '^[0-9a-z]{4}' then SUBSTRING(adgroups_name FROM 5) end, '_','') as adn_affiliate_id,
               case when a.adgroups_name ~ '^[0-9]{4}' then left(a.adgroups_name, 4) end::int publisher_site_id,
               replace(case when a.adgroups_name ~ '^[0-9a-z]{4}' then SUBSTRING(adgroups_name FROM 5) end, '_','') publisher_site_name,
               a.camp_name campaign_name,
               case
                   when lower(os_name) like '%android%' then 'Android'
                   when lower(os_name) like '%ios%' then 'iOS'
                   when lower(os_name) in ('linux', 'macos', 'windows', 'unknown') then 'Desktop'
                   end                                                                      platform,
               sum(installs) installs,
               sum(cost)::numeric(12,2) cost,
               currency,
               upper(country) country,
               x.app_id store_id
        from adjust_api.stats a
        join salesforce.campaign c
                      on c.attribution_tracking_platform__c = 'Adjust'
                          and c.mmp_advertiser_id__c ~ '^[0-9a-z]{12}$'
                          and c.mmp_advertiser_id__c = a.app_token
                          and (lower(c.ho_offer_id__c) = lower(a.camp_name) or lower(c.ho_offer_name__c) = lower(a.camp_name))
                          and coalesce(c.billing_type__c, 'CPI') != 'CPE'
                          and c.singular_cost__c = 'True'
                          and dt >= date_trunc('month', current_date - interval '${monthsback} month')
         left join salesforce.account s
           on c.advertiser_id__c = s.advertiser_id__c
         left join ho_api.offer_apps x on c.ho_offer_id__c::int = x.offer_id::int
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9,12,13,14
    ;
    grant select on singular_adjust_cpi to public;
  "
  echo "${SQL}"
  time psql -h ${AA_REDSHIFT_HOST} -U ${AA_REDSHIFT_USER} -d ${AA_REDSHIFT_DB} -p 5439 -c "${SQL}"

  #singular 
  export PGPASSWORD=${AA_REDSHIFT_PASS}
  SQL="
    drop table if exists singular_clients_cpe;
    create table singular_clients_cpe (like singular_base_mmp_table);

    insert into singular_clients_cpe
      with singular_clients_cpe_conversions as (
        select 
               replace(replace(s.name, ' ', ''),',','_') account_name,
               date(a.end_date) dt,
               a.app,
               a.adn_campaign_id::int                                                        campaign_id,
               a.publisher_site_id,
               a.publisher_site_name,
               a.adn_campaign_name campaign_name,
               case
                   when lower(os) like '%android%' then 'Android'
                   when lower(os) like '%ios%' then 'iOS'
                   when lower(os) in ('linux', 'macos', 'windows', 'unknown') then 'Desktop'
                   end                                                                      platform,
                   upper(country_field)                                                     country,
               sum(case
                   when a.account_name = 'appverse' then cohort_period_7d
                   when a.account_name = 'current_media' then adn_cost
                   when a.account_name = 'lbc_studios' then cohort_period_actual
               end) installs
        from singular_source a
                 join salesforce.campaign c
                      on c.attribution_tracking_platform__c = 'Singular'
                      and lower(c.ho_offer_id__c) = lower(a.adn_campaign_id)
                          and c.billing_type__c = 'CPE'
                          and c.singular_cost__c = 'True'
                          and end_date >= date_trunc('month', current_date - interval '${monthsback} month')
                 left join salesforce.account s
                   on c.advertiser_id__c = s.advertiser_id__c
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9
    )
    select a.account_name, a.dt, a.app, a.campaign_id, a.publisher_site_id::varchar + '_' + publisher_site_name::varchar as adn_affiliate_id, a.publisher_site_id::int, a.publisher_site_name, a.campaign_name, a.platform, a.installs, (b.rate*a.installs)::numeric(12,2) cost, 'USD' currency, a.country, x.app_id store_id
      from singular_clients_cpe_conversions a
      left join singular_rates b on a.campaign_id = b.campaign_id and a.publisher_site_id = b.publisher_site_id and a.dt = b.dt
      left join ho_api.offer_apps x on a.campaign_id = x.offer_id::int;
    grant select on singular_clients_cpe to public;
  "
  echo "${SQL}"
  time psql -h ${AA_REDSHIFT_HOST} -U ${AA_REDSHIFT_USER} -d ${AA_REDSHIFT_DB} -p 5439 -c "${SQL}"

  export PGPASSWORD=${AA_REDSHIFT_PASS}
  SQL="
    drop table if exists singular_clients_cpi;
    create table singular_clients_cpi (like singular_base_mmp_table);

    insert into singular_clients_cpi
      with singular_clients_cpi_conversions as (
        select replace(replace(s.name, ' ', ''),',','_') account_name,
               date(a.end_date) dt,
               a.app,
               a.adn_campaign_id::int                                                        campaign_id,
               a.publisher_site_id,
               a.publisher_site_name,
               a.adn_campaign_name campaign_name,
               case
                   when lower(os) like '%android%' then 'Android'
                   when lower(os) like '%ios%' then 'iOS'
                   when lower(os) in ('linux', 'macos', 'windows', 'unknown') then 'Desktop'
                   end                                                                      platform,
               upper(country_field)                                                     country,
               sum(custom_installs) installs
        from singular_source a
                 join salesforce.campaign c
                      on c.attribution_tracking_platform__c = 'Singular'
                      and lower(c.ho_offer_id__c) = lower(a.adn_campaign_id)
                          and upper(c.billing_type__c) = 'CPI'
                          and c.singular_cost__c = 'True'
                          and end_date >= date_trunc('month', current_date - interval '${monthsback} month')
                 left join salesforce.account s
                   on c.advertiser_id__c = s.advertiser_id__c
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9
    )
    select a.account_name, a.dt, a.app, a.campaign_id, a.publisher_site_id::varchar + '_' + publisher_site_name::varchar as adn_affiliate_id, a.publisher_site_id::int, a.publisher_site_name, a.campaign_name, a.platform, a.installs, (b.rate*a.installs)::numeric(12,2) cost, 'USD' currency, a.country, x.app_id store_id
      from singular_clients_cpi_conversions a
      left join singular_rates b on a.campaign_id = b.campaign_id and a.publisher_site_id = b.publisher_site_id and a.dt = b.dt
      left join ho_api.offer_apps x on a.campaign_id = x.offer_id::int;
    grant select on singular_clients_cpi to public;
  "
  echo "${SQL}"
  time psql -h ${AA_REDSHIFT_HOST} -U ${AA_REDSHIFT_USER} -d ${AA_REDSHIFT_DB} -p 5439 -c "${SQL}"

  #kochava
  export PGPASSWORD=${AA_REDSHIFT_PASS}
  SQL="
    drop table if exists singular_kochava_cpe;
    create table singular_kochava_cpe (like singular_base_mmp_table);

    insert into singular_kochava_cpe
      with singular_kochava_cpe_conversions as (
        select replace(replace(s.name, ' ', ''),',','_')                                                            account_name,
               date(a.install_date_utc)                                                                             dt,
               ''                                                                                                   app,
               a.offer_id::int                                                                                           campaign_id,
               a.attribution_network                                                                                publisher_site_id,
               a.attribution_site                                                                                   publisher_site_name,
               a.attribution_campaign_name                                                                          campaign_name,
               case
                   when lower(attribution_network_name) like '%android%' then 'Android'
                   when lower(attribution_network_name) like '%ios%' then 'iOS'
                   when lower(attribution_network_name) in ('linux', 'macos', 'windows', 'unknown') then 'Desktop'
                   end                                                                      platform,
                   upper(installgeo_country_code)                                                           country,
               count(1)                                                                  installs,
               sum(bid_won) cost
        from kochavas a
                 join salesforce.campaign c
                      on c.attribution_tracking_platform__c = 'Kochava'
                          and a.offer_id = c.ho_offer_id__c
                          and c.billing_type__c = 'CPE'
                          and c.singular_cost__c = 'True'
                          and a.install_date_utc >= date_trunc('month', current_date - interval '${monthsback} month')
                 left join salesforce.account s
                   on c.advertiser_id__c = s.advertiser_id__c
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9
    )
    select a.account_name, a.dt, a.app, a.campaign_id, a.publisher_site_id::varchar + '_' + publisher_site_name::varchar as adn_affiliate_id, a.publisher_site_id::int, a.publisher_site_name, a.campaign_name, a.platform, a.installs, (a.cost)::numeric(12,2) cost, 'USD' currency, a.country, x.app_id store_id
      from singular_kochava_cpe_conversions a
      left join singular_rates b on a.campaign_id = b.campaign_id and a.publisher_site_id = b.publisher_site_id and a.dt = b.dt
      left join ho_api.offer_apps x on a.campaign_id = x.offer_id::int;
    grant select on singular_kochava_cpe to public;
  "
  echo "${SQL}"
  time psql -h ${AA_REDSHIFT_HOST} -U ${AA_REDSHIFT_USER} -d ${AA_REDSHIFT_DB} -p 5439 -c "${SQL}"

  export PGPASSWORD=${AA_REDSHIFT_PASS}
  SQL="
    drop table if exists singular_kochava_cpi;
    create table singular_kochava_cpi (like singular_base_mmp_table);

    insert into singular_kochava_cpi
      with singular_kochava_cpi_conversions as (
        select replace(replace(s.name, ' ', ''),',','_')                                                            account_name,
               date(a.install_date_utc)                                                                             dt,
               ''                                                                                                   app,
               a.offer_id::int                                                                                           campaign_id,
               a.attribution_network                                                                                publisher_site_id,
               a.attribution_site                                                                                   publisher_site_name,
               a.attribution_campaign_name                                                                          campaign_name,
               case
                   when lower(attribution_network_name) like '%android%' then 'Android'
                   when lower(attribution_network_name) like '%ios%' then 'iOS'
                   when lower(attribution_network_name) in ('linux', 'macos', 'windows', 'unknown') then 'Desktop'
                   end                                                                      platform,
                   upper(installgeo_country_code)                                                           country,
               count(1)                                                                  installs,
               sum(bid_won) cost
        from kochavas a
                 join salesforce.campaign c
                      on c.attribution_tracking_platform__c = 'Kochava'
                          and a.offer_id = c.ho_offer_id__c
                          and c.billing_type__c = 'CPI'
                          and c.singular_cost__c = 'True'
                          and a.install_date_utc >= date_trunc('month', current_date - interval '${monthsback} month')
                 left join salesforce.account s
                   on c.advertiser_id__c = s.advertiser_id__c
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9
    )
    select a.account_name, a.dt, a.app, a.campaign_id, a.publisher_site_id::varchar + '_' + publisher_site_name::varchar as adn_affiliate_id, a.publisher_site_id::int, a.publisher_site_name, a.campaign_name, a.platform, a.installs, (a.cost)::numeric(12,2) cost, 'USD' currency, a.country, x.app_id store_id
      from singular_kochava_cpi_conversions a
      left join singular_rates b on a.campaign_id = b.campaign_id and a.publisher_site_id = b.publisher_site_id and a.dt = b.dt
      left join ho_api.offer_apps x on a.campaign_id = x.offer_id::int;
    grant select on singular_kochava_cpi to public;
  "
  echo "${SQL}"
  time psql -h ${AA_REDSHIFT_HOST} -U ${AA_REDSHIFT_USER} -d ${AA_REDSHIFT_DB} -p 5439 -c "${SQL}"

  export PGPASSWORD=${AA_REDSHIFT_PASS}
  SQL="
    drop table if exists doubledown_rates;
    create table doubledown_rates as
    select dt, offer_id campaign_id, affiliate_id publisher_site_id, goal_id, sum(revenue)/sum(conversions) as rate, sum(conversions) as conversions,sum(revenue) as revenue
    from ho_api.stats
    where dt >= date_trunc('month', current_date - interval '${monthsback} month')
    and affiliate_id <> 2
    and advertiser_id = 1928
    and revenue <> 0
    group by 1,2,3,4;
  "
  echo "${SQL}"
  time psql -h ${AA_REDSHIFT_HOST} -U ${AA_REDSHIFT_USER} -d ${AA_REDSHIFT_DB} -p 5439 -c "${SQL}"

  #DoubleDown logic
  export PGPASSWORD=${AA_REDSHIFT_PASS}
  SQL="
    -------------------------------
    -- doubledown_appsflyer
    drop table if exists doubledown_appsflyer;
    create table doubledown_appsflyer (like singular_base_mmp_table);

    insert into doubledown_appsflyer
    with doubledown_appsflyer_conversions as (
        select replace(replace(s.name, ' ', ''),',','_') account_name,
               event_time::date dt,
               a.app_name                               app,
               left(sub_param_5, 5)::int                campaign_id,
               affiliate_id                             publisher_site_id,
               source                                   publisher_site_name,
               c.name                                   campaign_name,
               case
                   when lower(platform) like '%iphone%'
                       or lower(platform) like '%ipod%'
                       or lower(platform) like '%ipad%'
                       or lower(platform) like '%ios%'
                       then 'iOS'
                   when lower(platform) like '%android%'
                       then 'Android'
                   else 'Other'
                   end                                  platform,
               country_code                             country,
               count(1)                                 installs
        from appsflyer_postbacks a
        join salesforce.campaign c
          on left(a.sub_param_5, 5) = c.ho_offer_id__c
         and c.singular_cost__c = 'True'
         and c.advertiser_id__c = '1928'
        left join salesforce.account s
          on c.advertiser_id__c = s.advertiser_id__c
    where date(event_time) >= date_trunc('month', current_date - interval '${monthsback} month')
            and (left(sub_param_5, 5) ~ '^[0-9\.]+$')
            and lower(a.event_name) = lower(c.event_token__c)
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9
    )
    select a.account_name, a.dt, a.app, a.campaign_id, a.publisher_site_id::varchar + '_' + publisher_site_name::varchar as adn_affiliate_id, a.publisher_site_id, a.publisher_site_name, a.campaign_name, a.platform, a.installs, (coalesce(b.rate,y.rate)*a.installs)::numeric(12,2) cost, 'USD' currency, a.country, x.app_id store_id
      from doubledown_appsflyer_conversions a
      left join doubledown_rates b on a.campaign_id = b.campaign_id and a.publisher_site_id = b.publisher_site_id and a.dt = b.dt
      left join singular_rates y on a.campaign_id = y.campaign_id and a.publisher_site_id = y.publisher_site_id and a.dt = y.dt
      left join ho_api.offer_apps x on a.campaign_id = x.offer_id::int;
    grant select on singular_appsflyer_cpe to public;
  "
  echo "${SQL}"
  time psql -h ${AA_REDSHIFT_HOST} -U ${AA_REDSHIFT_USER} -d ${AA_REDSHIFT_DB} -p 5439 -c "${SQL}"

  export PGPASSWORD=${AA_REDSHIFT_PASS}
  for mmp in clients adjust appsflyer kochava; do
      echo $mmp
      for mnth in `psql -h ${AA_REDSHIFT_HOST} -U ${AA_REDSHIFT_USER} -d ${AA_REDSHIFT_DB} -p 5439 -t -c "select to_char(dt, 'YYYYMM') mnth from singular_${mmp}_cpe group by 1 union select to_char(dt, 'YYYYMM') mnth from singular_${mmp}_cpi group by 1 union select to_char(dt, 'YYYYMM') mnth from doubledown_appsflyer group by 1"`;do
          echo $mnth

          for adv in `psql -h ${AA_REDSHIFT_HOST} -U ${AA_REDSHIFT_USER} -d ${AA_REDSHIFT_DB} -p 5439 -t -c "select account_name from singular_${mmp}_cpe where to_char(dt, 'YYYYMM') = '${mnth}' group by 1 union select account_name from singular_${mmp}_cpi where to_char(dt, 'YYYYMM') = '${mnth}' group by 1 union select account_name from doubledown_appsflyer where to_char(dt, 'YYYYMM') = '${mnth}' and '${mmp}' = 'appsflyer' group by 1"`;do
            echo $adv
            mod_adv=${adv/'+'/'_'}
            mod_adv=$(echo "$mod_adv" | sed -e 's@/@_@g')
            mod_adv=${mod_adv/'.'/''}
            echo $mod_adv

            SQL="
              drop table if exists tmp.singular_export_${mmp}_${mod_adv}_${mnth};
              create table tmp.singular_export_${mmp}_${mod_adv}_${mnth} (like singular_kochava_cpi);
            "
            echo "${SQL}"
            time psql -h ${AA_REDSHIFT_HOST} -U ${AA_REDSHIFT_USER} -d ${AA_REDSHIFT_DB} -p 5439 -c "${SQL}"

            SQL="
              insert into tmp.singular_export_${mmp}_${mod_adv}_${mnth} select * from singular_${mmp}_cpe where account_name = '${adv}' and to_char(dt, 'YYYYMM') = '${mnth}';
            "
            echo "${SQL}"
            time psql -h ${AA_REDSHIFT_HOST} -U ${AA_REDSHIFT_USER} -d ${AA_REDSHIFT_DB} -p 5439 -c "${SQL}"


            SQL="
              insert into tmp.singular_export_${mmp}_${mod_adv}_${mnth} select * from singular_${mmp}_cpi where account_name = '${adv}' and to_char(dt, 'YYYYMM') = '${mnth}';
            "
            echo "${SQL}"
            time psql -h ${AA_REDSHIFT_HOST} -U ${AA_REDSHIFT_USER} -d ${AA_REDSHIFT_DB} -p 5439 -c "${SQL}"


            SQL="
              insert into tmp.singular_export_${mmp}_${mod_adv}_${mnth} select * from doubledown_appsflyer where account_name = '${adv}' and to_char(dt, 'YYYYMM') = '${mnth}' and '${mmp}' = 'appsflyer';
            "
            echo "${SQL}"
            time psql -h ${AA_REDSHIFT_HOST} -U ${AA_REDSHIFT_USER} -d ${AA_REDSHIFT_DB} -p 5439 -c "${SQL}"

            SQL="
              unload ('
                select * from tmp.singular_export_${mmp}_${mod_adv}_${mnth}
              ') to 's3://aa-singular/${mod_adv}/${mod_adv}_${mmp}_${mnth}_'
              credentials 'aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}'
              delimiter ',' addquotes allowoverwrite region as 'us-west-2' PARALLEL OFF HEADER;
            "
            time psql -h ${AA_REDSHIFT_HOST} -U ${AA_REDSHIFT_USER} -d ${AA_REDSHIFT_DB} -p 5439 -c "${SQL}"

          done
      done
  done

) 200>/var/lock/.${SCRIPT_NAME}.exclusivelock

exit
