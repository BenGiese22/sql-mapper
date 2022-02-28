
table='adjust_api.cohort_stats'

sql = """
select mmp_advertiser_id__c app_token
from salesforce.campaign
where attribution_tracking_platform__c = 'Adjust'
and mmp_advertiser_id__c ~ '^[0-9a-z]{12}$'
group by 1
order by 1 desc
"""
res = engine.execute(sql)


sql="""
--drop table if exists adjust_api.cohort_stats;
create table if not exists adjust_api.cohort_stats
(
	app_token varchar(256),
	app_name varchar(256),
	event_token varchar(256),
	event_name varchar(256),
	currency varchar(256),
	network_token varchar(256),
	network_name varchar(256),
	dt varchar(256),
	period int,
	camp_token varchar(256),
	camp_name varchar(256),
	adgroups_token varchar(256),
	adgroups_name varchar(256),
	creatives_name varchar(256),
	country varchar(256),
	os_name varchar(256),
	retained_users int,
	cohort_size int,
	sessions int encode az64,
	all_revenue decimal(12,4) encode az64,
	revenue decimal(12,4) encode az64,
	revenue_total_in_cohort decimal(12,4) encode az64,
	events bigint encode az64,
	converted_users int encode az64
)
sortkey(dt);
commit;
"""
res = engine.execute(sql)

sql="""
drop table if exists adjust_api.cohort_stats_tmp;
create table adjust_api.cohort_stats_tmp (like adjust_api.cohort_stats);
"""
res = engine.execute(sql)


#rollup tmp data
#update
sql = """
  drop table if exists adjust_api.cohort_stats_tmp2;
  create table adjust_api.cohort_stats_tmp2 (like adjust_api.cohort_stats);
"""
res = engine.execute(sql)

sql = """
      insert into adjust_api.cohort_stats_tmp2 (app_token,
	           app_name,
	           event_token,
--	           event_name,
	           currency,
	           network_token,
	           network_name,
	           dt,
	           period,
	           camp_token,
	           camp_name,
	           adgroups_token,
	           adgroups_name,
	           creatives_name,
	           country,
	           os_name,
             retained_users,
	           cohort_size,
	           sessions,
	           all_revenue,
	           revenue,
	           revenue_total_in_cohort,
	           events,
	           converted_users)
      select
      	     app_token,
	           app_name,
	           event_token,
--	           event_name,
	           currency,
	           network_token,
	           network_name,
	           dt,
	           period,
	           camp_token,
	           camp_name,
	           adgroups_token,
	           adgroups_name,
	           creatives_name,
	           country,
	           os_name,
             sum(retained_users         ) retained_users,
	           sum(cohort_size            ) cohort_size,
	           sum(sessions               ) sessions,
	           sum(all_revenue            ) all_revenue,
	           sum(revenue                ) revenue,
	           sum(revenue_total_in_cohort) revenue_total_in_cohort,
	           sum(events                 ) events,
	           sum(converted_users        ) converted_users
	      from adjust_api.cohort_stats_tmp
	     group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
"""
try:
#    print(sql)
    res = engine.execute(sql)
except:
    print("error rolling tmp data:", sys.exc_info())
    print(res)
    sys.exit()

#merge the data to source
#update
sql = """
      update adjust_api.cohort_stats
         set retained_users = b.retained_users
             , cohort_size = b.cohort_size
             , sessions = b.sessions
             , all_revenue = b.all_revenue
             , revenue = b.revenue
             , revenue_total_in_cohort = b.revenue_total_in_cohort
             , events = b.events
             , converted_users = b.converted_users
        from adjust_api.cohort_stats_tmp2 b
       where adjust_api.cohort_stats.dt                           >= '""" + str(start_date) + """'
         and adjust_api.cohort_stats.dt                           <= '""" + str(end_date) + """'
         and adjust_api.cohort_stats.dt                           = b.dt
         and adjust_api.cohort_stats.period                       = b.period
         and coalesce(adjust_api.cohort_stats.app_token     , '-99')               = coalesce(b.app_token        , '-99')
         and coalesce(adjust_api.cohort_stats.event_token   , '-99')               = coalesce(b.event_token      , '-99')
         and coalesce(adjust_api.cohort_stats.currency      , '-99')               = coalesce(b.currency         , '-99')
         and coalesce(adjust_api.cohort_stats.network_token , '-99')               = coalesce(b.network_token    , '-99')
         and coalesce(adjust_api.cohort_stats.camp_token    , '-99')               = coalesce(b.camp_token       , '-99')
         and coalesce(adjust_api.cohort_stats.adgroups_token, '-99')               = coalesce(b.adgroups_token   , '-99')
         and coalesce(adjust_api.cohort_stats.country       , '-99')               = coalesce(b.country          , '-99')
         and coalesce(adjust_api.cohort_stats.os_name       , '-99')               = coalesce(b.os_name          , '-99')
         and (   coalesce(adjust_api.cohort_stats.retained_users,0)          != coalesce(b.retained_users,0)
              or coalesce(adjust_api.cohort_stats.cohort_size,0)          != coalesce(b.cohort_size,0)
              or coalesce(adjust_api.cohort_stats.sessions,0)          != coalesce(b.sessions,0)
              or coalesce(adjust_api.cohort_stats.all_revenue,0)          != coalesce(b.all_revenue,0)
              or coalesce(adjust_api.cohort_stats.revenue,0)          != coalesce(b.revenue,0)
              or coalesce(adjust_api.cohort_stats.revenue_total_in_cohort,0)          != coalesce(b.revenue_total_in_cohort,0)
              or coalesce(adjust_api.cohort_stats.events,0)          != coalesce(b.events,0)
              or coalesce(adjust_api.cohort_stats.converted_users,0)          != coalesce(b.converted_users,0)
             )
"""

# insert
sql = """
      insert into adjust_api.cohort_stats (app_token,
	           app_name,
	           event_token,
--	           event_name,
	           currency,
	           network_token,
	           network_name,
	           dt,
	           period,
	           camp_token,
	           camp_name,
	           adgroups_token,
	           adgroups_name,
	           creatives_name,
	           country,
	           os_name,
             retained_users,
	           cohort_size,
	           sessions,
	           all_revenue,
	           revenue,
	           revenue_total_in_cohort,
	           events,
	           converted_users)
      select a.app_token,
             a.app_name,
	           a.event_token,
--	           event_name,
	           a.currency,
	           a.network_token,
	           a.network_name,
	           a.dt,
	           a.period,
	           a.camp_token,
	           a.camp_name,
	           a.adgroups_token,
	           a.adgroups_name,
	           a.creatives_name,
	           a.country,
	           a.os_name,
             a.retained_users,
	           a.cohort_size,
	           a.sessions,
	           a.all_revenue,
	           a.revenue,
	           a.revenue_total_in_cohort,
	           a.events,
	           a.converted_users
        from adjust_api.cohort_stats_tmp2 a
        left join adjust_api.cohort_stats b
          on a.dt = b.dt
          and a.period = b.period
          and coalesce(a.event_token   , '-99') = coalesce(b.event_token    , '-99')
          and coalesce(a.app_token     , '-99') = coalesce(b.app_token      , '-99')
          and coalesce(a.currency      , '-99') = coalesce(b.currency       , '-99')
          and coalesce(a.network_token , '-99') = coalesce(b.network_token  , '-99')
          and coalesce(a.camp_token    , '-99') = coalesce(b.camp_token     , '-99')
          and coalesce(a.adgroups_token, '-99') = coalesce(b.adgroups_token , '-99')
          and coalesce(a.country       , '-99') = coalesce(b.country        , '-99')
          and coalesce(a.os_name       , '-99') = coalesce(b.os_name        , '-99')
        where b.app_token is null
"""
