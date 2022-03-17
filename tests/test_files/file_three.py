#!/home/ec2-user/anaconda3/bin/python

sql = """
select mmp_advertiser_id__c app_token
from salesforce.campaign
where attribution_tracking_platform__c = 'Adjust'
and mmp_advertiser_id__c ~ '^[0-9a-z]{12}$'
group by 1 
order by 1
"""       
res = engine.execute(sql)



sql="""
drop table if exists adjust_api.stats_tmp;
create table adjust_api.stats_tmp (like adjust_api.stats);
"""
res = engine.execute(sql)



for app_token in app_tokens:
    print('app_token=',app_token)
#    if app_token != '2wu3cpcllzwj':
#        continue

    print('---')

    if str(response)=='<Response [200]>':
        output=response.json()
        results=output['result_set']
        kpis=output['result_parameters']['kpis']
        #grouping=output['result_parameters']['grouping']
        attribution_source = output['result_parameters']['attribution_source']

        try:
            apps=results['apps']
            #print(apps)
        except:
            continue

        kpi_string = ', '.join(str(x) for x in kpis)
        sqlhead='app_token, app_name, currency, network_token, network_name, dt, camp_token, camp_name, adgroups_token, adgroups_name, creatives_name, country, os_name, ' + str(kpi_string)
        sql = 'insert into adjust_api.stats_tmp (' + sqlhead + ') values '

        batch_size=0
        counter += 1
        #app_token = apps[i]['token']
        app_name = apps[0]['name']
        currency = apps[0]['currency']

    #     print('**********app level begin****************')
    #     print('app_token =', apps[i]['token'])
    #     print('app_name =', apps[i]['name'])
    #     print('currency =', apps[i]['currency'])
    #     print('**********app level end****************')
        networks=apps[0]['networks']
        for j in range(len(networks)):
            network_token = networks[j]['token']
            network_name = networks[j]['name']

    #         print('\t**********network level begin****************')
    #         print('\tnetwork_token =', networks[j]['token'])
    #         print('\tnetwork_name =', networks[j]['name'])
    #         print('\t**********network level end****************')        
            dts=networks[j]['dates']
            for k in range(len(dts)):
                dt=dts[k]['date']

    #             print('\t\t**********dt level begin****************')
    #             print('\t\tdt = ', dt)
                #print('\t\tnetwork_dates =', dts[k])
                camps=dts[k]['campaigns']
                #print('\t\tcamps = ', camps)
                for l in range(0,len(camps)):
                    camp_token = camps[l]['token']
                    camp_name = camps[l]['name']

    #                 print('\t\t\t**********camp level begin****************')
    #                 print('\t\t\tcamp_token = ', camps[l]['token'])
    #                 print('\t\t\tcamp_name = ', camps[l]['name'])
    #                 print('\t\t\tcamp_adgroups = ', camps[l]['adgroups'])
                    adgroups=camps[l]['adgroups']
                    for m in range(len(adgroups)):
                        adgroups_token = adgroups[m]['token']
                        adgroups_name = adgroups[m]['name']

    #                     print('\t\t\t\tadgroups_token = ', adgroups[m]['token'])
    #                     print('\t\t\t\tadgroups_name = ', adgroups[m]['name'])
    #                     print('\t\t\t\tadgroups_creatives = ', adgroups[m]['creatives'])
    #                     print('adgroups[m] =',adgroups[m])
                        creatives=adgroups[m]['creatives']
                        for n in range(len(creatives)):
                            creatives_name = creatives[n]['name']

    #                         print('\t\t\t\t\tcreatives[' + str(n) + '] =',creatives[n])
    #                         print('\t\t\t\t\tcreatives_name =',creatives[n]['name'])
    #                         print('\t\t\t\t\tcreatives_countries =',creatives[n]['countries'])
                            countries=creatives[n]['countries']
                            for o in range(len(countries)):
                                country = countries[o]['country']

    #                             print('\t\t\t\t\t\tcountries[' + str(o) + '] =',countries[o])
    #                             print('\t\t\t\t\t\tcountry =',countries[o]['country'])
    #                            print('\t\t\t\t\t\tos_names =',countries[o]['os_names'])
                                os_names = countries[o]['os_names']
                                for p in range(len(os_names)):
                                    os_name = os_names[p]['os_name']

    #                                 print('\t\t\t\t\t\t\tos_names[' + str(p) + '] =',os_names[p])
    #                                 print('\t\t\t\t\t\t\tos_name =',os_names[p]['os_name'])
    #                                 print('\t\t\t\t\t\t\tperiods =',os_names[p]['periods'])
                                    periods=os_names[p]['periods']
                                    for q in range(len(periods)):
                                        kpi_values = periods[q]['kpi_values']
                                        #print(kpi_values)
                                        kpi_values_string = ', '.join(str(x) for x in kpi_values)
                                        #print(kpi_values_string)
                                        #print('\t\t\t\t\t\t\t\tperiods[' + str(q) + '] =',periods[q])
                                        #print('app_token, app_name, currency, network_token, network_name, dt, camp_token, camp_name, adgroups_token, adgroups_name, creatives_name, country, os_name, ', kpis) 
                                        #print(app_token, app_name, currency, network_token, network_name, dt, camp_token, camp_name, adgroups_token, adgroups_name, creatives_name, country, os_name, kpi_values_string) 
                                        
#                                        if camp_token != 'mtynjp7':
#                                            continue
#                                        print('camp_NAME = ', camp_name)                                             

                                        values = "("
                                        values +=       "'" + str(app_token           ) + "'"
                                        values += "," + "'" + str(app_name.replace("'","''").replace("\n","")) + "'"
                                        values += "," + "'" + str(currency            ) + "'"
                                        values += "," + "'" + str(network_token       ) + "'"
                                        values += "," + "'" + str(network_name        ) + "'"
                                        values += "," + "'" + str(dt                  ) + "'"
                                        values += "," + "'" + str(camp_token          ) + "'"
                                        values += "," + "'" + str(camp_name.replace("' OR '","").replace("''","").replace("'","''").replace("\n","")) + "'"
                                        values += "," + "'" + str(adgroups_token      ) + "'"
                                        values += "," + "'" + str(adgroups_name.replace("' OR '","").replace("''","").replace("'","''").replace("\n","")) + "'"
                                        values += "," + "'" + str(creatives_name.replace("' OR '","").replace("'","''").replace("\n","")      ) + "'"
                                        values += "," + "'" + str(country             ) + "'"
                                        values += "," + "'" + str(os_name             ) + "'"
                                        values += ", "      + str(kpi_values_string   )                                    
                                        values += ")"

                                        batch_size += 1
                                        if batch_size != 1:
                                            values = ", " + values

                                        sql = sql + values

#                                                    if batch_size == batch_fill or counter == len(apps):        
                                        if batch_size == batch_fill:        
                                            #print(batch_size, batch_fill)
                                            batch_size = 0;
                                            #print(counter, len(apps))
                                    #         if counter >= 11000:
                                            try: 
                                                res = engine.execute(sql)
                                            except:
                                                print("Unexpected error:", sys.exc_info())
                                                print(sql)
                                                print(res)
                                                sys.exit(1)
                                            sql = 'insert into adjust_api.stats_tmp (' + sqlhead + ') values '

        #empty the batch
        print('***', batch_size, batch_fill)
        batch_size = 0;
        try: 
            res = engine.execute(sql)
        except:
            print("Unexpected error:", sys.exc_info())
            print(sql)
            print(res)
        sql = 'insert into adjust_api.stats_tmp (' + sqlhead + ') values '

print('time check: '+ str(datetime.datetime.now()))
    
#merge the data to source
#update
sql = """
      update adjust_api.stats
         set clicks = b.clicks
             , installs = b.installs
             , cost = b.cost
        from adjust_api.stats_tmp b
       where adjust_api.stats.dt                           >= '""" + str(start_date) + """'
         and adjust_api.stats.dt                           <= '""" + str(end_date) + """'
         and adjust_api.stats.dt                           = b.dt
         and coalesce(adjust_api.stats.app_token     , '-99')               = coalesce(b.app_token        , '-99')
         and coalesce(adjust_api.stats.currency      , '-99')               = coalesce(b.currency         , '-99')
         and coalesce(adjust_api.stats.network_token , '-99')               = coalesce(b.network_token    , '-99')
         and coalesce(adjust_api.stats.camp_token    , '-99')               = coalesce(b.camp_token       , '-99')
         and coalesce(adjust_api.stats.creatives_name    , '-99')               = coalesce(b.creatives_name       , '-99')
         and coalesce(adjust_api.stats.adgroups_token, '-99')               = coalesce(b.adgroups_token   , '-99')
         and coalesce(adjust_api.stats.country       , '-99')               = coalesce(b.country          , '-99')
         and coalesce(adjust_api.stats.os_name       , '-99')               = coalesce(b.os_name          , '-99')
         and (coalesce(adjust_api.stats.clicks,0)          != coalesce(b.clicks,0)
              or coalesce(adjust_api.stats.installs,0)          != coalesce(b.installs,0)
              or coalesce(adjust_api.stats.cost,0)          != coalesce(b.cost,0)
             )
"""
try: 
    print("updating")
    res = engine.execute(sql)
except:
    print("error updating:", sys.exc_info())
    print(res)

# insert
sql = """
      insert into adjust_api.stats
      select a.* 
        from adjust_api.stats_tmp a
        left join adjust_api.stats b
          on a.dt = b.dt
          and coalesce(a.app_token     , '-99') = coalesce(b.app_token      , '-99')
          and coalesce(a.currency      , '-99') = coalesce(b.currency       , '-99')
          and coalesce(a.network_token , '-99') = coalesce(b.network_token  , '-99')
          and coalesce(a.camp_token    , '-99') = coalesce(b.camp_token     , '-99')
          and coalesce(a.creatives_name, '-99') = coalesce(b.creatives_name , '-99')
          and coalesce(a.adgroups_token, '-99') = coalesce(b.adgroups_token , '-99')
          and coalesce(a.country       , '-99') = coalesce(b.country        , '-99')
          and coalesce(a.os_name       , '-99') = coalesce(b.os_name        , '-99')
        where b.app_token is null
"""
try: 
    print("inserting")
    res = engine.execute(sql)
except:
    print("error inserting:", sys.exc_info())
    print(res)

