
import datetime
import numpy as np
import pandas as pd
import google.oauth2.credentials
import pandas_gbq


# Setup gcloud SDK
# login to gcloud
# gcloud auth application-default login
# print token
# fill oauth2 creds and project name
# install tqdm
# install xlxswriter


def create_date():
    year = int(input('Enter a year'))
    month = int(input('Enter a month'))
    day = int(input('Enter a day'))
    try:
        return datetime.date(year, month, day)
    except Exception as error:
        print(error)
        return None


def main():

    #connect to BQ via default-login token
    credentials = google.oauth2.credentials.Credentials(
        'a29.A0AfH6SMDgL2ePU-NjQI4XsY56JNkG4HmGCTyu4qS4375bo9TyvPNxg9RebXmEThDbLwSzuUO1N1_dS5LJ8MTGWRmiPzzyyOp7v3sESmGtwVM70PYcj-kgMZ3-H2zPTB1NMd0-yCK9wpdSS1djIDRa4o7xW7RzqExZmgGrF2sSweQ')
    project_id = "al-bi-bq-prod"


    date1 = create_date()
    date2 = create_date()
    #affiliate_id = [int(x) for x in input("Enter affiliate_ids\n").split(',')]
    advertiser_id = int(input('Enter an adv_id\n'))
  #  offer_ids = [x for x in input("Enter offer_ids\n").split(',')]


    #query the data
    sql = f"""
          select
          MP, event_name, round(100*(state_count / total),2) as percentage 
          from (
          SELECT a.network_affiliate_id as MP, event_name, count(*) AS state_count, sum(count(*)) OVER(partition by a.network_affiliate_id) AS total
          
          FROM 
            `al-bi-bq-prod.dwh.fact_conversions_and_events` a left join `al-bi-bq-prod.dwh.dim_affiliates` b on a.network_affiliate_id = b.affiliate_id
          
          WHERE
        event_name != 'app_open' and event_name != 'rejected_install'
          
          AND network_advertiser_id = {advertiser_id} 
          AND _partitiondate between '{date1}' and '{date2}'
          GROUP by 2,1
          order by MP) s
    """

    sql2 = f"""
            SELECT
            conversion_id,transaction_id, network_offer_id, conversion_status, event_name, network_affiliate_id, affiliate_name, source_id, click_date, date,
            timestamp_diff (date,click_date, second) as timediff,
            CASE 
            WHEN timestamp_diff (date,click_date, second) >=  0 
            AND timestamp_diff (date,click_date, second) < 10 THEN '1_<10secs'
            WHEN timestamp_diff (date,click_date, second) >=  10 
            AND timestamp_diff (date,click_date, second) < 60 THEN '2_<1min'
            WHEN timestamp_diff (date,click_date, second) >=  60 
            AND timestamp_diff (date,click_date, second) < 300 THEN '3_<5min'
            WHEN timestamp_diff (date,click_date, second) >=  300 
            AND timestamp_diff (date,click_date, second) < 600 THEN '4_<10min'
            WHEN timestamp_diff (date,click_date, second) >=  600 
            AND timestamp_diff (date,click_date, second) < 1800 THEN '5_<30min'
            WHEN timestamp_diff (date,click_date, second) >=  1800 
            AND timestamp_diff (date,click_date, second) < 3600 THEN '6_<1hr'
            WHEN timestamp_diff (date,click_date, second) >=  3600
            AND timestamp_diff (date,click_date, second) < 86400 THEN '7_<24hr'
            ELSE 'more than 24h' END as buckets, 
            session_user_ip, conversion_user_ip, payout, revenue, query_parameters_device_detail, email, carrier, browser,brand, device_type, os_version, language, region, country, city, android_id, adv1, adv2, adv3, adv4, adv5, is_view_through, is_scrub, is_cookie_based, idfa, google_ad_id, sub1
            
            FROM 
            `al-bi-bq-prod.dwh.fact_conversions_and_events` a left join `al-bi-bq-prod.dwh.dim_affiliates` b on a.network_affiliate_id = b.affiliate_id
            
            WHERE
            _partitionDATE BETWEEN '{date1}' and '{date2}'
            AND network_advertiser_id = {advertiser_id}
            
            AND conversion_status = 'approved'  
            AND event_name != 'app_open'
            AND event_name != 'rejected_install'
    """


    sql3 = f"""
            #standardSQL
            SELECT
            affiliate_id,
            round(b.IP_dups_percentage,2) as IP_dups_percentage, 
            round(b.Wifi_percentage,2) as wifi_percentage,
            sum(approved_installs)/sum(clicks) as Click_to_installs,
            sum(conversions)/sum(approved_installs) as Install_to_action_rate,
            sum(mmp_rejected_installs)/sum(total_installs) as rejection_rate,
            sum(clicks) as total_clicks, 
            sum(approved_installs) as approved_installs,
            sum(actions) as App_and_pend_conv,
            sum(conversions) as approved_conversions,
            sum(a.payout) as total_payout, 
            sum(a.revenue) as total_revenue
            FROM
            `al-bi-bq-prod.dwh.fact_daily_stats` a 
            left join
            (select network_affiliate_id,
            (count(session_user_ip) - count(distinct(session_user_ip)))/count(session_user_ip) as IP_dups_percentage,
            SUM( CASE WHEN carrier ='' THEN 1 ELSE 0 END )/count(carrier) as Wifi_percentage  
            
            FROM 
            `al-bi-bq-prod.dwh.fact_conversions_and_events`
            where conversion_status = 'approved' AND payout != 0
            group by 1) b on a.affiliate_id = b.network_affiliate_id
            
            WHERE
            _partitionDATE BETWEEN '{date1}' and '{date2}'
            AND advertiser_id = {advertiser_id}
            
            group by 1,2,3
            having approved_installs >0 and total_clicks >0 and total_payout > 100
            order by total_payout desc
    """

    sql4 = f"""
            SELECT
            conversion_id,transaction_id, network_offer_id, conversion_status, event_name, network_affiliate_id, affiliate_name, source_id, click_date, date,
            timestamp_diff (date,click_date, second) as timediff,
            CASE 
            WHEN timestamp_diff (date,click_date, second) >=  0 
            AND timestamp_diff (date,click_date, second) < 10 THEN '1_<10secs'
            WHEN timestamp_diff (date,click_date, second) >=  10 
            AND timestamp_diff (date,click_date, second) < 60 THEN '2_<1min'
            WHEN timestamp_diff (date,click_date, second) >=  60 
            AND timestamp_diff (date,click_date, second) < 300 THEN '3_<5min'
            WHEN timestamp_diff (date,click_date, second) >=  300 
            AND timestamp_diff (date,click_date, second) < 600 THEN '4_<10min'
            WHEN timestamp_diff (date,click_date, second) >=  600 
            AND timestamp_diff (date,click_date, second) < 1800 THEN '5_<30min'
            WHEN timestamp_diff (date,click_date, second) >=  1800 
            AND timestamp_diff (date,click_date, second) < 3600 THEN '6_<1hr'
            WHEN timestamp_diff (date,click_date, second) >=  3600
            AND timestamp_diff (date,click_date, second) < 86400 THEN '7_<24hr'
            ELSE 'more than 24h' END as buckets, 
            session_user_ip, conversion_user_ip, payout, revenue, query_parameters_device_detail, email, carrier, browser,brand, device_type, os_version, language, region, country, city, android_id, adv1, adv2, adv3, adv4, adv5, is_view_through, is_scrub, is_cookie_based, idfa, google_ad_id, sub1
            
            FROM 
            `al-bi-bq-prod.dwh.fact_conversions_and_events` a left join `al-bi-bq-prod.dwh.dim_affiliates` b on a.network_affiliate_id = b.affiliate_id

            WHERE
            _partitionDATE BETWEEN '{date1}' and '{date2}'
            AND network_advertiser_id = {advertiser_id}
           
            AND conversion_status = 'approved'  
            AND network_offer_payout_revenue_id = 0
    """

    sql5 = f"""
            SELECT
            conversion_id,transaction_id, network_offer_id, conversion_status, event_name, network_affiliate_id, affiliate_name, source_id, click_date, date,
            timestamp_diff (date,click_date, second) as timediff,
            CASE 
            WHEN timestamp_diff (date,click_date, second) >=  0 
            AND timestamp_diff (date,click_date, second) < 10 THEN '1_<10secs'
            WHEN timestamp_diff (date,click_date, second) >=  10 
            AND timestamp_diff (date,click_date, second) < 60 THEN '2_<1min'
            WHEN timestamp_diff (date,click_date, second) >=  60 
            AND timestamp_diff (date,click_date, second) < 300 THEN '3_<5min'
            WHEN timestamp_diff (date,click_date, second) >=  300 
            AND timestamp_diff (date,click_date, second) < 600 THEN '4_<10min'
            WHEN timestamp_diff (date,click_date, second) >=  600 
            AND timestamp_diff (date,click_date, second) < 1800 THEN '5_<30min'
            WHEN timestamp_diff (date,click_date, second) >=  1800 
            AND timestamp_diff (date,click_date, second) < 3600 THEN '6_<1hr'
            WHEN timestamp_diff (date,click_date, second) >=  3600
            AND timestamp_diff (date,click_date, second) < 86400 THEN '7_<24hr'
            ELSE 'more than 24h' END as buckets, 
            session_user_ip, conversion_user_ip, payout, revenue, query_parameters_device_detail, email, carrier, browser,brand, device_type, os_version, language, region, country, city, android_id, adv1, adv2, adv3, adv4, adv5, is_view_through, is_scrub, is_cookie_based, idfa, google_ad_id, sub1
            
            FROM 
            `al-bi-bq-prod.dwh.fact_conversions_and_events` a left join `al-bi-bq-prod.dwh.dim_affiliates` b on a.network_affiliate_id = b.affiliate_id

            WHERE
            _partitionDATE BETWEEN '{date1}' and '{date2}'
            AND network_advertiser_id = {advertiser_id}
           
            AND conversion_status = 'approved'  
            AND payout != 0
    """

    #examples for testing
    #(76771, 76772)
    #10415
    #'2020-10-25' and '2020-10-30'

    df1 = pandas_gbq.read_gbq(sql, project_id=project_id)

    df2 = pandas_gbq.read_gbq(sql2, project_id=project_id)
    df2['click_date'] = df2['click_date'].dt.tz_localize(None)
    df2['date'] = df2['date'].dt.tz_localize(None)

    df3 = pandas_gbq.read_gbq(sql3, project_id=project_id)

    df4 = pandas_gbq.read_gbq(sql4, project_id=project_id)
    df4['click_date'] = df4['click_date'].dt.tz_localize(None)
    df4['date'] = df4['date'].dt.tz_localize(None)

    df5 = pandas_gbq.read_gbq(sql5, project_id=project_id)
    df5['click_date'] = df5['click_date'].dt.tz_localize(None)
    df5['date'] = df5['date'].dt.tz_localize(None)

    df6 = pd.pivot_table(df4, values = 'timediff', index=['network_affiliate_id'], columns=['buckets'], aggfunc=np.count_nonzero)

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter('/Users/user/Downloads/report 2020.xlsx', engine='xlsxwriter')

    # Write each dataframe to a different worksheet.
    df5.to_excel(writer, sheet_name='Conversions')
    df4.to_excel(writer, sheet_name='Installs')
    df3.to_excel(writer, sheet_name='metrics_table')
    df6.to_excel(writer, sheet_name='CTIT')
    df2.to_excel(writer, sheet_name='full_report')
    df1.to_excel(writer, sheet_name='In-app_activity_behavior')

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()


if __name__ == "__main__":
    main()

