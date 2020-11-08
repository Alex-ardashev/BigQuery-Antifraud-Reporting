import pandas_gbq
import google.oauth2.credentials

# login to gcloud
# gcloud auth application-default login
# print the key
#
credentials = google.oauth2.credentials.Credentials(
    'a29.A0AfH6SMDgL2ePU-NjQI4XsY56JNkG4HmGCTyu4qS4375bo9TyvPNxg9RebXmEThDbLwSzuUO1N1_dS5LJ8MTGWRmiPzzyyOp7v3sESmGtwVM70PYcj-kgMZ3-H2zPTB1NMd0-yCK9wpdSS1djIDRa4o7xW7RzqExZmgGrF2sSweQ')
project_id = "al-bi-bq-prod"
sql = """

  select --network_offer_id, 
  MP, event_name, round(100*(state_count / total),2) as percentage
from (
  SELECT a.network_affiliate_id as MP, event_name, count(*) AS state_count, sum(count(*)) OVER(partition by a.network_affiliate_id) AS total
  FROM `al-bi-bq-prod.dwh.fact_conversions_and_events` a left join `al-bi-bq-prod.dwh.dim_affiliates` b on a.network_affiliate_id = b.affiliate_id
  Where event_name != 'app_open' and event_name != 'rejected_install'
  AND network_offer_id in (76771,76772)
  AND network_advertiser_id = 10415
  --AND network_affiliate_id in ()
  AND _partitiondate between '2020-10-10' and '2020-10-31'
  GROUP by 2,1
  order by MP
) s
"""

df = pandas_gbq.read_gbq(sql, project_id=project_id)
print(df)

