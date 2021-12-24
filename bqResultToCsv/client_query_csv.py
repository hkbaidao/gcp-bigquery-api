# www.baidaodata.com

import csv

def client_query():

    # [START bigquery_query]

    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    query = """
        SELECT
		  billing_account_id AS Billing_account_ID,
		  project.name AS Project_name,
		  project.id AS Project_ID,
		  service.description AS Service_description,
		  service.id AS Service_ID,
		  sku.description AS SKU_description,
		  sku.id AS SKU_ID,
		  credits.type AS Credit_type,
		  cost_type AS Cost_type,
		  usage_start_time AS Usage_start_date,
		  usage_end_time AS Usage_end_date,
		  usage.amount AS Usage_amount,
		  usage.unit AS Usage_unit,
		  cost AS Cost
		FROM
		  `#数据集.表名`,
		  UNNEST(credits) AS credits
		WHERE
		  invoice.month = '#月份';
    """

    query_job = client.query(query)  # Make an API request.

    l = []
    
    # 自定义导出到csv的列名称
    b=["Billing_account_ID","Project_name","Project_ID","Service_description","Service_ID","SKU_description","SKU_ID","Credit_type","Cost_type","Usage_start_date","Usage_end_date","Usage_amount","Usage_unit","cost($)"]

    for row in query_job:
        # 创建临时的列表
        l_temp = [row[0], row[1], row[2], row[3], row[4],row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13]]
        l.append(l_temp)

    with open("demo.csv",'w',newline='',encoding='utf-8-sig') as t:
        writer=csv.writer(t)
        writer.writerow(b)
        writer.writerows(l)

client_query()
