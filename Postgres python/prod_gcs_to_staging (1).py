from google.cloud import bigquery
from datetime import  datetime,timedelta
yesterday = (datetime.today()-timedelta(days=1))
yesterday = yesterday.strftime('%Y%m%d')
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\Admin\PycharmProjects\pythonProject\eclub-data-d18845fd301c.json'
# Construct a BigQuery client object.
client = bigquery.Client()


# Set table_id to the ID of the table to create.
table_id = ['eclub-data.prod.stg_product_categories',
'eclub-data.prod.stg_product_categories_interests',
'eclub-data.prod.stg_product_category_translations',
'eclub-data.prod.stg_product_course_schedules',
'eclub-data.prod.stg_product_user_hashtags',
'eclub-data.prod.stg_product_courses',
'eclub-data.prod.stg_product_event_schedules',
'eclub-data.prod.stg_product_events',
'eclub-data.prod.stg_product_friend_statuses',
'eclub-data.prod.stg_product_user_files',
'eclub-data.prod.stg_product_user_polls',
'eclub-data.prod.stg_product_user_reactions',
'eclub-data.prod.stg_product_user_shares',
'eclub-data.prod.stg_product_user_profiles',
'eclub-data.prod.stg_product_user_posts',
'eclub-data.prod.stg_product_user_stories',
'eclub-data.prod.stg_product_user_comments',
'eclub-data.prod.stg_product_users_event_schedules',
'eclub-data.prod.stg_product_sub_category_translations',
'eclub-data.prod.stg_product_posts_users',
'eclub-data.prod.stg_product_users',
'eclub-data.prod.stg_product_event_translations',
'eclub-data.prod.stg_product_report_posts',
'eclub-data.prod.stg_product_report_story',
'eclub-data.prod.stg_product_report_users',
'eclub-data.prod.stg_product_report_comments',
'eclub-data.prod.stg_product_block_users',
'eclub-data.prod.stg_product_poll_votes',
'eclub-data.prod.stg_product_story_viewers',
'eclub-data.prod.stg_product_block_user_statuses']

job_config = bigquery.LoadJobConfig(
    allow_quoted_newlines = True,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
    # If table is already loaded, following command will ensure its over-written
    write_disposition = 'WRITE_TRUNCATE'
)
uri = ['gs://eclub_data_prod_product/product_categories',
'gs://eclub_data_prod_product/product_categories_interests',
'gs://eclub_data_prod_product/product_category_translations',
'gs://eclub_data_prod_product/product_course_schedules',
'gs://eclub_data_prod_product/product_user_hashtags',
'gs://eclub_data_prod_product/product_courses',
'gs://eclub_data_prod_product/product_event_schedules',
'gs://eclub_data_prod_product/product_events',
'gs://eclub_data_prod_product/product_friend_statuses',
'gs://eclub_data_prod_product/product_user_files',
'gs://eclub_data_prod_product/product_user_polls',
'gs://eclub_data_prod_product/product_user_reactions',
'gs://eclub_data_prod_product/product_user_shares',
'gs://eclub_data_prod_product/product_user_profiles',
'gs://eclub_data_prod_product/product_user_posts',
'gs://eclub_data_prod_product/product_user_stories',
'gs://eclub_data_prod_product/product_user_comments',
'gs://eclub_data_prod_product/product_users_event_schedules',
'gs://eclub_data_prod_product/product_sub_category_translations',
'gs://eclub_data_prod_product/product_posts_users',
'gs://eclub_data_prod_product/product_users',
'gs://eclub_data_prod_product/product_event_translations',
'gs://eclub_data_prod_product/product_report_posts',
'gs://eclub_data_prod_product/product_report_story',
'gs://eclub_data_prod_product/product_report_users',
'gs://eclub_data_prod_product/product_report_comments',
'gs://eclub_data_prod_product/product_block_users',
'gs://eclub_data_prod_product/product_poll_votes',
'gs://eclub_data_prod_product/product_story_viewers',
'gs://eclub_data_prod_product/product_block_user_statuses']
        
        
for i in range(0,len(table_id)):
    load_job = client.load_table_from_uri(uri[i]+'_'+yesterday+'.csv', table_id[i], job_config=job_config)  # Make an API request.
    # uri[i], table_id[i], job_config=job_config)  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id[i])  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))