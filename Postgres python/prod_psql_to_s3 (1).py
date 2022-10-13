import psycopg2
from psycopg2 import Error
from datetime import datetime, timedelta

yesterday = (datetime.today() - timedelta(days=1))
yesterday_f1 = yesterday.strftime('%Y-%m-%d')
yesterday_f2 = yesterday.strftime('%Y%m%d')
try:
    connection = psycopg2.connect(user='deuser',
                                  password='9be1E028d8f73b4cc3Sa2Te2bc1Z7efX',
                                  host='eclub-db.crl3zsijmxal.ap-south-1.rds.amazonaws.com',
                                  port='5432',
                                  database='sclub_prod')

    views =['view_categories',
            'view_categories_interests',
            'view_category_translations',
            'view_course_schedules',
            'view_user_hashtags',
            'view_courses',
            'view_event_schedules',
            'view_events',
            'view_friend_statuses',
            'view_user_files',
            'view_user_polls',
            'view_user_reactions',
            'view_user_shares',
            'view_user_profiles',
            'view_user_posts',
            'view_user_stories',
            'view_user_comments',
            'view_users_event_schedules',
            'view_sub_category_translations',
            'view_posts_users',
            'view_users',
            'view_event_translations',
            'view_report_posts',
            'view_report_story',
            'view_report_users',
            'view_report_comments',
            'view_block_users',
            'view_poll_votes',
            'view_story_viewers',
            'view_block_user_statuses']
    files =['product_categories',
            'product_categories_interests',
            'product_category_translations',
            'product_course_schedules',
            'product_user_hashtags',
            'product_courses',
            'product_event_schedules',
            'product_events',
            'product_friend_statuses',
            'product_user_files',
            'product_user_polls',
            'product_user_reactions',
            'product_user_shares',
            'product_user_profiles',
            'product_user_posts',
            'product_user_stories',
            'product_user_comments',
            'product_users_event_schedules',
            'product_sub_category_translations',
            'product_posts_users',
            'product_users',
            'product_event_translations',
            'product_report_posts',
            'product_report_story',
            'product_report_users',
            'product_report_comments',
            'product_block_users',
            'product_poll_votes',
            'product_story_viewers',
            'product_block_user_statuses']

    for i in range(0,len(views)):
        create_file = 'select * from aws_s3.query_export_to_s3' + '(' + "'" + 'select * from' + ' ' + str(views[i]) + ' ' + 'where cast(updated_at as text) LIKE' + ' ' + "''" + yesterday_f1 + '%' + "''" + "'" + ',' + "'" + 'evergreenclub-data-engineering-prod' + "'" + ',' + "'" + files[i] + '_' + yesterday_f2 + '.csv' + "'" + ',' + "'" + 'ap-south-1' + "'" + ','  + 'options:='+ "'" + 'FORMAT CSV' + "'" + ')' + ';'
        print(create_file)
        cursor = connection.cursor()
        cursor.execute(create_file)
        print('file created' + 'for' +' '+ views[i])


except(Exception, Error) as error:
    print('Error while connection to Database', error)
finally:
    if (connection):
        cursor.close()
        connection.close()
        print('PostgreSQL connection closed')