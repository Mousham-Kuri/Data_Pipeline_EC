from google.cloud import bigquery
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\Admin\PycharmProjects\pythonProject\eclub-data-d18845fd301c.json'
# Construct a BigQuery client object.
client = bigquery.Client()

query_string1 = """
delete from eclub-data.prod.main_product_categories            where id in (select a.id from prod.main_product_categories             a inner join prod.stg_product_categories             b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_categories_interests  where id in (select a.id from prod.main_product_categories_interests   a inner join prod.stg_product_categories_interests   b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_category_translations where id in (select a.id from prod.main_product_category_translations  a inner join prod.stg_product_category_translations  b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_course_schedules      where id in (select a.id from prod.main_product_course_schedules       a inner join prod.stg_product_course_schedules       b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_user_hashtags         where id in (select a.id from prod.main_product_user_hashtags          a inner join prod.stg_product_user_hashtags          b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_courses               where id in (select a.id from prod.main_product_courses                a inner join prod.stg_product_courses                b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_event_schedules       where id in (select a.id from prod.main_product_event_schedules        a inner join prod.stg_product_event_schedules        b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_events                where id in (select a.id from prod.main_product_events                 a inner join prod.stg_product_events                 b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_friend_statuses       where id in (select a.id from prod.main_product_friend_statuses        a inner join prod.stg_product_friend_statuses        b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_user_files            where id in (select a.id from prod.main_product_user_files             a inner join prod.stg_product_user_files             b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_user_polls            where id in (select a.id from prod.main_product_user_polls             a inner join prod.stg_product_user_polls             b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_user_reactions        where id in (select a.id from prod.main_product_user_reactions         a inner join prod.stg_product_user_reactions         b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_user_shares           where id in (select a.id from prod.main_product_user_shares            a inner join prod.stg_product_user_shares            b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_user_profiles         where id in (select a.id from prod.main_product_user_profiles          a inner join prod.stg_product_user_profiles          b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_user_posts            where id in (select a.id from prod.main_product_user_posts             a inner join prod.stg_product_user_posts             b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_user_stories          where id in (select a.id from prod.main_product_user_stories           a inner join prod.stg_product_user_stories           b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_user_comments         where id in (select a.id from prod.main_product_user_comments          a inner join prod.stg_product_user_comments          b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_users_event_schedules where id in (select a.id from prod.main_product_users_event_schedules  a inner join prod.stg_product_users_event_schedules  b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_sub_category_translations where id in (select a.id from prod.main_product_sub_category_translations a inner join prod.stg_product_sub_category_translations b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_posts_users           where id in (select a.id from prod.main_product_posts_users            a inner join prod.stg_product_posts_users            b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_users                 where id in (select a.id from prod.main_product_users                  a inner join prod.stg_product_users                  b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_event_translations    where id in (select a.id from prod.main_product_event_translations     a inner join prod.stg_product_event_translations     b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_report_posts        where id in (select a.id from prod.main_product_report_posts         a inner join prod.stg_product_report_posts         b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_report_story        where id in (select a.id from prod.main_product_report_story         a inner join prod.stg_product_report_story         b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_report_users        where id in (select a.id from prod.main_product_report_users         a inner join prod.stg_product_report_users         b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_report_comments     where id in (select a.id from prod.main_product_report_comments      a inner join prod.stg_product_report_comments      b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_block_users         where id in (select a.id from prod.main_product_block_users          a inner join prod.stg_product_block_users          b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_poll_votes          where id in (select a.id from prod.main_product_poll_votes           a inner join prod.stg_product_poll_votes           b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_story_viewers       where id in (select a.id from prod.main_product_story_viewers        a inner join prod.stg_product_story_viewers        b on a.id=b.id)  ;
delete from eclub-data.prod.main_product_block_user_statuses where id in (select a.id from prod.main_product_block_user_statuses  a inner join prod.stg_product_block_user_statuses  b on a.id=b.id)  ;

insert into eclub-data.prod.main_product_categories            select *,date(updated_at) from  prod.stg_product_categories               ;
insert into eclub-data.prod.main_product_categories_interests  select *,date(updated_at) from  prod.stg_product_categories_interests     ;
insert into eclub-data.prod.main_product_category_translations select *,date(updated_at) from  prod.stg_product_category_translations    ;
insert into eclub-data.prod.main_product_course_schedules      select *,date(updated_at) from  prod.stg_product_course_schedules         ;
insert into eclub-data.prod.main_product_user_hashtags         select *,date(updated_at) from  prod.stg_product_user_hashtags            ;
insert into eclub-data.prod.main_product_courses               select *,date(updated_at) from  prod.stg_product_courses                  ;
insert into eclub-data.prod.main_product_event_schedules       select *,date(updated_at) from  prod.stg_product_event_schedules          ;
insert into eclub-data.prod.main_product_events                select *,date(updated_at) from  prod.stg_product_events                   ;
insert into eclub-data.prod.main_product_friend_statuses       select *,date(updated_at) from  prod.stg_product_friend_statuses          ;
insert into eclub-data.prod.main_product_user_files            select *,date(updated_at) from  prod.stg_product_user_files               ;
insert into eclub-data.prod.main_product_user_polls            select *,date(updated_at) from  prod.stg_product_user_polls               ;
insert into eclub-data.prod.main_product_user_reactions        select *,date(updated_at) from  prod.stg_product_user_reactions           ;
insert into eclub-data.prod.main_product_user_shares           select *,date(updated_at) from  prod.stg_product_user_shares              ;
insert into eclub-data.prod.main_product_user_profiles         select *,date(updated_at) from  prod.stg_product_user_profiles            ;
insert into eclub-data.prod.main_product_user_posts            select *,date(updated_at) from  prod.stg_product_user_posts               ;
insert into eclub-data.prod.main_product_user_stories          select *,date(updated_at) from  prod.stg_product_user_stories             ;
insert into eclub-data.prod.main_product_user_comments         select *,date(updated_at) from  prod.stg_product_user_comments            ;
insert into eclub-data.prod.main_product_users_event_schedules select *,date(updated_at) from  prod.stg_product_users_event_schedules    ;
insert into eclub-data.prod.main_product_sub_category_translations select *,date(updated_at) from  prod.stg_product_sub_category_translations    ;
insert into eclub-data.prod.main_product_posts_users           select *,date(updated_at) from  prod.stg_product_posts_users    ;
insert into eclub-data.prod.main_product_users                 select *,date(updated_at) from  prod.stg_product_users   ;
insert into eclub-data.prod.main_product_event_translations    select *,date(updated_at) from  prod.stg_product_event_translations   ;
insert into eclub-data.prod.main_product_report_posts         select *,date(updated_at) from prod.stg_product_report_posts       ;
insert into eclub-data.prod.main_product_report_story         select *,date(updated_at) from prod.stg_product_report_story       ;
insert into eclub-data.prod.main_product_report_users         select *,date(updated_at) from prod.stg_product_report_users       ;
insert into eclub-data.prod.main_product_report_comments      select *,date(updated_at) from prod.stg_product_report_comments    ;
insert into eclub-data.prod.main_product_block_users          select *,date(updated_at) from prod.stg_product_block_users        ;
insert into eclub-data.prod.main_product_poll_votes           select *,date(updated_at) from prod.stg_product_poll_votes         ;
insert into eclub-data.prod.main_product_story_viewers        select *,date(updated_at) from prod.stg_product_story_viewers      ;
insert into eclub-data.prod.main_product_block_user_statuses  select *,date(updated_at) from prod.stg_product_block_user_statuses;
"""
client.query(query_string1).result()