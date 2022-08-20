DROP TABLE IF EXISTS ZHUKOVANANYANDEXRU__STAGING.group_log;
CREATE TABLE ZHUKOVANANYANDEXRU__STAGING.group_log
(group_id int primary key,
user_id int,
user_id_from int,
event varchar(20),
datetime timestamp)
ORDER BY datetime
SEGMENTED BY group_id all nodes
PARTITION BY datetime::date
GROUP BY calendar_hierarchy_day(datetime::date, 3, 2)
