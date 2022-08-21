DROP VIEW IF EXISTS ZHUKOVANANYANDEXRU__DWH.cdm_conversion;
CREATE VIEW ZHUKOVANANYANDEXRU__DWH.cdm_conversion as (
with group_need_to_analyse as 
(SELECT 
	hk_group_id
FROM ZHUKOVANANYANDEXRU__DWH.h_groups hg 
order by registration_dt
limit 10),

user_group_messages as (
    select 
    	hk_group_id
        ,count(distinct hk_user_id) as cnt_users_in_group_with_messages
    from ZHUKOVANANYANDEXRU__DWH.l_user_message as lum
    left join ZHUKOVANANYANDEXRU__DWH.l_groups_dialogs as lgm
    on lum.hk_message_id = lgm.hk_message_id
    inner join group_need_to_analyse as gnta
    on gnta.hk_group_id = lgm.hk_group_id
    group by hk_group_id
    
),

user_group_log as (
    select hk_group_id
        ,count(distinct hk_user_id) as cnt_added_users
    from ZHUKOVANANYANDEXRU__DWH.s_auth_history as sah
    left join ZHUKOVANANYANDEXRU__DWH.l_user_group_activity as luga
    on sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
    inner join group_need_to_analyse as gnta
    on gnta.hk_group_id = lgm.hk_group_id
    where event = 'add'
    group by
    	hk_group_id
) 

select 
	gnta.hk_group_id
	,cnt_added_users
	,cnt_users_in_group_with_messages
	,cnt_users_in_group_with_messages*1.00/cnt_added_users as group_conversion
from group_need_to_analyse as gnta
left join user_group_messages as ugm
on gnta.hk_group_id = ugm.hk_group_id
left join user_group_log as ugl
on gnta.hk_group_id = ugl.hk_group_id
order by group_conversion desc)
