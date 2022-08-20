INSERT INTO ZHUKOVANANYANDEXRU__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
SELECT
	hk_l_user_group_activity
	,user_id_from
	,event
	,datetime AS event_dt
	,now() AS load_dt
	,'s3' AS load_src
FROM ZHUKOVANANYANDEXRU__STAGING.group_log gl
LEFT JOIN ZHUKOVANANYANDEXRU__DWH.h_groups as hg 
ON gl.group_id = hg.group_id
LEFT JOIN ZHUKOVANANYANDEXRU__DWH.h_users as hu 
ON gl.user_id = hu.user_id
LEFT JOIN ZHUKOVANANYANDEXRU__DWH.l_user_group_activity as lg 
ON hg.hk_group_id = lg.hk_group_id 
AND hu.hk_user_id = lg.hk_user_id