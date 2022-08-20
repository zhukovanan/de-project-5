INSERT INTO ZHUKOVANANYANDEXRU__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
SELECT DISTINCT
hash(hg.hk_user_id,hu.hk_group_id),
hg.hk_user_id,
hu.hk_group_id,
now() AS load_dt,
's3' AS load_src
FROM ZHUKOVANANYANDEXRU__STAGING.group_log AS d
LEFT JOIN ZHUKOVANANYANDEXRU__DWH.h_groups AS hu 
ON d.group_id  = hu.group_id
LEFT JOIN ZHUKOVANANYANDEXRU__DWH.h_users AS hg 
ON d.user_id  = hg.user_id 
WHERE hash(hg.hk_user_id,hu.hk_group_id) NOT IN (SELECT 
                                                    hk_l_user_group_activity 
                                                 FROM ZHUKOVANANYANDEXRU__DWH.l_user_group_activity);
