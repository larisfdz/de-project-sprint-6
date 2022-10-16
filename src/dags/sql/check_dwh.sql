(select
    load_dt,
    'l_user_group_activity' as table_name,
    count(*) as rows_inserted
from
    abaelardusyandexru__DWH.l_user_group_activity
group by
    load_dt
order by
    load_dt desc
limit
    1)
union all
(select
    load_dt,
    's_auth_history' as table_name,
    count(*) as rows_inserted
from
    abaelardusyandexru__DWH.s_auth_history
group by
    load_dt
order by
    load_dt desc
limit 1)
union all
(select
    load_dt,
    's_group_user_from' as table_name,
    count(*) as rows_inserted
from
    abaelardusyandexru__DWH.s_group_user_from
group by
    load_dt
order by
    load_dt desc
limit 1);