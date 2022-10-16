drop table if exists abaelardusyandexru__DWH.l_user_group_activity cascade;

drop table if exists abaelardusyandexru__DWH.s_auth_history cascade;

drop table if exists abaelardusyandexru__DWH.s_group_user_from cascade;

create table if not exists abaelardusyandexru__DWH.l_user_group_activity (
    hk_l_user_group_activity bigint primary key,
    hk_user_id bigint not null constraint fk_l_user_group_activity_user references abaelardusyandexru__DWH.h_users (hk_user_id),
    hk_group_id bigint not null constraint fk_l_user_group_activity_group references abaelardusyandexru__DWH.h_groups (hk_group_id),
    load_dt datetime,
    load_src varchar(20)
)
order by
    load_dt SEGMENTED BY hk_l_user_group_activity all nodes PARTITION BY load_dt :: date
GROUP BY
    calendar_hierarchy_day(load_dt :: date, 3, 2);

create table if not exists abaelardusyandexru__DWH.s_auth_history (
    hk_l_user_group_activity bigint not null constraint fk_s_auth_history_l_user_group_activity references abaelardusyandexru__DWH.l_user_group_activity (hk_l_user_group_activity),
    event varchar(200),
    event_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by
    load_dt SEGMENTED BY hk_l_user_group_activity all nodes PARTITION BY load_dt :: date
GROUP BY
    calendar_hierarchy_day(load_dt :: date, 3, 2);

create table if not exists abaelardusyandexru__DWH.s_group_user_from (
    hk_l_user_group_activity bigint not null constraint fk_s_auth_history_l_user_group_activity references abaelardusyandexru__DWH.l_user_group_activity (hk_l_user_group_activity),
    hk_user_id bigint not null constraint fk_s_user_from_h_users references abaelardusyandexru__DWH.h_users (hk_user_id),
    user_id_from bigint,
    load_dt datetime,
    load_src varchar(20)
)
order by
    load_dt SEGMENTED BY hk_l_user_group_activity all nodes PARTITION BY load_dt :: date
GROUP BY
    calendar_hierarchy_day(load_dt :: date, 3, 2);