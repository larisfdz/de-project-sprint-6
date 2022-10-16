insert into
    abaelardusyandexru__DWH.l_user_group_activity(
        hk_l_user_group_activity,
        hk_user_id,
        hk_group_id,
        load_dt,
        load_src
    )
select
    hash(hu.hk_user_id, hg.hk_group_id),
    hu.hk_user_id,
    hg.hk_group_id,
    now() as load_dt,
    's3' as load_src
from
    abaelardusyandexru__STAGING.group_log as gl
    left join abaelardusyandexru__DWH.h_users hu on hu.user_id = gl.user_id
    left join abaelardusyandexru__DWH.h_groups hg on hg.group_id = gl.group_id
where
    hash(hu.hk_user_id, hg.hk_group_id) not in (
        select
            hk_l_user_group_activity
        from
            abaelardusyandexru__DWH.l_user_group_activity
    );

INSERT INTO
    abaelardusyandexru__DWH.s_auth_history(
        hk_l_user_group_activity,
        event,
        event_dt,
        load_dt,
        load_src
    )
select
    luga.hk_l_user_group_activity,
    gl.event,
    gl."datetime" as event_dt,
    now() as load_dt,
    's3' as load_src
from
    abaelardusyandexru__STAGING.group_log as gl
    left join abaelardusyandexru__DWH.h_groups as hg on gl.group_id = hg.group_id
    left join abaelardusyandexru__DWH.h_users as hu on gl.user_id = hu.user_id
    left join abaelardusyandexru__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id
    and hu.hk_user_id = luga.hk_user_id;

INSERT INTO
    abaelardusyandexru__DWH.s_group_user_from(
        hk_l_user_group_activity,
        hk_user_id,
        user_id_from,
        load_dt,
        load_src
    )
select
    distinct luga.hk_l_user_group_activity,
    hu.hk_user_id,
    gl.user_id_from,
    now() as load_dt,
    's3' as load_src
from
    abaelardusyandexru__STAGING.group_log as gl
    left join abaelardusyandexru__DWH.h_groups as hg on gl.group_id = hg.group_id
    left join abaelardusyandexru__DWH.h_users as hu on gl.user_id = hu.user_id
    left join abaelardusyandexru__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id
    and hu.hk_user_id = luga.hk_user_id;