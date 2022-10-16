drop table if exists abaelardusyandexru__staging.group_log;

create table if not exists abaelardusyandexru__staging.group_log (
    id auto_increment primary key,
    group_id int,
    user_id int,
    user_id_from bigint,
    event varchar(200),
    "datetime" datetime
)
order by
    id --segmented by hash(message_id) all nodes
    partition by "datetime" :: date
group by
    calendar_hierarchy_day("datetime" :: date, 3, 2);