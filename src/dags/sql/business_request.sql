with user_group_messages as (
select
	hg.hk_group_id,
	count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
from
	h_groups hg
left join l_groups_dialogs lgd on
	lgd.hk_group_id = hg.hk_group_id
left join l_user_message lum on
	lgd.hk_message_id = lum.hk_message_id
group by
	hg.hk_group_id),
user_group_log as (
select
	luga.hk_group_id,
	count(distinct luga.hk_user_id) cnt_added_users
from
	l_user_group_activity luga
where
	luga.hk_l_user_group_activity in
(
	select
		sah.hk_l_user_group_activity
	from
		s_auth_history sah
	where
		event = 'add')
	and luga.hk_group_id in 
(
	select
		hg.hk_group_id
	from
		h_groups hg
	order by
		hg.registration_dt
	limit 10)
group by
	luga.hk_group_id)
select
	ugl.hk_group_id,
	ugl.cnt_added_users,
	ugm.cnt_users_in_group_with_messages,
	round(ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users, 2) as group_conversion
from
	user_group_log ugl
left join user_group_messages ugm on
	ugm.hk_group_id = ugl.hk_group_id
order by
	group_conversion desc
;

