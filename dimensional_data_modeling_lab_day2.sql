-- create table players_scd(
-- 			players_name TEXT,
-- 			scoring_class scoring_class,
-- 			is_active BOOLEAN,
-- 			start_season integer,
-- 			end_season integer,		
-- 			current_season integer,
-- 			primary key (players_name, start_season)
-- );

insert into players_scd

with with_previous as(
select 
		player_name, 
		scoring_class, 
		current_season,
		is_active,
		LAG(scoring_class, 1) over (partition by player_name order by current_season) as previous_scoring_class,
		LAG(is_active, 1) over (partition by player_name order by current_season) as previous_is_active
from players
where current_season <= 2022
),

with_indicator as(
select * , 
		case when scoring_class <> previous_scoring_class then 1 
			 when is_active <> previous_is_active then 1 
		else 0 
		end as change_indicator
from with_previous),

with_streaks as(
select *,
		sum(change_indicator)
		over (partition by player_name order by current_season) as streak_identifier
from with_indicator
)

select  player_name,
		scoring_class,
		is_active,
		min(current_season) as start_season,
		max(current_season) as end_season,
		2021 as current_season
from with_streaks
group by player_name, is_active, scoring_class, streak_identifier
order by player_name, streak_identifier
		
------- part 2
-- create type scd_type as (
-- 			scoring_class scoring_class,
-- 			is_active boolean, 
-- 			start_season integer,
-- 			end_season integer
-- )

with last_season_scd as (
select * from players_scd 
where current_season = 2021  	
and end_season = 2021
),

historical_scd as (
select 
players_name,
scoring_class,
is_active,
start_season,
end_season

from players_scd 
where current_season = 2021 
and end_season < 2021 
), 

this_season_data as (
select * from players 
where current_season = 2022 
),

unchanged_records as(
select  ts.player_name,
		ts.scoring_class,
		ts.is_active,
		ls.start_season,
		ls.current_season as end_season
	from this_season_data ts
	join last_season_scd ls
	on ts.player_name = ls.players_name
	where ts.scoring_class = ls.scoring_class 
	and ts.is_active = ls.is_active
),
changed_records as(
select  ts.player_name,
		unnest(array[	
		row(
		ls.scoring_class,
		ls.is_active,
		ls.start_season,
		ls.end_season
		)::scd_type,
		row(
		ts.scoring_class,
		ts.is_active,
		ts.current_season,
		ts.current_season
		)::scd_type]) as records
	from this_season_data ts
	left join last_season_scd ls
	on ts.player_name = ls.players_name
	where (ts.scoring_class <> ls.scoring_class
	or ts.is_active <> ls.is_active)
--	or ls.players_name is null 
),
unnested_changed_records as (
select player_name,
(records::scd_type).scoring_class,
(records::scd_type).is_active,
(records::scd_type).start_season,
(records::scd_type).end_season
from changed_records
),
new_records as(
		 select
            ts.player_name,
                ts.scoring_class,
                ts.is_active,
                ts.current_season as start_season,
                ts.current_season as end_season
		from this_season_data ts
left join last_season_scd ls
on ts.player_name = ls.players_name
where ls.players_name is null
)

select * from historical_scd
union all 
select * from unchanged_records
union all
select * from unnested_changed_records
union all 
select * from new_records 


 
 




