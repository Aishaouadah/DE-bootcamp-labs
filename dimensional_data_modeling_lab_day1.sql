-- select * from player_seasons
-- order by player_name;
-- create type season_stats as (
-- 						season INTEGER,
-- 						gp INTEGER,
-- 						pts REAL,
-- 						reb REAL,
-- 						ast REAL
-- );

-- create type scoring_class as enum ('star', 'good', 'average', 'bad');

-- create TABLE players (
-- 			 player_name TEXT,
-- 			 height TEXT,
-- 			 college TEXT,
-- 			 country TEXT,
-- 			 draft_year text,
-- 			 draft_round TEXT,
-- 			 draft_number TEXT,
-- 			 season_stats season_stats[],
-- 			 scoring_class scoring_class,
-- 			 years_since_last_season INTEGER,
-- 			 current_season INTEGER,
--              is_active BOOLEAN,
-- 			 PRIMARY KEY(player_name, current_season)
-- );

-- drop table players;

-- select * from player_seasons; 
-- select max(season)  from player_seasons; 


with yesterday as (
select * from players
where current_season = 2021
),
today as(
select * from player_seasons 
where season = 2022
)
insert into players
select  
coalesce (t.player_name, y.player_name) as player_name,
coalesce (t.height, y.height) as height,
coalesce (t.college, y.college) as college,
coalesce (t.country, y.country) as country,
coalesce (t.draft_year, y.draft_year) as draft_year,
coalesce (t.draft_round, y.draft_round) as draft_round,
coalesce (t.draft_number, y.draft_number) as draft_number,

case when y.season_stats is null 
then ARRAY [row(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
when t.season is not null then y.season_stats || ARRAY [row(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
else y.season_stats
end as season_stats,

case when t.season is not null then
case when t.pts > 20 then 'star'
	 when t.pts > 15 then 'good'
	 when t.pts > 10 then 'average'
	 else 'bad'
end::scoring_class
else y.scoring_class
end,

case when t.season is not null then 0
else y.years_since_last_season + 1
end as years_since_last_season,
coalesce(t.season, y.current_season+1) as current_season,
t.season IS NOT NULL as is_active
 
from today t full outer join yesterday y
on t.player_name = y.player_name;


select * from players where current_season = 2022;

select player_name, (season_stats[CARDINALITY(season_stats)]::season_stats).pts / 
case when (season_stats[1]::season_stats).pts = 0 then 1 else (season_stats[1]::season_stats).pts end
from players 
where current_season = 2001;

-- with unnested as (
-- select player_name, 
-- 		UNNEST(season_stats)::season_stats as season_stats
-- from players
-- where current_season = 2001 
-- and player_name = 'Michael Jordan'
-- )

-- select *
-- from unnested;
select player_name, scoring_class, current_season
from players;
select * from players;
