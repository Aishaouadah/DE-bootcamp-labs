create type vertex_type as enum('player','team','game');
-- drop type vertex_type cascade
-- drop table vertices 
create table vertices(
			identifier text,
			type vertex_type,
			properties json,
			primary key(identifier, type)			
);
create type edge_type as 
		enum(
				'plays_against',
				'shares_team',
				'plays_in',
				'plays_on'
					);
drop table edges;
create table edges(
			--edge_id text, 
			subject_identifier text,
			subject_type vertex_type,
			object_identifier text,
			object_type vertex_type,
			edge_type edge_type,
			properties json,
			primary key(
			subject_identifier,
			subject_type,
			object_identifier,
			object_type,
			edge_type
			)
);

insert into vertices 
select game_id as identifier,
'game'::vertex_type as type,
json_build_object(
'pts_home', pts_home,
'pts_away', pts_away,
'winning_team', case when home_team_wins = 1 then home_team_id else visitor_team_id end
)
from games;

insert into vertices 
with players_agg as(
select 
		player_id as identifier,
		max(player_name) as player_name,
		count(1) as number_of_games,
		sum(pts) as total_points,
		array_agg(distinct team_id) as teams
from game_details
group by player_id 
)
select  identifier, 
		'player'::vertex_type,
		json_build_object(
		'player_name', player_name,
		'number_of_games', number_of_games,
		'total_points', total_points,
		'teams', teams
		)
from players_agg;

insert into vertices
with teams_deduped as (
select * , row_number() over (partition by team_id) as row_num 
from teams
)

select 
	team_id as identifier,
	'team'::vertex_type as type,
	json_build_object(
		'abbreviation', abbreviation,
		'nickname', nickname,
		'city', city,
		'arena', arena,
		'year_founded', yearfounded
	)
from teams_deduped
where row_num = 1;

insert into edges
with games_deduped as (
select * , row_number() over (partition by player_id, game_id) as row_num 
from game_details
),
filtered as (
	select * from games_deduped
	where row_num = 1
)
select 
f1.player_name,
f1.player_id,
f2.player_name,
f2.player_id,
f1.team_abbreviation,
f2.team_abbreviation,
case when f1.team_abbreviation = f2.team_abbreviation then 'shares_team'::edge_type
else 'plays_against'::edge_type
end,
count(1) as num_games,
sum(f1.pts) as left_points,
sum(f2.pts) as right_points
from filtered f1
join filtered f2 
on f1.game_id = f2.game_id
and f1.player_name <> f2.player_name
group by 
f1.player_name,
f1.player_id,
f2.player_name,
f2.player_id,
f1.team_abbreviation,
f2.team_abbreviation,
case when f1.team_abbreviation = f2.team_abbreviation then 'shares_team'::edge_type
else 'plays_against'::edge_type
end;

select 
player_id as subject_identifier,
'player'::vertex_type as subject_type,
game_id as object_identifier,
'game'::vertex_type as object_type,
'plays_in'::edge_type as edge_type,
json_build_object(
 			'start_position', start_position,
			 'pts', pts,
			 'team_id', team_id,
			 'team_abbreviation', team_abbreviation 
) as properties 
from games_deduped
where row_num = 1;


select * from edges;
-- select type, count(1)
-- from vertices
-- group by 1;

select 
v.properties->>'player_name',
max(cast(e.properties->>'pts' as integer))
from vertices v join edges e
on e.subject_identifier = v.identifier
and e.subject_type = v.type
group by 1;


