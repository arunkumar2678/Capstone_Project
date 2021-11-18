"""
Create the fact and dimension tables.

"""

# CREATE TABLES
game_fact_table_create = """drop table if exists game_fact;
  create table if not exists game_fact (
  game_id int not null,
  team_id int not null,
  drive_id bigint not null,
  play_id bigint not null,
  conference_id smallint not null,
  yards_gained integer not null
);"""

conference_dim_table_create = """create table if not exists conference_dim (
  conference_id smallint not null,
  conference_full_name varchar(256), 
  conference_short_name varchar(256), 
  abbreviation varchar(10)
);""" 

team_dim_table_create = """create table if not exists team_dim (
  team_id int not null, 
  school varchar(256) not null,
  mascot varchar(256) not null,
  team_abbreviation varchar(20) not null,
  alt_name1 varchar(100),
  alt_name2 varchar(100),
  alt_name3 varchar(100),
  division varchar(256) not null,
  color varchar(256) not null,
  alt_color varchar(256) not null,
  logos0 varchar(256) not null,
  logos1 varchar(256) not null,
  venue_id int not null, 
  venue_name varchar(256) not null,
  location_city varchar(50) not null ,
  location_state varchar(50) not null,
  location_zip varchar(50) not null,
  location_country_code varchar(100) not null,
  location_timezone varchar(100) not null,
  location_latitude float not null,
  location_longitude float not null,
  location_elevation float not null,
  location_capacity int not null,
  location_year_constructed int not null,
  location_grass boolean not null,
  location_dome boolean not null
);"""

game_dim_table_create = """create table if not exists game_dim (
  game_id int not null,
  neutral_site boolean not null,
  conference_game boolean not null,
  attendance int,  
  venue_id int,
  venue varchar(256), 
  home_team_id int not null,
  away_team_id int not null,
  home_team_points int,  
  away_team_points int,  
  highlights varchar (256), 
  notes varchar (256)
);"""

drive_dim_table_create = """create table if not exists drive_dim (
  drive_id bigint not null,
  offense varchar(256) not null,
  defense varchar(256) not null,
  drive_number smallint not null,
  scoring boolean not null,
  start_period int not null,
  start_yardline smallint not null,
  start_yards_to_goal smallint not null,
  start_time_minutes smallint not null,
  start_time_seconds smallint not null,
  end_period smallint not null,
  end_yardline smallint not null,
  end_yards_to_goal smallint not null,
  end_time_minutes smallint not null,
  end_time_seconds smallint not null,
  elapsed_minutes smallint not null,
  elapsed_seconds smallint not null,
  plays smallint not null,
  yards smallint not null,
  drive_result varchar(100) not null,
  is_home_offense boolean not null,
  start_offense_score smallint not null,
  start_defense_score smallint not null,
  end_offense_score smallint not null,
  end_defense_score smallint not null
);"""

play_dim_table_create = """create table if not exists play_dim (
  play_id bigint not null,
  offense_team_id int not null,
  defense_team_id int not null,
  play_number smallint not null,
  period smallint not null,
  clock_minutes	smallint not null,
  clock_seconds	smallint not null,
  offense_timeouts smallint,  
  defense_timeouts smallint,  
  yard_line	smallint not null,
  yards_to_goal	smallint not null,
  down smallint not null,
  distance integer not null,
  scoring boolean not null,
  yards_gained integer not null,
  play_type varchar(256) not null,
  play_type varchar(2000),  
  ppa float,  
  wallclock timestamp
);"""


playtype_dim_table_create = """create table if not exists playtype_dim(
play_type_id smallint null, 
play_type varchar(2000),  
abbreviation varchar(10)
);"""


betting_line_dim_table_create = """create table betting_line_dim (
  betting_line_dim_id bigint IDENTITY(1,1),
  game_id int,
  home_line_scores_0 int,  
  home_line_scores_1 int,  
  home_line_scores_2 int,  
  home_line_scores_3 int,  
  home_post_win_prob float,  
  away_line_scores_0 int,  
  away_line_scores_1 int,  
  away_line_scores_2 int,  
  away_line_scores_3 int,  
  away_post_win_prob float,  
  excitement_index float
);"""

# QUERY LISTS
#create_table_queries = [game_fact_table_create, conference_dim_table_create, team_dim_table_create, game_dim_table_create, drive_dim_table_create, play_dim_table_create, playtype_dim_table_create, betting_line_dim_table_create]
#drop_table_queries = [game_fact_table_drop, conference_dim_table_drop, team_dim_table_drop, game_dim_table_drop, drive_dim_table_drop, play_dim_table_drop, playtype_dim_table_drop, betting_line_dim_table_drop]




game_fact_table_insert = """Insert into public.game_fact (select distinct 
                                c.game_id, 
                                b.team_id, 
                                d.drive_id, 
                                e.play_id, 
                                a.conference_id, 
                                e.yards_gained 
                            from conference a 
                            join team b on a.conference_id = b.conference_id 
                            join game c on b.team_id = c.home_team_id
                            join drive d on c.game_id = d.game_id
                            join play e on d.drive_id = e.drive_id
                            );"""

conference_dim_table_insert = """Insert into public.conference_dim(select * from public.conference);"""

team_dim_table_insert = """insert into public.team_dim (select 
                              team_id, 
                              school,
                              mascot,
                              team_abbreviation,
                              alt_name1,
                              alt_name2,
                              alt_name3,
                              division,
                              color,
                              alt_color,
                              logos0,
                              logos1,
                              venue_id, 
                              venue_name,
                              location_city,
                              location_state,
                              location_zip,
                              location_country_code,
                              location_timezone,
                              location_latitude,
                              location_longitude,
                              location_elevation,
                              location_capacity,
                              location_year_constructed,
                              location_grass,
                              location_dome
                        from public.team
                        );"""

play_dim_table_insert = """insert into public.play_dim (select 
                                play_id,
                                offense_team_id,
                                defense_team_id, 
                                play_number, 
                                period, 
                                clock_minutes, 
                                clock_seconds, 
                                offense_timeouts,
                                defense_timeouts,
                                yard_line, 
                                yards_to_goal,
                                down,
                                distance,
                                scoring, 
                                yards_gained,
                                play_type,  
                                play_text, 
                                ppa, 
                                wallclock
                        from public.play
                        );"""

game_dim_table_insert = """insert into public.game_dim (select
                                game_id,
                                neutral_site,
                                conference_game,
                                attendance,
                                venue_id,
                                venue,
                                home_team_id,
                                away_team_id,
                                home_team_points,
                                away_team_points,
                                highlights,
                                notes
                            from public.game
                            );"""

drive_dim_table_insert = """insert into public.drive_dim(select
                                drive_id,
                                offense,
                                defense,
                                drive_number,
                                scoring,
                                start_period,
                                start_yardline,
                                start_yards_to_goal,
                                start_time_minutes,
                                start_time_seconds,
                                end_period,
                                end_yardline,
                                end_yards_to_goal,
                                end_time_minutes,
                                end_time_seconds,
                                elapsed_minutes,
                                elapsed_seconds,
                                plays,
                                yards,
                                drive_result,
                                is_home_offense,
                                start_offense_score,
                                start_defense_score,
                                end_offense_score,
                                end_defense_score
                            from public.drive
                            );"""

playtype_dim_table_insert = """insert into public.playtype_dim(select
                                play_type_id,
                                play_type,
                                abbreviation                                 
                            from public.playtype
                            );"""

betting_line_dim_table_insert = """insert into public.betting_line_dim (game_id,
                                      home_line_scores_0,
                                      home_line_scores_1,
                                      home_line_scores_2,
                                      home_line_scores_3,
                                      home_post_win_prob,
                                      away_line_scores_0,
                                      away_line_scores_1,
                                      away_line_scores_2,
                                      away_line_scores_3,
                                      away_post_win_prob,
                                      excitement_index
                                     ) 
                                (select
                                      game_id,
                                      home_line_scores_0,
                                      home_line_scores_1,
                                      home_line_scores_2,
                                      home_line_scores_3,
                                      home_post_win_prob,
                                      away_line_scores_0,
                                      away_line_scores_1,
                                      away_line_scores_2,
                                      away_line_scores_3,
                                      away_post_win_prob,
                                      excitement_index
                                from public.game
                                );"""

