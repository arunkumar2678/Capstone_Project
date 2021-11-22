
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
                                offense_score,
                                defense_score,
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
                                season,
                                week,
                                season_type,                                         
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

drive_dim_table_insert = """insert into public.drive_dim(select distinct
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

