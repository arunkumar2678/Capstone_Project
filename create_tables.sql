CREATE TABLE conference (
    conference_id smallint NOT NULL,
    conference_full_name varchar(256) NULL,
    conference_short_name varchar(256) NULL,
    conference_abbreviation varchar(20),
    CONSTRAINT conference_pkey PRIMARY KEY (conference_id)
)

CREATE TABLE team (
    team_id int NOT NULL, 
    conference_id int NOT NULL,
    school varchar(256) NOT NULL,
    mascot varchar(256) NOT NULL,
    team_abbreviation varchar(20) NOT NULL,
    alt_name1 varchar(100),
    alt_name2 varchar(100),
    alt_name3 varchar(100),
    division varchar(256) NOT NULL,
    color varchar(256) NOT NULL,
    alt_color varchar(256) NOT NULL,
    logos0 varchar(256) NOT NULL,
    logos1 varchar(256) NOT NULL,
    venue_id int NOT NULL, 
    venue_name varchar(256) NOT NULL,
    location_city varchar(50) NOT NULL ,
    location_state varchar(50) NOT NULL,
    location_zip varchar(50) NOT NULL,
    location_country_code varchar(100) NOT NULL,
    location_timezone varchar(100) NOT NULL,
    location_latitude float NOT NULL,
    location_longitude float NOT NULL,
    location_elevation float NOT NULL,
    location_capacity int NOT NULL,
    location_year_constructed int NOT NULL,
    location_grass boolean NOT NULL,
    location_dome boolean NOT NULL,
    CONSTRAINT team_pkey PRIMARY KEY (team_id)
)

CREATE TABLE game (
    game_id int NOT NULL,
    season int NOT NULL,
    week int NOT NULL, 
    season_type varchar(50) NOT NULL, 
    start_date timestamp NOT NULL,
    neutral_site boolean NOT NULL,
    conference_game boolean NOT NULL,
    attendance int,
    venue_id int,
    venue varchar(256) NULL,
    home_team_id int NOT NULL,
    home_team varchar(256) NOT NULL,
    home_points int,
    home_line_scores_0 int,
    home_line_scores_1 int,
    home_line_scores_2 int,
    home_line_scores_3 int,
    home_post_win_prob float,
    away_team_id int NOT NULL,
    away_team varchar(256) NOT NULL,
    away_points int,
    away_line_scores_0 int,
    away_line_scores_1 int, 
    away_line_scores_2 int,
    away_line_scores_3 int,
    away_post_win_prob float,
    excitement_index float,
    highlights varchar (256), 
    notes varchar (256),
    CONSTRAINT game_pk PRIMARY KEY (game_id)
)

CREATE TABLE drive(
    drive_id int NOT NULL,
    game_id bigint NOT NULL,
    offense varchar(256) NOT NULL,
    offense_conference varchar(256),
    defense varchar(256) NOT NULL,
    defense_conference varchar(256),
    drive_number smallint NOT NULL,
    scoring boolean NOT NULL,
    start_period int NOT NULL,
    start_yardline smallint NOT NULL,
    start_yards_to_goal smallint NOT NULL,
    start_time_minutes smallint NOT NULL,
    start_time_seconds smallint NOT NULL,
    end_period smallint NOT NULL,
    end_yardline smallint NOT NULL,
    end_yards_to_goal smallint NOT NULL,
    end_time_minutes smallint NOT NULL,
    end_time_seconds smallint NOT NULL,
    elapsed_minutes smallint NOT NULL,
    elapsed_seconds smallint NOT NULL,
    plays smallint NOT NULL,
    yards smallint NOT NULL,
    drive_result varchar(100) NOT NULL,
    is_home_offense boolean NOT NULL,
    start_offense_score smallint NOT NULL,
    start_defense_score smallint NOT NULL,
    end_offense_score smallint NOT NULL,
    end_defense_score smallint NOT NULL,
    CONSTRAINT drive_pk PRIMARY KEY (drive_id)
)


CREATE TABLE play (
    play_id bigint NOT NULL,
    game_id bigint NOT NULL,
    drive_id bigint NOT NULL,
    offense_team_id int NOT NULL, 
    offense_team varchar(256) NOT NULL,
    defense_team_id int NOT NULL,
    defense_team varchar(256) NOT NULL, 
    drive_number smallint NOT NULL,
    play_number smallint NOT NULL,
    period  smallint NOT NULL,
    clock_minutes smallint NOT NULL,
    clock_seconds smallint NOT NULL,
    offense_timeouts smallint,
    defense_timeouts smallint,
    yard_line smallint NOT NULL,
    yards_to_goal smallint NOT NULL,
    down smallint NOT NULL,
    distance integer NOT NULL,
    scoring boolean NOT NULL,
    yards_gained integer NOT NULL,
    play_type varchar(256) NOT NULL, 
    play_text varchar(2000), 
    ppa float, 
    wallclock timestamp,
    CONSTRAINT plays_pk PRIMARY KEY (play_id)
 )

CREATE TABLE playtype(
    play_type_id smallint NOT NULL,
    play_type varchar(256) NOT NULL, 
    abbreviation varchar(10),
    CONSTRAINT playtypes_pk PRIMARY KEY  (play_type_id)
)
