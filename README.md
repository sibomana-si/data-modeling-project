# SPARKIFY SONG PLAY ANALYSIS

## Description
This codebase implements a [star schema](https://en.wikipedia.org/wiki/Star_schema) design on a Postgres database, as well as an [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) pipeline for loading data into the database tables.
The goal is to enable the Sparkify analytics team to easily analyze the data they've been collecting on songs and user activity through their music streaming app.  
Currently the data resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


## Database Design and ETL Pipeline
The database schema design that we chose is the star schema as it facilitates simplified queries and fast aggregations. The requirement from the Sparkify analytics team was for a database with tables designed to optimize queries on song play analysis. Our design consists of one fact table (songplays), and four dimension tables (users, songs, artists, and time).


#### FACT Table

|            **songplays**          |
|:------------------------------:	|
| songplay_id SERIAL PRIMARY KEY 	|
| start_time BIGINT NOT NULL     	|
| user_id INT NOT NULL           	|
| level TEXT                     	|
| song_id TEXT NOT NULL          	|
| artist_id TEXT NOT NULL        	|
| session_id INT NOT NULL        	|
| location TEXT                  	|
| user_agent TEXT                	|


#### DIMENSION Tables

##### 1. users table

|           **users**       |		
|:------------------------:	|
| user_id INT PRIMARY KEY  	|
| first_name TEXT NOT NULL 	|
| last_name TEXT NOT NUL   	|
| gender CHAR              	|
| level TEXT               	|


##### 2. songs table

|           **songs**       |
|:------------------------:	|
| song_id TEXT PRIMARY KEY 	|
| title TEXT NOT NULL      	|
| artist_id TEXT NOT NULL  	|
| year INT                 	|
| duration NUMERIC         	|


##### 3. artists table

|           **artists**         |
|:--------------------------:	|
| artist_id TEXT PRIMARY KEY 	|
| name TEXT NOT NULL         	|
| location TEXT              	|
| latitude NUMERIC           	|
| longitude NUMERIC          	|


##### 4. time table

|              **time**             |
|:-----------------------------:	|
| start_time BIGINT PRIMARY KEY 	|
| hour INT                      	|
| day INT                       	|
| week INT                      	|
| month INT                     	|
| year INT                      	|
| weekday INT                   	|


#### ETL Pipeline

The ETL pipeline was designed in a modular way and consists of three modules:
- **sql_queries.py:** contains all the sql queries used during the ETL process. This includes the queries for creating the sparkify database, 
creating/dropping the fact and dimension tables, as well as inserting data into the tables.
- **create_tables.py:** creates the postgres database, as well as the fact and dimension tables.
- **etl.py:** processes the song and log files to extract the required data, loads the data into the dimension tables, and populates the fact table.


##### Usage
1. create the sparkify database and tables
	`$ python create_tables.py`
	
2. load the song and log data into the fact and dimension tables
	`$ python etl.py`
	
	