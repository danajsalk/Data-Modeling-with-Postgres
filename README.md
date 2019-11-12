## How to run
Initialize the PostgreSQL instance using the create_tables script. The main() function launches the creation of the database connection. <br>

 The general procedure of this ETL is copying data from one or more sources, in this case single raw files are read and processed from song_data
 and log_data, temporarily store them using Sparkify. This now represents the data as organized/filtered data tables. The analytical goals of 
 this ETL are to easily pull data from different sources and efficiently process it into useful information or statistics on the overall data
 size and shape. The database schema is structured around the data stored in seperate PostgreSQL tables to better understand what is store. This
 follows a star schema optimized for queries on song play analysis.<br>

References/Templates: <br>
[Project 1](https://github.com/lucaskjaero/udacity-data-engineering-projects/tree/master/Project%201%20-%20Data%20Modeling%20with%20PostgreSQL) <br>

[DOUBLE PRECISION](https://www.postgresql.org/docs/9.0/datatype-numeric.html)
[DATA TYPES:](https://www.postgresql.org/docs/9.5/datatype.html)
[DATA TYPES2:](http://www.postgresqltutorial.com/postgresql-data-types/)

[Udacity Project Template Details:](https://classroom.udacity.com/nanodegrees/nd027/parts/f7dbb125-87a2-4369-bb64-dc5c21bb668a/modules/c0e48224-f2d0-4bf5-ac02-3e1493e530fc/lessons/01995bb4-db30-4e01-bf38-ff11b631be0f/concepts/1533c19b-0505-49fd-b1b7-06c987641f0d)
[Udacity SongDataSet/LogDataSet Details:](https://classroom.udacity.com/nanodegrees/nd027/parts/f7dbb125-87a2-4369-bb64-dc5c21bb668a/modules/c0e48224-f2d0-4bf5-ac02-3e1493e530fc/lessons/01995bb4-db30-4e01-bf38-ff11b631be0f/concepts/a5609601-2314-4d8b-a7cf-e40048b3ee96)

Do the following steps in your README.md file.

Provide example queries and results for song play analysis.
SELECT COUNT(*) FROM songplays sp

