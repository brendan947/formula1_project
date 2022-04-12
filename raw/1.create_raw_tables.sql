-- Databricks notebook source
create database if not exists f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create circuits file

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(circuitsId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path "/mnt/formula1dl100/raw/circuits.csv", header true)

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create races tables

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING
)
USING csv
OPTIONS (path "/mnt/formula1dl100/raw/races.csv", header true)

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors(constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
USING json
OPTIONS (path "/mnt/formula1dl100/raw/constructors.json", header true)

-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(driverId INT,
driverRef STRING,
number STRING,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING
)
USING json
OPTIONS (path "/mnt/formula1dl100/raw/drivers.json", header true)

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results(raceId INT,
driverId INT,
constructorId INT,
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points FLOAT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusID STRING
)
USING json
OPTIONS (path "/mnt/formula1dl100/raw/results.json")

-- COMMAND ----------

select * from f1_raw.results;

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING
)
USING json
OPTIONS (path "/mnt/formula1dl100/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops;

-- COMMAND ----------

drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times(raceId INT,
lap INT,
position INT,
time STRING,
milliseconds STRING
)
USING csv
OPTIONS (path "/mnt/formula1dl100/raw/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times;

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(qualifyId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING
) USING json
OPTIONS (path "/mnt/formula1dl100/raw/qualifying", multiLine true)

-- COMMAND ----------

select *
from f1_raw.qualifying;

-- COMMAND ----------

 