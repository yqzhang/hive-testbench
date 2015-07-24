@echo off
setlocal EnableExtensions

:: Tables in the TPC-H schema
SET TABLES="part partsupp supplier customer orders lineitem nation region"

:: Get the parameters
SET DIR=/tmp/tpch-generate
SET SCALE=10
SET BUCKETS=13

:: Do the actual data load.
CALL hdfs dfs -mkdir -p %DIR%
CALL hdfs dfs -ls %DIR%/%SCALE%

echo "Generating data at scale factor %SCALE%."
cd tpch-gen
CALL hadoop jar target/*.jar -d %DIR%/%SCALE% -s %SCALE%
cd ..

CALL hdfs dfs -ls %DIR%/%SCALE%

echo "TPC-H text data generation complete."

:: Create the text/flat tables as external tables. These will be later be converted to ORCFile.
echo "Loading text data into external tables."
CALL hive -i settings\load-flat.sql -f ddl-tpch\bin_flat\alltables.sql ^
  -d DB=tpch_text_%SCALE% -d LOCATION=%DIR%/%SCALE%

:: Create the optimized tables
SET i=1
SET total=8
SET database=tpch_flat_orc_%SCALE%

setlocal EnableDelayedExpansion
for %%t in %TABLES% do(
  echo "Optimizing table %%t (%i% / %total%)"
  CALL hive -i settings\load-flat.sql -f ddl-tpch\bin_flat\%%t.sql ^
    -d DB=%DATABASE% ^
    -d SOURCE=tpch_text_%SCALE% -d BUCKETS=%BUCKETS% ^
    -d FILE=orc
  SET /A i+=1
)

echo "Data loaded into database %DATABASE%."
