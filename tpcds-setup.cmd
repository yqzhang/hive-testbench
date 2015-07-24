@echo off
setlocal EnableExtensions

:: :: Tables in the TPC-DS schema.
SET DIMS=(date_dim time_dim item customer customer_demographics household_demographics customer_address store promotion warehouse ship_mode reason income_band call_center web_page catalog_page web_site)
SET FACTS=(store_sales store_returns web_sales web_returns catalog_sales catalog_returns inventory)

:: Configurations
SET DIR=/tmp/tpcds-generate
SET SCALE=10
SET FORMAT=orc
SET BUCKETS=13
SET RETURN_BUCKETS=13

:: Do the actual data load.
CALL hdfs dfs -mkdir -p %DIR%
CALL hdfs dfs -ls %DIR%/%SCALE%

cd tpcds-gen\
CALL hadoop jar .\target\*.jar -d %DIR%/%SCALE%/ -s %SCALE%
cd ..

CALL hdfs dfs -ls %DIR%/%SCALE%

echo "TPC-DS text data generation complete."

:: Create the text/flat tables as external tables. These will be later be converted to ORCFile.
CALL hive -i settings\load-flat.sql -f ddl-tpcds\text\alltables.sql ^
  -d DB=tpcds_text_%SCALE% -d LOCATION=%DIR%/%SCALE%

:: Create the partitioned and bucketed tables.
SET i=1
SET total=24
SET DATABASE=tpcds_bin_partitioned_%FORMAT%_%SCALE%

setlocal EnableDelayedExpansion
for %%t in %FACTS% do (
  echo "Optimizing table %%t (%i% / %total%)"
  CALL hive -i settings\load-partitioned.sql ^
    -f ddl-tpcds\bin_partitioned\%%t.sql ^
    -d DB=tpcds_bin_partitioned_%FORMAT%_%SCALE% ^
    -d SOURCE=tpcds_text_%SCALE% -d BUCKETS=%BUCKETS% ^
    -d RETURN_BUCKETS=%RETURN_BUCKETS% -d FILE=%FORMAT%
  SET /A i+=1
)

:: Populate the smaller tables
for %%t in %DIMS% do (
  echo "Optimizing table %%t (%i% / %total%)"
  CALL hive -i settings\load-partitioned.sql ^
    -f ddl-tpcds\bin_partitioned\%%t.sql ^
    -d DB=tpcds_bin_partitioned_%FORMAT%_%SCALE% -d SOURCE=tpcds_text_%SCALE% ^
    -d FILE=%FORMAT%
  SET /A i+=1
)

echo "Data loaded into database %DATABASE%."
