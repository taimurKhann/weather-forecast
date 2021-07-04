weather_daily_table = "weather_daily_data"
weather_staging_table = "weather_staging_data"
change_schema = '''
    SET search_path TO {};
'''

create_table_query = """
    CREATE SCHEMA IF NOT EXISTS comatch;

    CREATE TABLE IF NOT EXISTS comatch.weather_staging_data (
        station varchar,
        time varchar,
        year varchar,
        month varchar,
        day varchar,
        tavg varchar
    );

    CREATE TABLE IF NOT EXISTS comatch.weather_daily_data (
        station varchar,
        time date,
        year int,
        month int,
        day int,
        tavg numeric(5,1)
    );

    CREATE INDEX IF NOT EXISTS idx_wdd_stym ON comatch.weather_daily_data (station, time, year, month);

    CREATE TABLE IF NOT EXISTS comatch.weather_monthly_aggregate (
        station varchar,
        year int,
        month int,
        tavg numeric(5,1)
    );

    CREATE INDEX IF NOT EXISTS idx_wma_sym ON comatch.weather_monthly_aggregate (station, year, month);
"""

history_table_deletion = """
    TRUNCATE TABLE comatch.weather_staging_data;

    TRUNCATE TABLE comatch.weather_daily_data;

    TRUNCATE TABLE comatch.weather_monthly_aggregate;
"""

daily_table_deletion = """
    TRUNCATE TABLE comatch.weather_staging_data;

    DELETE FROM comatch.weather_daily_data
    WHERE time = '{}';

    DELETE FROM comatch.weather_monthly_aggregate
    WHERE year = {}
    and month = {};
"""

insertion_from_staging = """
    insert into 
    	comatch.weather_daily_data 
    select 
    	station, 
    	date("time"), 
    	cast("year" as int), 
    	cast("month" as int), 
    	cast("day" as int), 
    	cast(nullif(tavg ,'') as numeric)
    from 
    	comatch.weather_staging_data 
    where 
    	date("time") <= now();
"""

historical_monthly_agg_query = """
    INSERT INTO 
        comatch.weather_monthly_aggregate
    select 
    	station,
    	"year",
    	"month",
    	avg(tavg) 
    from 
    	comatch.weather_daily_data
    group by 
    	station,
    	"year",
    	"month";
""" 

daily_monthly_agg_query = """
    INSERT INTO 
        comatch.weather_monthly_aggregate
    select 
    	station,
    	"year",
    	"month",
    	avg(tavg) 
    from 
    	comatch.weather_daily_data
    where
        year = {}
	    and month = {}
    group by 
    	station,
    	"year",
    	"month";
""" 

tegel_monthly_tavg_report = """
    select 
        * 
    from 
        comatch.weather_monthly_aggregate
    where 
        station = 'Berlin / Tegel'
        and month = 2
    order by 
        year desc;
"""