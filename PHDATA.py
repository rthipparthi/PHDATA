# Databricks notebook source
# DBTITLE 1,Configure Widgets
dbutils.widgets.text("snowflake_database","USER_RANJITH")
dbutils.widgets.text("snowflake_schema","NORTHWOODS_AIRLINE")
dbutils.widgets.text("snowflake_warehouse","INTERVIEW_WH")

# COMMAND ----------

# DBTITLE 1,Assign Widgets values
snowflake_warehouse = dbutils.widgets.get('snowflake_warehouse').strip()
snowflake_database = dbutils.widgets.get('snowflake_database').strip()
snowflake_schema = dbutils.widgets.get('snowflake_schema').strip()

# COMMAND ----------

# DBTITLE 1,CSV File Path
file_path = '/FileStore/tables/'
flights_path = '/FileStore/tables/partition*.csv'

# COMMAND ----------

# DBTITLE 1,Read Data Into Dataframe
airlines_df = spark.read.format('csv').option("header","true").option("inferSchema", "true").load(file_path+'airlines.csv')
airports_df = spark.read.format('csv').option("header","true").option("inferSchema", "true").load(file_path+'airports.csv')
flights_df = spark.read.option("header", "true").option("inferSchema", "true").csv(flights_path)

# COMMAND ----------

# DBTITLE 1,Configure Snowflake 
options = {
  "sfUrl": "wn35828.west-us-2.azure.snowflakecomputing.com",
  "sfUser": "ranjith90",
  "sfPassword": "Ranjith90",
  "sfDatabase": snowflake_database ,
  "sfSchema": snowflake_schema ,
  "sfWarehouse": snowflake_warehouse
}


# COMMAND ----------

# DBTITLE 1,Write Raw CSV to Snowflake tables
airlines_df.write.format("snowflake").options(**options).option("dbtable","airlines").mode("overwrite").save()
airports_df.write.format("snowflake").options(**options).option("dbtable","airports").mode("overwrite").save()
flights_df.write.format("snowflake").options(**options).option("dbtable","flights").mode("overwrite").save()

# COMMAND ----------

# DBTITLE 1,Create Temporary Databricks tables
airlines_df.createOrReplaceTempView("airlines")
airports_df.createOrReplaceTempView("airports")
flights_df.createOrReplaceTempView("flights")

# COMMAND ----------

# DBTITLE 1,Report 1 -Total number of flights by airline and airport on a monthly basis 
# MAGIC %sql
# MAGIC select
# MAGIC   a.AIRLINE,
# MAGIC   c.Airport,
# MAGIC   f.month,
# MAGIC   count(*) as number_of_flights
# MAGIC from
# MAGIC   flights as f
# MAGIC   join airlines as a on f.AIRLINE = a.IATA_CODE
# MAGIC   join airports as c on f.ORIGIN_AIRPORT = c.IATA_CODE
# MAGIC group by
# MAGIC   a.AIRLINE,
# MAGIC   c.Airport,
# MAGIC   f.month
# MAGIC order by
# MAGIC   number_of_flights

# COMMAND ----------

# DBTITLE 1,Report 2 - On time percentage of each airline for the year 2015 
# MAGIC %sql 
# MAGIC with query1 as (
# MAGIC   select
# MAGIC     a.AIRLINE,
# MAGIC     count(*) as on_time
# MAGIC   from
# MAGIC     flights as f
# MAGIC     join airlines as a on f.AIRLINE = a.IATA_CODE
# MAGIC   where
# MAGIC     f.year = 2015
# MAGIC     and (
# MAGIC       f.ARRIVAL_DELAY is null
# MAGIC       or f.ARRIVAL_DELAY = 0
# MAGIC     )
# MAGIC     and (
# MAGIC       f.DEPARTURE_DELAY is null
# MAGIC       or f.DEPARTURE_DELAY = 0
# MAGIC     )
# MAGIC     and (
# MAGIC       f.AIR_SYSTEM_DELAY is null
# MAGIC       or f.AIR_SYSTEM_DELAY = 0
# MAGIC     )
# MAGIC     and (
# MAGIC       f.SECURITY_DELAY is null
# MAGIC       or f.SECURITY_DELAY = 0
# MAGIC     )
# MAGIC     and (
# MAGIC       f.AIRLINE_DELAY is null
# MAGIC       or f.AIRLINE_DELAY = 0
# MAGIC     )
# MAGIC     and (
# MAGIC       f.LATE_AIRCRAFT_DELAY is null
# MAGIC       or f.LATE_AIRCRAFT_DELAY = 0
# MAGIC     )
# MAGIC     and (
# MAGIC       f.WEATHER_DELAY is null
# MAGIC       or f.WEATHER_DELAY = 0
# MAGIC     )group by
# MAGIC     a.airline
# MAGIC ),
# MAGIC query2 as (
# MAGIC   select
# MAGIC     count(*) as total_flights,
# MAGIC     a.AIRLINE
# MAGIC   from
# MAGIC     flights as f
# MAGIC     join airlines as a on f.AIRLINE = a.IATA_CODE
# MAGIC   where
# MAGIC     CANCELLED = 0
# MAGIC   group by
# MAGIC     a.AIRLINE
# MAGIC )
# MAGIC select
# MAGIC   a.AIRLINE,
# MAGIC   a.on_time,
# MAGIC   b.total_flights,
# MAGIC   ((a.on_time / b.total_flights) * 100) as on_time_percentage
# MAGIC from
# MAGIC   query1 as a,
# MAGIC   query2 as b
# MAGIC where
# MAGIC   a.AIRLINE = b.AIRLINE

# COMMAND ----------

# DBTITLE 1,Report 3 - Airlines with the largest number of delays 
# MAGIC %sql
# MAGIC select
# MAGIC   a.AIRLINE,
# MAGIC   count(*) as delays 
# MAGIC from
# MAGIC   flights as f
# MAGIC   join airlines as a on f.AIRLINE = a.IATA_CODE
# MAGIC where
# MAGIC   (
# MAGIC     f.ARRIVAL_DELAY is not null
# MAGIC     or f.ARRIVAL_DELAY != 0
# MAGIC   )
# MAGIC   or (
# MAGIC     f.DEPARTURE_DELAY is not null
# MAGIC     or f.DEPARTURE_DELAY != 0
# MAGIC   )
# MAGIC   or (
# MAGIC     f.AIR_SYSTEM_DELAY is not null
# MAGIC     or f.AIR_SYSTEM_DELAY != 0
# MAGIC   )
# MAGIC   or (
# MAGIC     f.SECURITY_DELAY is not null
# MAGIC     or f.SECURITY_DELAY != 0
# MAGIC   )
# MAGIC   or (
# MAGIC     f.AIRLINE_DELAY is not null
# MAGIC     or f.AIRLINE_DELAY != 0
# MAGIC   )
# MAGIC   or (
# MAGIC     f.LATE_AIRCRAFT_DELAY is null
# MAGIC     or f.LATE_AIRCRAFT_DELAY != 0
# MAGIC   )
# MAGIC   or (
# MAGIC     f.WEATHER_DELAY is not null
# MAGIC     or f.WEATHER_DELAY != 0
# MAGIC   )
# MAGIC group by
# MAGIC   a.airline
# MAGIC order by delays DESC

# COMMAND ----------

# DBTITLE 1,Report 4 - Cancellation reasons by airport 
# MAGIC %sql
# MAGIC select 
# MAGIC   a.AIRPORT,
# MAGIC   f.CANCELLATION_REASON
# MAGIC from
# MAGIC   flights as f
# MAGIC   join airports as a on f.ORIGIN_AIRPORT = a.IATA_CODE
# MAGIC where  f.CANCELLATION_REASON  is not null
# MAGIC group by  f.CANCELLATION_REASON,a.AIRPORT
# MAGIC   

# COMMAND ----------

# DBTITLE 1,Report 5 - Delay reasons by airport 
# MAGIC %sql with query1 as(
# MAGIC   select
# MAGIC     distinct a.AIRPORT,
# MAGIC     case
# MAGIC       when f.ARRIVAL_DELAY is not null then "ARRIVAL_DELAY"
# MAGIC     end as DELAY_REASON1,
# MAGIC     case
# MAGIC       when f.DEPARTURE_DELAY is not null then "DEPARTURE_DELAY"
# MAGIC     end as DELAY_REASON2,
# MAGIC     case
# MAGIC       when f.AIR_SYSTEM_DELAY is not null then "AIR_SYSTEM_DELAY"
# MAGIC     end as DELAY_REASON3,
# MAGIC     case
# MAGIC       when f.SECURITY_DELAY is not null then "SECURITY_DELAY"
# MAGIC     end as DELAY_REASON4,
# MAGIC     case
# MAGIC       when f.AIRLINE_DELAY is not null then "AIRLINE_DELAY"
# MAGIC     end as DELAY_REASON5,
# MAGIC     case
# MAGIC       when f.LATE_AIRCRAFT_DELAY is not null then "LATE_AIRCRAFT_DELAY"
# MAGIC     end as DELAY_REASON6,
# MAGIC     case
# MAGIC       when f.WEATHER_DELAY is not null then "WEATHER_DELAY"
# MAGIC     end as DELAY_REASON7
# MAGIC   from
# MAGIC     flights as f
# MAGIC     join airports as a on f.ORIGIN_AIRPORT = a.IATA_CODE
# MAGIC   where
# MAGIC     (
# MAGIC       f.ARRIVAL_DELAY is not null
# MAGIC       or f.ARRIVAL_DELAY != 0
# MAGIC     )
# MAGIC     or (
# MAGIC       f.DEPARTURE_DELAY is not null
# MAGIC       or f.DEPARTURE_DELAY != 0
# MAGIC     )
# MAGIC     or (
# MAGIC       f.AIR_SYSTEM_DELAY is not null
# MAGIC       or f.AIR_SYSTEM_DELAY != 0
# MAGIC     )
# MAGIC     or (
# MAGIC       f.SECURITY_DELAY is not null
# MAGIC       or f.SECURITY_DELAY != 0
# MAGIC     )
# MAGIC     or (
# MAGIC       f.AIRLINE_DELAY is not null
# MAGIC       or f.AIRLINE_DELAY != 0
# MAGIC     )
# MAGIC     or (
# MAGIC       f.LATE_AIRCRAFT_DELAY is null
# MAGIC       or f.LATE_AIRCRAFT_DELAY != 0
# MAGIC     )
# MAGIC     or (
# MAGIC       f.WEATHER_DELAY is not null
# MAGIC       or f.WEATHER_DELAY != 0
# MAGIC     )
# MAGIC ),
# MAGIC query2 as (
# MAGIC   select
# MAGIC     f.AIRPORT,
# MAGIC     CONCAT(
# MAGIC       collect_set(f.DELAY_REASON1),
# MAGIC       collect_set(f.DELAY_REASON2),
# MAGIC       collect_set(f.DELAY_REASON3),
# MAGIC       collect_set(f.DELAY_REASON4),
# MAGIC       collect_set(f.DELAY_REASON5),
# MAGIC       collect_set(f.DELAY_REASON6),
# MAGIC       collect_set(f.DELAY_REASON7)
# MAGIC     ) as Delay_reasons
# MAGIC   from
# MAGIC     query1 as f
# MAGIC   group by
# MAGIC     f.AIRPORT
# MAGIC )
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   query2

# COMMAND ----------

# DBTITLE 1,Report 6 - Airline with the most unique routes 
# MAGIC %sql
# MAGIC select
# MAGIC   AIRLINE,
# MAGIC   ORIGIN_AIRPORT,
# MAGIC   DESTINATION_AIRPORT
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       a.AIRLINE,
# MAGIC       f.ORIGIN_AIRPORT,
# MAGIC       f.DESTINATION_AIRPORT,
# MAGIC       row_number() over(
# MAGIC         partition by case
# MAGIC           when f.ORIGIN_AIRPORT < f.DESTINATION_AIRPORT then f.ORIGIN_AIRPORT
# MAGIC           else f.DESTINATION_AIRPORT
# MAGIC         end,
# MAGIC         case
# MAGIC           when f.ORIGIN_AIRPORT > f.DESTINATION_AIRPORT then f.ORIGIN_AIRPORT
# MAGIC           else f.DESTINATION_AIRPORT
# MAGIC         end
# MAGIC         order by
# MAGIC           f.ORIGIN_AIRPORT
# MAGIC       ) as rnum
# MAGIC     from
# MAGIC       flights as f
# MAGIC       join airlines as a on f.AIRLINE = a.IATA_CODE
# MAGIC   ) t
# MAGIC where
# MAGIC   rnum = 1

# COMMAND ----------

# DBTITLE 1,Write Reports to Snowflake
# Total number of flights by airline and airport on a monthly basis 
# On time percentage of each airline for the year 2015
# Airlines with the largest number of delays 
# Cancellation reasons by airport 
# Delay reasons by airport 
# Airline with the most unique routes 

# COMMAND ----------

# DBTITLE 1,Total number of flights by airline and airport on a monthly basis 
Total_number_of_flights_by_airline_and_airport_on_a_monthly_basis  ="""select
  a.AIRLINE,
  c.Airport,
  f.month,
  count(*) as number_of_flights
from
  flights as f
  join airlines as a on f.AIRLINE = a.IATA_CODE
  join airports as c on f.ORIGIN_AIRPORT = c.IATA_CODE
group by
  a.AIRLINE,
  c.Airport,
  f.month
order by
  number_of_flights"""
flights_monthly_basis = spark.sql(Total_number_of_flights_by_airline_and_airport_on_a_monthly_basis)
airlines_df.write.format("snowflake").options(**options).option("dbtable","Total_Flights_Monthly").mode("overwrite").save()

# COMMAND ----------

# DBTITLE 1,On time percentage of each airline for the year 2015 
On_time_percentage_airline ="""with query1 as (
  select
    a.AIRLINE,
    count(*) as on_time
  from
    flights as f
    join airlines as a on f.AIRLINE = a.IATA_CODE
  where
    f.year = 2015
    and (
      f.ARRIVAL_DELAY is null
      or f.ARRIVAL_DELAY = 0
    )
    and (
      f.DEPARTURE_DELAY is null
      or f.DEPARTURE_DELAY = 0
    )
    and (
      f.AIR_SYSTEM_DELAY is null
      or f.AIR_SYSTEM_DELAY = 0
    )
    and (
      f.SECURITY_DELAY is null
      or f.SECURITY_DELAY = 0
    )
    and (
      f.AIRLINE_DELAY is null
      or f.AIRLINE_DELAY = 0
    )
    and (
      f.LATE_AIRCRAFT_DELAY is null
      or f.LATE_AIRCRAFT_DELAY = 0
    )
    and (
      f.WEATHER_DELAY is null
      or f.WEATHER_DELAY = 0
    )group by
    a.airline
),
query2 as (
  select
    count(*) as total_flights,
    a.AIRLINE
  from
    flights as f
    join airlines as a on f.AIRLINE = a.IATA_CODE
  where
    CANCELLED = 0
  group by
    a.AIRLINE
)
select
  a.AIRLINE,
  a.on_time,
  b.total_flights,
  ((a.on_time / b.total_flights) * 100) as on_time_percentage
from
  query1 as a,
  query2 as b
where
  a.AIRLINE = b.AIRLINE"""
On_time_percentage = spark.sql(On_time_percentage_airline)
airlines_df.write.format("snowflake").options(**options).option("dbtable","On_time_airline_percentage").mode("overwrite").save()

# COMMAND ----------

# DBTITLE 1,Airlines with the largest number of delays 
Airlines_largest_number_delays  ="""select
  a.AIRLINE,
  count(*) as delays 
from
  flights as f
  join airlines as a on f.AIRLINE = a.IATA_CODE
where
  (
    f.ARRIVAL_DELAY is not null
    or f.ARRIVAL_DELAY != 0
  )
  or (
    f.DEPARTURE_DELAY is not null
    or f.DEPARTURE_DELAY != 0
  )
  or (
    f.AIR_SYSTEM_DELAY is not null
    or f.AIR_SYSTEM_DELAY != 0
  )
  or (
    f.SECURITY_DELAY is not null
    or f.SECURITY_DELAY != 0
  )
  or (
    f.AIRLINE_DELAY is not null
    or f.AIRLINE_DELAY != 0
  )
  or (
    f.LATE_AIRCRAFT_DELAY is null
    or f.LATE_AIRCRAFT_DELAY != 0
  )
  or (
    f.WEATHER_DELAY is not null
    or f.WEATHER_DELAY != 0
  )
group by
  a.airline
order by delays DESC"""
largest_number_delays = spark.sql(Airlines_largest_number_delays)
airlines_df.write.format("snowflake").options(**options).option("dbtable","Airlines_largest_number_delays").mode("overwrite").save()

# COMMAND ----------

# DBTITLE 1,Cancellation reasons by airport 
Cancellation_reasons_by_airport  ="""select 
  a.AIRPORT,
  f.CANCELLATION_REASON
from
  flights as f
  join airports as a on f.ORIGIN_AIRPORT = a.IATA_CODE
where  f.CANCELLATION_REASON  is not null
group by  f.CANCELLATION_REASON,a.AIRPORT"""
Cancellation_reasons = spark.sql(Cancellation_reasons_by_airport)
airlines_df.write.format("snowflake").options(**options).option("dbtable","Cancellation_reasons").mode("overwrite").save()

# COMMAND ----------

# DBTITLE 1,Delay reasons by airport 
Delay_reasons_by_airport  ="""with query1 as(
  select
    distinct a.AIRPORT,
    case
      when f.ARRIVAL_DELAY is not null then "ARRIVAL_DELAY"
    end as DELAY_REASON1,
    case
      when f.DEPARTURE_DELAY is not null then "DEPARTURE_DELAY"
    end as DELAY_REASON2,
    case
      when f.AIR_SYSTEM_DELAY is not null then "AIR_SYSTEM_DELAY"
    end as DELAY_REASON3,
    case
      when f.SECURITY_DELAY is not null then "SECURITY_DELAY"
    end as DELAY_REASON4,
    case
      when f.AIRLINE_DELAY is not null then "AIRLINE_DELAY"
    end as DELAY_REASON5,
    case
      when f.LATE_AIRCRAFT_DELAY is not null then "LATE_AIRCRAFT_DELAY"
    end as DELAY_REASON6,
    case
      when f.WEATHER_DELAY is not null then "WEATHER_DELAY"
    end as DELAY_REASON7
  from
    flights as f
    join airports as a on f.ORIGIN_AIRPORT = a.IATA_CODE
  where
    (
      f.ARRIVAL_DELAY is not null
      or f.ARRIVAL_DELAY != 0
    )
    or (
      f.DEPARTURE_DELAY is not null
      or f.DEPARTURE_DELAY != 0
    )
    or (
      f.AIR_SYSTEM_DELAY is not null
      or f.AIR_SYSTEM_DELAY != 0
    )
    or (
      f.SECURITY_DELAY is not null
      or f.SECURITY_DELAY != 0
    )
    or (
      f.AIRLINE_DELAY is not null
      or f.AIRLINE_DELAY != 0
    )
    or (
      f.LATE_AIRCRAFT_DELAY is null
      or f.LATE_AIRCRAFT_DELAY != 0
    )
    or (
      f.WEATHER_DELAY is not null
      or f.WEATHER_DELAY != 0
    )
),
query2 as (
  select
    f.AIRPORT,
    CONCAT(
      collect_set(f.DELAY_REASON1),
      collect_set(f.DELAY_REASON2),
      collect_set(f.DELAY_REASON3),
      collect_set(f.DELAY_REASON4),
      collect_set(f.DELAY_REASON5),
      collect_set(f.DELAY_REASON6),
      collect_set(f.DELAY_REASON7)
    ) as Delay_reasons
  from
    query1 as f
  group by
    f.AIRPORT
)
select
  *
from
  query2"""
Delay_reasons = spark.sql(Delay_reasons_by_airport)
airlines_df.write.format("snowflake").options(**options).option("dbtable","Delay_reasons").mode("overwrite").save()

# COMMAND ----------

# DBTITLE 1,Airline with the most unique routes 
Airline_with_the_most_unique_routes  ="""select
  AIRLINE,
  ORIGIN_AIRPORT,
  DESTINATION_AIRPORT
from
  (
    select
      a.AIRLINE,
      f.ORIGIN_AIRPORT,
      f.DESTINATION_AIRPORT,
      row_number() over(
        partition by case
          when f.ORIGIN_AIRPORT < f.DESTINATION_AIRPORT then f.ORIGIN_AIRPORT
          else f.DESTINATION_AIRPORT
        end,
        case
          when f.ORIGIN_AIRPORT > f.DESTINATION_AIRPORT then f.ORIGIN_AIRPORT
          else f.DESTINATION_AIRPORT
        end
        order by
          f.ORIGIN_AIRPORT
      ) as rnum
    from
      flights as f
      join airlines as a on f.AIRLINE = a.IATA_CODE
  ) t
where
  rnum = 1"""
unique_routes = spark.sql(Airline_with_the_most_unique_routes)
airlines_df.write.format("snowflake").options(**options).option("dbtable","Airline_most_unique_routes").mode("overwrite").save()
