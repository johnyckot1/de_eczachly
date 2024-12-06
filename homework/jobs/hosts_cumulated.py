import datetime
from pyspark.sql import SparkSession


def do_host_transformation(spark, df_cumulated, df_events, load_dt):
    # A cumulative query to generate device_activity_datelist from events  

    query =f"""

    with 
    -- current date parameter
    cte_p_date as 
    (
    select date'{load_dt}' as dt
    )
    --data from previous day (cumulated)
    ,yesterday as (
    select 
        host,
        host_activity_datelist,
        current_dt
    from hosts_cumulated h 
    cross join cte_p_date p
    where current_dt = p.dt  - interval '1 day'
    )
    --data from current day
    , today as (
        select 
        host,
        to_date(event_time) as current_dt
        from  
        events
        cross join  cte_p_date p
        where host is not null 
        and to_date(event_time) =  p.dt
        group by host , to_date(event_time)
    )
    --joined data
    select 
    coalesce(t.host, y.host) as host, 
    (case 
        when y.host_activity_datelist is null then array(t.current_dt)
        when t.current_dt is null then  y.host_activity_datelist
        else array_union(array(t.current_dt),y.host_activity_datelist)
    end ) as host_activity_datelist,
    to_date(coalesce(t.current_dt, y.current_dt + interval '1 day')) as current_dt
    from today t 
    full join yesterday y on t.host = y.host

    """
    df_cumulated.createOrReplaceTempView("hosts_cumulated")
    df_events.createOrReplaceTempView("events")    
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("hosts_cumulated") \
      .getOrCreate()
    output_df = do_host_transformation(spark
                                            ,spark.table("do_host_transformation") 
                                            ,spark.table("events")                                            
                                            ,load_dt =  datetime.now().date())
    output_df.write.mode("overwrite").insertInto("do_host_transformation")




