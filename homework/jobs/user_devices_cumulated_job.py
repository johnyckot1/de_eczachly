import datetime
from pyspark.sql import SparkSession


def do_user_devices_transformation(spark, df_cumulated, df_events, df_devices, load_dt):
    # A cumulative query to generate device_activity_datelist from events  

    query =f"""

    with
    -- current date parameter
    cte_p_date as 
    (
    select date'{load_dt}' as dt    
    )
    -- data for previous day
    ,cte_yesterday as 
    (
        select 
        user_id,
        explode(device_activity_datelist) browser_activity,
        current_dt 
        from 
        user_devices_cumulated  
        cross join cte_p_date pd 
        where current_dt = date_add(pd.dt, -1)
    ),
    -- data for curent day
    cte_today as 
    (
        select
        e.user_id, 
        d.browser_type,
        to_date(e.event_time)	 as current_dt
        from events e
        join devices d on e.device_id  = d.device_id 
        cross join cte_p_date pd 
        where
        to_date(e.event_time)  = pd.dt
        and user_id is not null  -- filter out events with undefined user
        and e.device_id is not null  -- filter out events with undefined device
        group by
        e.user_id, 
        d.browser_type,
        to_date(e.event_time)
    )-- joined data at a user-browser_type level
    ,cte_result_unnested as 
    (
    select 
    coalesce(t.user_id , y.user_id) as user_id, 
    coalesce(t.current_dt, date_add(y.current_dt,1)) as current_dt,
    coalesce(t.browser_type,(y.browser_activity).browser_type) as browser_type,
    case
        when (y.browser_activity).activity_dates is null then array(t.current_dt)
        when t.current_dt is not null then array(t.current_dt) || (y.browser_activity).activity_dates
        else (y.browser_activity).activity_dates
    end as  activity_dates
    from cte_today t 
    full join cte_yesterday y 
        on t.user_id = y.user_id
        and t.browser_type = y.browser_activity.browser_type
    )
    --aggreate data on user and current-date level
    select 
    user_id,
    collect_list(
        struct(browser_type, activity_dates) ) as device_activity_datelist,
    current_dt 
    from cte_result_unnested        
    group by user_id, current_dt

    """
    df_cumulated.createOrReplaceTempView("user_devices_cumulated")
    df_events.createOrReplaceTempView("events")
    df_devices.createOrReplaceTempView("devices")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("user_devices_cumulated") \
      .getOrCreate()
    output_df = do_user_devices_transformation(spark
                                            ,spark.table("user_devices_cumulated") 
                                            ,spark.table("events")
                                            ,spark.table("devices")
                                            ,load_dt =  datetime.now().date())
    output_df.write.mode("overwrite").insertInto("user_devices_cumulated")




