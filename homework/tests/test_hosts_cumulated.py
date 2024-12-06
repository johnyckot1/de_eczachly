from datetime import date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, DateType, LongType
from chispa.dataframe_comparer import *
from ..jobs.hosts_cumulated import do_host_transformation


def test_hosts_cumulated(spark):
    
    test_data_folder = '/home/azureuser/data-engineer-handbook/hw3/homework/tests/data/'
    load_dt = '2021-01-02'

    schema =  StructType([
     StructField("host", StringType(), True),
     StructField("host_activity_datelist", ArrayType(DateType()), True),
     StructField("current_dt", DateType(), True)
    ])

    data_cumulative_in = [
        ('www.eczachly.com', [date(2021,1,1)], date(2021,1,1))    
    ]

    data_cumulative_out = [
        ('admin.zachwilson.tech', [date(2021,1,2)], date(2021,1,2)),
        ('www.eczachly.com', [date(2021,1,2), date(2021,1,1)], date(2021,1,2)),
        ('www.zachwilson.tech', [date(2021,1,2)], date(2021,1,2))
    ]

    
    df_cumulated = spark.createDataFrame(data_cumulative_in, schema)    
    # Read input data for Events
    df_events = spark.read.csv(f'{test_data_folder}test_hosts_cumulated_events_in.csv',header = True)    

    # Run cumulative query
    df_out = do_host_transformation(spark , df_cumulated,  df_events, load_dt )

    # Excpeted result
    df_expected = spark.createDataFrame(data_cumulative_out, schema)     
    
    assert_df_equality(df_out, df_expected, ignore_nullable=True)

    