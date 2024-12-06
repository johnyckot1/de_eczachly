from datetime import date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, DateType, LongType
from chispa.dataframe_comparer import *
from ..jobs.user_devices_cumulated_job import do_user_devices_transformation


def test_user_device_cumulated(spark):
    
    test_data_folder = '/home/azureuser/data-engineer-handbook/hw3/homework/tests/data/'
    load_dt = '2021-01-02'

    # Define the schema for the nested struct
    nested_struct_schema = StructType([
        StructField("browser_type", StringType(), True),
        StructField("activity_dates", ArrayType(DateType()), True)    
    ])
    # Define schema for device_cumulated table
    schema =  StructType([
        StructField("user_id", StringType(), True),
        StructField("device_activity_datelist", ArrayType(nested_struct_schema), True),
        StructField("current_dt", DateType(), True)
    ])

    # Generate device_cumulated entries for yesterday
    data_cumulative_in = [
        (210988258, [('PetalBot',[date(2021,1,1)])], date(2021,1,1))
        ,(1746646422, [('tst_bot',[date(2021,1,1)])], date(2021,1,1))
    ]

    #  Generate expected output for device_cumulated
    data_cumulative_out = [
        (210988258, [('PetalBot',[date(2021,1,2),date(2021,1,1)])], date(2021,1,2))
        ,(273700037, [('bingbot',[date(2021,1,2)])], date(2021,1,2))
        ,(1746646422, [('Chrome',[date(2021,1,2)]) , ('tst_bot',[date(2021,1,1)]) ], date(2021,1,2))
    ]

    
    df_cumulated = spark.createDataFrame(data_cumulative_in, schema)    
    # Read input data for Events
    df_events = spark.read.csv(f'{test_data_folder}test_user_device_cumulated_events_in.csv',header = True)
    # Read input data for Devices
    df_devices = spark.read.csv(f'{test_data_folder}test_user_device_cumulated_devices_in.csv', header = True)

    # Run cumulative query
    df_out = do_user_devices_transformation(spark , df_cumulated,  df_events ,df_devices , load_dt )

    # Excpeted result
    df_expected = spark.createDataFrame(data_cumulative_out, schema)     
    
    assert_df_equality(df_out, df_expected, ignore_nullable=True)

    