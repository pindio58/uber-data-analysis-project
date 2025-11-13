from shared.utils.sparkUtils import get_spark_session, get_logger, read_raw_data, write_data
from shared.settings import appname, BUCKET_NAME

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

import argparse

spark = get_spark_session(appname=appname, use_minio=True)

def read_data(path:str)-> DataFrame:
    df:DataFrame = (
        spark.read.format('csv')
        .option('inferSchema',True)
        .option('header',True)
        .load(path=path)
    )
    return df

def standardize(df:DataFrame)-> DataFrame:    
    # rename columns
    columns_rename = {'Time (Local)': 'hour',
               'Eyeballs ': 'eyeballs',
               'Zeroes ': 'zeroes',
               'Completed Trips ':'completed_trips',
               'Requests ': 'requests',
               'Unique Drivers': 'drivers',
               'Date': 'date'}
    
    for old,new in columns_rename.items():
        df = df.withColumnRenamed(old, new)

    # Replace null dates with correct dates
    window = Window.orderBy(F.lit(1)).rowsBetween(Window.unboundedPreceding,
                                                  Window.currentRow)
    df = df.withColumn('date', F.last(('date'), ignorenulls=True).over(window=window))

    # Add new column
    df = df.withColumn(
        "timeLocalTs",
        F.to_timestamp(
            F.concat(
                F.to_date('date', "dd-MMM-yy"),
                F.lit(' '),
                F.format_string('%02d:00:00', F.col('hour').cast('int'))

            )
        )
    )

    return df


# 1. Which date had the most completed trips during the two-week period?
def q1(df:DataFrame)->DataFrame:
    
    date_with_most_completed_trips = (
        df.groupBy("date")
        .agg(F.sum(F.col("completed_trips")).alias('total_completed_trips'))
        .orderBy(F.desc(F.col('total_completed_trips')))
        .limit(1)
    )

    return date_with_most_completed_trips

# 2. What was the highest number of completed trips within a 24-hour period?
# instead of taking 24 hours from midnight to midnight, we willl take it from min date/hour available
def q2(df:DataFrame)->DataFrame:
    
    min_dt = df.agg(F.min('timeLocalTs')).collect()[0][0]
    offset_hour = min_dt.hour
    offset_str = f'{offset_hour} hours'

    window = F.window('timelocalTs' , '24 hours', startTime=offset_str) # see diff between sql.functions.window vs sql.window
    
    highest_number_of_completed_trips = (
        df.groupBy(window)
        .agg(F.sum('completed_trips').alias('completed_trips_per_24_hour'))
        .orderBy(F.desc(F.col('completed_trips_per_24_hour')))
        .limit(1)
    )
    return highest_number_of_completed_trips


# 3. Which hour of the day had the most requests during the two-week period?
def q3(df:DataFrame)->DataFrame:
    
    hour_of_the_day = (
        df.groupBy('hour')
        .agg(F.sum('requests').alias('requestsPerHour'))
        .orderBy(F.desc(F.col('requestsPerHour')))
        .limit(1)
    )
    
    return hour_of_the_day

# 4. What percentages of all zeroes during the two-week period occurred on weekends (Friday at 5 pm to Sunday at 3 am)?
def q4(df:DataFrame):
    
    weekend_zeroes_df=(
        
        df.filter(

            ((F.dayofweek(F.col("timeLocalTS"))==6) & (F.hour(F.col("timeLocalTS"))>=17) ) |
            ((F.dayofweek(F.col("timeLocalTS"))==7) ) |
            ((F.dayofweek(F.col("timeLocalTS"))==1) & (F.hour(F.col("timeLocalTS"))<=3) )

        ).agg(F.sum(F.col("zeroes")).alias('weekend_zeroes'))
    )

    Total_zeroes_df = (
        df.agg(F.sum(F.col("zeroes")).alias('Total_zeroes'))
    )

    percentage_df = (
        weekend_zeroes_df
        .join(Total_zeroes_df)
        .withColumn('percentage', (F.col('weekend_zeroes')/ F.col('Total_zeroes'))*100)
    )

    return percentage_df

# 5. What is the weighted average ratio of completed trips per driver during the two-week period?
def q5(df:DataFrame)-> DataFrame:
    completed_trips_per_hour_df = (
        df.groupBy('hour').
            agg(F.sum(F.col('completed_trips')).alias('completed_trips_per_hour'))

    )
    
    drivers_per_hour_df = (
        df.groupBy('hour')
            .agg(F.sum(F.col('drivers')).alias('drivers_per_hour'))
    )
    
    
    ratio_per_hour_df = (
        completed_trips_per_hour_df
        .join(drivers_per_hour_df,on='hour')
        .withColumn(
            'weight', 
            F.col('completed_trips_per_hour') / F.col('drivers_per_hour') 
        )
        .withColumn(
            'weighted_average_per_hour', 
            F.col('completed_trips_per_hour') * F.col('weight') )
    )

    result = (
        ratio_per_hour_df
        .select(
            (F.sum(F.col('weighted_average_per_hour')) / 
             F.sum(F.col('completed_trips_per_hour'))).alias('weighted_average'))
    )

    return result

# 6. In drafting a driver schedule in terms of 8 hours shifts, when are the busiest 8 consecutive hours over the two-week period in terms of unique requests? 
#    A new shift starts every 8 hours. Assume that a driver will work the same shift each day.
def q6(df:DataFrame)-> DataFrame:
    window = Window.orderBy('timeLocalTs').rowsBetween(Window.currentRow, 7)
    
    df_requests = (
        df
        .withColumn('requests_in_8h', F.sum('requests').over(window))
        .orderBy(F.desc(F.col('requests_in_8h')))
        .limit(1)
        .select(
            F.col('timeLocalTs').alias('start_time'), 
            F.col('timeLocalTs')+ F.expr('INTERVAL 8 HOURS').alias('end_time'),
            F.col('requests_in_8h')
        )
    )
    return df_requests
    
# 7. True or False: Driver supply always increases when demand increases during the two-week period.
def q7(df:DataFrame)->DataFrame:
    corr = df.stat.corr('requests','drivers')
    result = bool(corr>0.3)
    return df.sparkSession.createDataFrame([(result,)], ['result'])

# 8. In which 72-hour period is the ratio of Zeroes to Eyeballs the highest?
def q8(df:DataFrame)-> DataFrame:
    window = Window.orderBy("timeLocalTS").rowsBetween(-71, Window.currentRow)
    
    df_with_ratio = (
        df
        .withColumn("ratio",F.try_divide(F.col("zeroes") , F.col("eyeballs")))
        .withColumn('72HoursWindow',F.sum(F.col('ratio')).over(window))
    )

    top_row = (
        df_with_ratio
        .orderBy(F.desc(F.col('72HoursWindow')))
        .limit(1)

    )
    result = (
        
        top_row.select(
            (F.col('timeLocalTS') - F.expr('INTERVAL 72 HOURS')).alias('start_time'),
            F.col('timeLocalTS').alias('end_time')   
        )
    )

    return result

# 9. If you could add 5 drivers to any single hour of every day during the two-week period, which hour should you add them to? 
# Hint: Consider both rider eyeballs and driver supply when choosing.
def q9(df:DataFrame)-> DataFrame:
    
    unmet_demand_df = (
        df
        .withColumn("unmet_demand", F.col("requests") - F.col("drivers"))
        .withColumn("unmet_demand", F.greatest(F.col("unmet_demand"),F.lit(0)))
        .groupBy('hour').agg(F.sum(F.col('unmet_demand')).alias('unmet_demand_per_hour'))
        .orderBy(F.desc(F.col('unmet_demand_per_hour')))
        .limit(1)
    )    

    return unmet_demand_df

# 10. Looking at the data from all two weeks, which time might make the most sense to consider a true “end day” instead of midnight? 
# (i.e when are supply and demand at both their natural minimums)

def q10(df:DataFrame)-> DataFrame:
    
    result = (
        df.groupby("hour")
        .agg(
            F.avg("eyeballs").alias("avg_eyeballs"),
            F.avg("drivers").alias("avg_drivers"),
            F.avg("requests").alias("avg_requests")
        )
        .withColumn("activity_score", 
            F.col("avg_eyeballs") + F.col("avg_drivers") + F.col("avg_requests")
        )
        .orderBy(F.col("activity_score") )    # lowest activity = true end-of-day
        .limit(1)
        
    )
    
    return result
