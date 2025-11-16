import sys
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent

if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))


from shared.utils.sparkUtils import get_spark_session, get_logger, read_raw_data, write_data
from shared.settings import appname, BUCKET_NAME

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

import argparse

spark = get_spark_session(appname=appname, use_minio=True)

# initialize logger
logger = get_logger(appname)

def standardize(df:DataFrame)-> DataFrame:    
    # rename columns
    columns_rename = {
        'Time (Local)': 'hour',
        'Eyeballs ': 'eyeballs',
        'Zeroes ': 'zeroes',
        'Completed Trips ':'completed_trips',
        'Requests ': 'requests',
        'Unique Drivers': 'drivers',
        'Date': 'date'
    }
    
    logger.info("standardize: renaming columns")
    for old,new in columns_rename.items():
        df = df.withColumnRenamed(old, new)

    # Replace null dates with correct dates
    window = Window.orderBy(F.lit(1)).rowsBetween(Window.unboundedPreceding,
                                                  Window.currentRow)
    df = df.withColumn('date', F.last(('date'), ignorenulls=True).over(window=window))

    # Add new column
    df = df.withColumn(
        "timeLocalTS",
        F.to_timestamp(
            F.concat(
                F.to_date('date', "dd-MMM-yy"),
                F.lit(' '),
                F.format_string('%02d:00:00', F.col('hour').cast('int'))

            )
        )
    )

    logger.debug("standardize: schema=%s", df.schema.simpleString())
    logger.info("standardize: completed")
    return df


# 1. Which date had the most completed trips during the two-week period?
def q1(df:DataFrame)->DataFrame:
    logger.info("q1: computing date with most completed trips")
    
    date_with_most_completed_trips = (
        df.groupBy("date")
        .agg(F.sum(F.col("completed_trips")).alias('total_completed_trips'))
        .orderBy(F.desc(F.col('total_completed_trips')))
        .limit(1)
    )

    # return date_with_most_completed_trips
    write_data(
        df=date_with_most_completed_trips,
        bucket_name=BUCKET_NAME,
        folder='question1'
    )
    logger.info("q1: written to %s/question1", BUCKET_NAME)


# 2. What was the highest number of completed trips within a 24-hour period?
# instead of taking 24 hours from midnight to midnight, we willl take it from min date/hour available
def q2(df:DataFrame)->DataFrame:
    logger.info("q2: computing highest number of completed trips in any 24-hour window")

    min_dt = df.agg(F.min('timeLocalTS')).collect()[0][0]
    offset_hour = min_dt.hour
    offset_str = f'{offset_hour} hours'

    window = F.window('timeLocalTS' , '24 hours', startTime=offset_str) # see diff between sql.functions.window vs sql.window
    
    highest_number_of_completed_trips = (
        df.groupBy(window)
        .agg(F.sum('completed_trips').alias('completed_trips_per_24_hour'))
        .orderBy(F.desc(F.col('completed_trips_per_24_hour')))
        .limit(1)
    )
    write_data(
        df=highest_number_of_completed_trips,
        bucket_name=BUCKET_NAME,
        folder='question2'
    )
    logger.info("q2: written to %s/question2", BUCKET_NAME)


# 3. Which hour of the day had the most requests during the two-week period?
def q3(df:DataFrame)->DataFrame:
    logger.info("q3: computing hour with most requests")

    hour_of_the_day = (
        df.groupBy('hour')
        .agg(F.sum('requests').alias('requestsPerHour'))
        .orderBy(F.desc(F.col('requestsPerHour')))
        .limit(1)
    )
    
    write_data(
        df=hour_of_the_day,
        bucket_name=BUCKET_NAME,
        folder='question3'
    )
    logger.info("q3: written to %s/question3", BUCKET_NAME)

# 4. What percentages of all zeroes during the two-week period occurred on weekends (Friday at 5 pm to Sunday at 3 am)?
def q4(df:DataFrame):
    logger.info("q4: computing weekend zeroes percentage")

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

    write_data(
        df=percentage_df,
        bucket_name=BUCKET_NAME,
        folder='question4'
    )
    logger.info("q4: written to %s/question4", BUCKET_NAME)

# 5. What is the weighted average ratio of completed trips per driver during the two-week period?
def q5(df:DataFrame)-> DataFrame:
    logger.info("q5: computing weighted average ratio of completed trips per driver")
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

    write_data(
        df=result,
        bucket_name=BUCKET_NAME,
        folder='question5'
    )
    logger.info("q5: written to %s/question5", BUCKET_NAME)

# 6. In drafting a driver schedule in terms of 8 hours shifts, when are the busiest 8 consecutive hours over the two-week period in terms of unique requests? 
#    A new shift starts every 8 hours. Assume that a driver will work the same shift each day.
def q6(df:DataFrame)-> DataFrame:
    logger.info("q6: computing busiest 8 consecutive hours")
    window = Window.orderBy('timeLocalTS').rowsBetween(Window.currentRow, 7)
    
    df_requests = (
        df
        .withColumn('requests_in_8h', F.sum('requests').over(window))
        .orderBy(F.desc(F.col('requests_in_8h')))
        .limit(1)
        .select(
            F.col('timeLocalTS').alias('start_time'), 
            F.col('timeLocalTS')+ F.expr('INTERVAL 8 HOURS').alias('end_time'),
            F.col('requests_in_8h')
        )
    )
    write_data(
        df=df_requests,
        bucket_name=BUCKET_NAME,
        folder='question6'
    )
    logger.info("q6: written to %s/question6", BUCKET_NAME)
    
# 7. True or False: Driver supply always increases when demand increases during the two-week period.
def q7(df:DataFrame)->DataFrame:
    logger.info("q7: computing correlation between requests and drivers")
    corr = df.stat.corr('requests','drivers')
    logger.info("q7: correlation value=%s", corr)
    answer = bool(corr>0.3)
    result = df.sparkSession.createDataFrame([(answer,)], ['answer'])
    write_data(
        df=result,
        bucket_name=BUCKET_NAME,
        folder='question7'
    )
    logger.info("q7: written to %s/question7", BUCKET_NAME)

# 8. In which 72-hour period is the ratio of Zeroes to Eyeballs the highest?
def q8(df:DataFrame)-> DataFrame:
    logger.info("q8: computing 72-hour window with highest zeroes/eyeballs ratio")
    window = Window.orderBy("timeLocalTS").rowsBetween(-71, Window.currentRow)
    
    df_with_ratio = (
        df
        # .withColumn("ratio",F.try_divide(F.col("zeroes") , F.col("eyeballs")))  only works in 3.5
        .withColumn("ratio", 
                    F.when(F.col("eyeballs") != 0, F.col("zeroes") / F.col("eyeballs"))
                    .otherwise(None))
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

    write_data(
        df=result,
        bucket_name=BUCKET_NAME,
        folder='question8'
    )
    logger.info("q8: written to %s/question8", BUCKET_NAME)

# 9. If you could add 5 drivers to any single hour of every day during the two-week period, which hour should you add them to? 
# Hint: Consider both rider eyeballs and driver supply when choosing.
def q9(df:DataFrame)-> DataFrame:
    logger.info("q9: computing hour to add 5 drivers")
    unmet_demand_df = (
        df
        .withColumn("unmet_demand", F.col("requests") - F.col("drivers"))
        .withColumn("unmet_demand", F.greatest(F.col("unmet_demand"),F.lit(0)))
        .groupBy('hour').agg(F.sum(F.col('unmet_demand')).alias('unmet_demand_per_hour'))
        .orderBy(F.desc(F.col('unmet_demand_per_hour')))
        .limit(1)
    )    

    write_data(
        df=unmet_demand_df,
        bucket_name=BUCKET_NAME,
        folder='question9'
    )
    logger.info("q9: written to %s/question9", BUCKET_NAME)

# 10. Looking at the data from all two weeks, which time might make the most sense to consider a true “end day” instead of midnight? 
# (i.e when are supply and demand at both their natural minimums)

def q10(df:DataFrame)-> DataFrame:
    logger.info("q10: computing suggested end-of-day hour")
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
    
    write_data(
        df=result,
        bucket_name=BUCKET_NAME,
        folder='question10'
    )
    logger.info("q10: written to %s/question10", BUCKET_NAME)

def main(question_id:str | None, path:str|None = None):
    logger.info("main: starting processing path=%s question_id=%s", path, question_id)
    df = read_raw_data(spark=spark, source_file=path)
    df = standardize(df)
    
    questions = {
        1:q1,
        2:q2,
        3:q3,
        4:q4,
        5:q5,
        6:q6,
        7:q7,
        8:q8,
        9:q9,
        10:q10

    }

    if not question_id:
        questions_list = list(questions.keys())
    else:
        questions_list=sorted(
            set(int(x.strip()) for x in question_id.split(','))
        )

    for question in questions_list:
        logger.info("main: running question %s", question)
        questions[question](df)
        logger.info("main: completed question %s", question)

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--question-id',default=None)
    parser.add_argument('--path',type=str,required=True)
    args = parser.parse_args()

    main(args.question_id,args.path)

    logger.info("main: finished processing; stopping spark")
    spark.stop()