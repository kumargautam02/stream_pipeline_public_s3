import re
import os
import logging
import datetime
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from general_functions import *
# import matplotlib.pyplot as plt
# import matplotlib.pyplot as pyplt
# from scipy.interpolate import make_interp_spline
# from scipy.ndimage import gaussian_filter1d

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger('Data_Processing')
logger.info('main.py Script started')


def apply_transformations(spark,destination_path):
    try:
        """
        This function is used to transform raw data received in Landing folder and save it in clean folder as parquet format, with partitionBy date.
        Parameters:
        destination_path: This is the path of clean folder where data is being saved. 
        spark: spark object
        returns: transformed pyspark-dataframe
        """
        logger.info('stream_processing_Script started')
        df = (spark.readStream.option("cleanSource","archive")
              .option("sourceArchiveDir", f"./runtime_log/archived/here/")
              .option("maxFilesPerTrigger", 1).format("json").load(f"./raw/perf_click_data-2-2024-05-10-00-03-03-b36c6ecd-9f5c-45fa-87ac-634f1c6a31ea.gz"))
        
        df = df.select('*', "ip_geo.*", "query.*").drop("query", "ip_geo")
        df = df.toDF(*get_unique_column_names(df.columns))

        df = df.drop(*get_duplicate_column_names(df))
        #removing all the numbers from column name ex: column_name_1 --> column_name
        df = df.toDF(*[column.rsplit("_",1)[0] for column in df.columns])

        df = df.withColumn("real_filepath", input_file_name())
        df = df.withColumn("click_time" ,col("click_time").cast("timestamp"))
        df = df.withColumn("actual_file" , split(df.real_filepath, '/',limit=-1))
        df = df.withColumn("count_file", size(df.actual_file))
        df = df.withColumn("actual_file" , df.actual_file[col("count_file")-1]).drop("count_file")
        df = df.withColumn("file_creation_date", get_file_generation_date_udf(col("actual_file")))
        df = df.withColumn("file_creation_date", to_timestamp("file_creation_date", "yyyy-MM-dd HH-mm"))
        publisher_id  = get_publisher_id_column_name(df)
        df = df.na.fill("null")
        df = df.withColumnRenamed(publisher_id, "publisher_id")
        df = df.select("publisher_id", "file_creation_date", "actual_file","click_time")
        df = df.withColumn("publisher_id", when(length(col("publisher_id")) > 6, regexp_extract(col("publisher_id"), "^(va-\d{3})|^(VA-\d{3})",0)).otherwise(col("publisher_id")))
        df = df.withWatermark("click_time", "10 minutes").groupBy(window("click_time", "10 minutes"),"publisher_id", "file_creation_date", "actual_file").agg(count("publisher_id").alias("total_clicks"))
        df = df.withColumn("date", split(col("file_creation_date"), " ").getItem(0))
        df = df.withColumn("date", to_timestamp("date", "yyyy-MM-dd"))
        df.writeStream.format("console").option("checkpointLocation", f"./runtime_log/checkpoint/").trigger(processingTime="30 seconds").outputMode("append").start().awaitTermination()

        return f"{destination_path}/output/"
    except Exception as e:
        logger.info(f"Error has been encountered at apply_transformations {e}")



try:
    if __name__ == "__main__":

        currect_working_directory = os.getcwd().replace("\\", "/")
        logger.info(f"current working directory: {currect_working_directory}")



        # spark = SparkSession.builder.master("local[*]").appName("stream_procecssing_pipeline_from_s3").config("spark.sql.legacy.timeParserPolicy","LEGACY").getOrCreate()
        spark = (SparkSession.builder.master("local[*]").appName("stream_procecssing_pipeline_from_s3")\
                 .config("spark.sql.legacy.timeParserPolicy","LEGACY")\
                    .config("spark.executor.memory", "4g")\
                        .config("spark.driver.memory", "4g")\
                            .config("spark.cores.max", "3")\
                             .config("spark.sql.streaming.schemaInference", True).getOrCreate())
        logger.info(f"SparkSession Created Successfully")
        spark.conf.set("spark.sql.streaming.schemaInference", True)

        
        logger.info(f"apply_transformations function started successfully reading data from location : /data/raw/")
        destination_path = apply_transformations(spark,f"{currect_working_directory}")
            
except Exception as e:
        logger.info(f"Error has been encountered at main {e}")
