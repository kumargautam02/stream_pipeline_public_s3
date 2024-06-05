# import Processing.logger_configuration.logger_config
import os
from Processing.logger_config import *
from Processing.stream_processing_script import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def ingest_data_from_s3(current_working, s3, s3_path):
    try:
        """
        This function is used to Ingest data from Public S3 bucket site using s3fs library and store inside Landing Folder of Current working Directory. 
        Parameters:
        current_working: receive the current working directory to save data ex:- current_working_directory/Landing
        s3: S3 object for Public s3_bucket.
        s3_path: s3 bucket path from which we have to ingest data. 

        returns: None
        """

        # current_working = os.getcwd()
        # s3.get(f'{s3_path}', f'{current_working}/Landing1/',recursive=True, maxdepth=None)
        return f'{current_working}/Landing1/'
    except Exception as e:
        logger.info(f"Error has been encountered at ingest_data_from_s3 {e}")


def get_file_generation_date(column):
    """
    This function is used to extract the date_column from file_name.
    Parameters:
    column[pyspark dataframe column]: column with the file_name.

    returns: the date of file generation.
    """
    try:
        pattern_for_date = "(2024-05-\d{2})"
        pattern_for_whole = "(2024-05-\d{2}-\d{2}-\d{2}-\d{2})"
        date_value = re.search(pattern_for_date, column).group(0)
        whole_value = re.search(pattern_for_whole, column).group(0)
        time_value = whole_value.replace(f'{date_value}-', "")

        return f"{date_value} {time_value}"
    except Exception as e:
        logger.info(f"Error has been encountered at get_file_generation_date {e}")



get_file_generation_date_udf  = udf(lambda column: get_file_generation_date(column),StringType())


def get_unique_column_names(column_names):
    """
    This function is used to give unique names to spark columns based on index of the columns.
    Parameters:
    column[python list]: Python list with the column_names

    returns: [python list with unique names]
    """
    try:
        # print(column_names)
        for i in range(len(column_names)):
            if column_names[i].strip() != "file_creation_date":
                column_names[i] = column_names[i] + f"_{i}" 

        return column_names
    except Exception as e:
        logger.info(f"Error has been encountered at get_unique_column_names {e}")
    

def get_duplicate_column_names(df):
    """
    This function is used to get the duplciate columns from the dataframe.
    Parameters:
    df[pyspark dataframe column]: pyspark dataframe

    returns: [Python list] list of duplicate column names present in the dataframe.
    """
    try:
        duplicate_columns = []
        original_columns = []
        for column in df.columns:
            # print(column)
            if column.rsplit("_", 1)[0] not in original_columns:
                original_columns.append(column.rsplit("_", 1)[0])
            else:
                duplicate_columns.append(column)
        return duplicate_columns
    except Exception as e:
        logger.info(f"Error has been encountered at get_duplicate_column_names {e}")


def get_publisher_id_column_name(df):
    """
    This function is used to get the publisher_id column name.
    Parameters:
    column[pyspark dataframe column]: dataframe.

    returns: [python string] column name of publisher_id
    """
    try:
        # print("yes")
        for column in df.columns:
            # print(column)
            if column.rsplit("_", 1)[0] == 'publisher_id':
                # print(column)
                
                return column
    except Exception as e:
        logger.info(f"Error has been encountered at get_publisher_id_column_name {e}")




def apply_transformations(spark,destination_path):
    try:
        """
        This function is used to transform raw data received in Landing folder and save it in clean folder as parquet format, with partitionBy date.
        Parameters:
        destination_path: This is the path of clean folder where data is being saved. 
        spark: spark object
        returns: transformed pyspark-dataframe
        """
        logger = get_logger()
        logger.info('stream_processing_Script started')
        df = (spark.readStream.option("cleanSource","archive")
              .option("sourceArchiveDir", f"{destination_path}/Files_done/here/")
              .option("maxFilesPerTrigger", 1).format("json").load(f"{destination_path}/landing"))
        # df.printSchema()
        df = df.select('*', "ip_geo.*", "query.*").drop("query", "ip_geo")
        df = df.toDF(*get_unique_column_names(df.columns))

        df = df.drop(*get_duplicate_column_names(df))
        df = df.toDF(*[column.rsplit("_",1)[0] for column in df.columns])

        df = df.withColumn("real_filepath", input_file_name())
        # df.printSchema()
        df = df.withColumn("click_time" ,col("click_time").cast("timestamp"))
        # df.printSchema()
        # df.show()
        df = df.withColumn("actual_file" , split(df.real_filepath, '/',limit=-1))
        df = df.withColumn("count_file", size(df.actual_file))
        df = df.withColumn("actual_file" , df.actual_file[col("count_file")-1]).drop("count_file")
        df = df.withColumn("file_creation_date", get_file_generation_date_udf(col("actual_file")))
        df = df.withColumn("file_creation_date", to_timestamp("file_creation_date", "yyyy-MM-dd HH-mm"))
        publisher_id  = get_publisher_id_column_name(df)
        df = df.na.fill("null")
        # df.printSchema()
        # # print("this is the column structure", df.columns)
        df = df.withColumnRenamed(publisher_id, "publisher_id")
        df = df.select("publisher_id", "file_creation_date", "actual_file","click_time")
        df = df.withColumn("publisher_id", when(length(col("publisher_id")) > 6, regexp_extract(col("publisher_id"), "^(va-\d{3})|^(VA-\d{3})",0)).otherwise(col("publisher_id")))

        # df.printSchema()
        df = df.withWatermark("click_time", "10 minutes").groupBy(window("click_time", "10 minutes"),"publisher_id", "file_creation_date", "actual_file").agg(count("publisher_id").alias("total_clicks"))
        # print("this is the window function count", df.count())
        df = df.withColumn("date", split(col("file_creation_date"), " ").getItem(0))
        df = df.withColumn("date", to_timestamp("date", "yyyy-MM-dd"))
        # df = df.withColumn("path", lit(path))
        # df.printSchema()

        # df.write.partitionBy("date").mode("append").format("parquet").save(str(os.getcwd()).replace("\\", "/")+f'/clean1')
        # df.writeStream.format("console").trigger(processingTime="30 seconds").start().awaitTermination()
        df.writeStream.format("console").option("checkpointLocation", f"{destination_path}/checkpoint/").trigger(processingTime="30 seconds").outputMode("append").start().awaitTermination()
        # .option("path", f"{destination_path}/output/")
        # .option("path", f"{os.getcwd()}/output/")
        return f"{destination_path}/output/"
    except Exception as e:
        logger.info(f"Error has been encountered at apply_transformations {e}")










try:
    if __name__ == "__main__":
        logger = get_logger()

        currect_working_directory = os.getcwd().replace("\\", "/")
        logger.info(f"current working directory: {currect_working_directory}")



        spark = SparkSession.builder.master("local[*]").appName("stream_procecssing_pipeline_from_s3").config("spark.sql.legacy.timeParserPolicy","LEGACY").getOrCreate()
        logger.info(f"SparkSession Created Successfully")
        spark.conf.set("spark.sql.streaming.schemaInference", True)

        
        logger.info(f"apply_transformations function started successfully reading data from location : {currect_working_directory}/landing")
        destination_path = apply_transformations(spark,f"{currect_working_directory}")

        # logger.info(f"apply_transformations function completed saved parquet at location: {destination_path}")
        # df = generating_publisher_id_with_maximum_queries(spark, destination_path)
        # logger.info("generating_publisher_id_with_maximum_queries function runned successfully")
        # df.coalesce(1).write.mode("overwrite").csv("top_5_publishers_id_data")
        # logger.info("top-5 publishers_id saved in csv file")
        # generatring_line_graph_for_top_5_publishers(df, os.getcwd())
        # logger.info(f"generatring_line_graph_for_top_5_publishers function completed saved parquet at location: {destination_path}")
            
except Exception as e:
        logger.info(f"Error has been encountered at main {e}")