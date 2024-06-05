

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
        df = (spark.readStream.option("cleanSource","archive").option("sourceArchiveDir", f"{destination_path}/archived/").option("maxFilesPerTrigger", 1).format("json").load(f"{destination_path}/landing"))
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
        return f"{destination_path}/output/"
    except Exception as e:
        logger.info(f"Error has been encountered at apply_transformations {e}")
