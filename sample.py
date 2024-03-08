
def city_temp_analysis(spark,filepath1,column_name):
    df = spark.read.option("header", "false").option("inferschema", "true").csv(filepath1).toDF(*column_name)
    df.show()
    df_averag = df.groupBy("city").agg(avg("temperature").alias("avg_temperature"), sum("temperature").alias("total_temperature"),count("city").alias("num_measurement"))
    df_averag.show()

    df_averag.filter(df_averag["total_temperature"]>30.0).sort("city").show()


if __name__ == '__main__':
    spark:SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").enableHiveSupport().getOrCreate()
    filepath1 = df = "file:///home/takeo/pycharm_projects/DE43/citytemp.txt"
    column_name = ["city", "temperature"]
