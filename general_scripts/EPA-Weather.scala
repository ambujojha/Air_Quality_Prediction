val lanlong = spark.read.format("csv").option("header", "true").option("inferSchema", "false").load("hdfs:///user/dx374/BDAD_Shared/langLongZip.csv")

val EPARaw = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/dx374/BDAD_Shared/finacsv/*.csv")

val lanlongRenamed = lanlong.withColumnRenamed(" longitude", "longitude").withColumnRenamed(" zipcode", "zipcode")

val EPAjoined = EPARaw.join(lanlongRenamed, "longitude")


val weather = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/dx374/BDAD_Shared/Weather2019.csv")


val EPAfiltered = EPAjoined.filter(col("date_local") >= "2019-01-01 00:00:00")

val newkeyEPA = EPAfiltered.withColumn("newkey", concat(substring(col("zipcode"), 2, 7), lit(","), substring(col("date_local"), 0, 10), lit(" "), substring(col("time_local"), 0, 2))).drop("ZIPCODE")
val newkeyweather = weather.withColumn("newkey", concat(col("ZIPCODE"), lit(","), substring(col("DATE"), 0, 13)))

val weatherEPA = newkeyweather.join(newkeyEPA, "newkey")

weatherEPA.agg(countDistinct("ZIPCODE")).show

val finalWeatherEPA = weatherEPA.select("HourlyDryBulbTemperature", "HourlyPrecipitation","HourlyRelativeHumidity", "HourlyStationPressure", "HourlyWindSpeed" ,"ZIPCODE", "county", "county_code", "date_local", "sample_duration", "sample_frequency", "sample_measurement","time_local")
 


