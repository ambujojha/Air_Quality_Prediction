val AllData = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/dx374/BDAD/project/test/data/*.csv")

val Dropped = AllData.select("STATION", "NAME", "LATITUDE", "LONGITUDE",  "DATE","HourlyDryBulbTemperature", "HourlyPrecipitation","REPORT_TYPE6", "HourlyRelativeHumidity", "HourlyStationPressure", "HourlyWindSpeed")

val ChangedType = Dropped.withColumn("STATION", Dropped("STATION").cast("long")).withColumn("LATITUDE", Dropped("LATITUDE").cast("double")).withColumn("LONGITUDE", Dropped("LONGITUDE").cast("double"))

val FilteredData = ChangedType.filter(substring(col("REPORT_TYPE6"), 0, 2).isin("FM"))

FilteredData.select("STATION").distinct.count

FilteredData.agg(max("STATION"),min("STATION"),max("LATITUDE"),min("LATITUDE"),max("LONGITUDE"),min("LONGITUDE"),max("DATE"),min("DATE"))

val LocData = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/dx374/BDAD/project/AddLocation.csv")

val Combined = FilteredData.join(LocData, "STATION")

Combined.select(Combined.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)):_*).show

Combined.coalesce(1).write.option("header", "true").format("csv").save("hdfs:///user/dx374/BDAD/project/test/output/2016-19.csv")




