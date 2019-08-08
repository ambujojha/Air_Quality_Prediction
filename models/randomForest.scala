import spark.implicits._

val path = "projectData/PM2.5*"

val df = spark.read.json(path)

val dfNotNull = df.filter($"sample_measurement".isNotNull)

val dfFinal = dfNotNull.filter($"sample_measurement" >= 0)

dfFinal.write.option("header", "true").format("csv").save("projectData/finalcsv")

val relevantData = dfFinal.select("latitude", "longitude", "date_gmt", "date_local", "time_gmt", "time_local", "sample_duration", "sample_frequency", "sample_measurement")


