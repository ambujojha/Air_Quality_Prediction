import spark.implicits._
val path = "projectData/californiaJanFeb2019.json"

val df = spark.read.option("multiLine", true).option("mode", "PERMISSIVE").json(path)

val df = spark.read.json(path)

df.printSchema()
/*
root
 |-- cbsa_code: string (nullable = true)
 |-- county: string (nullable = true)
 |-- county_code: string (nullable = true)
 |-- date_gmt: string (nullable = true)
 |-- date_local: string (nullable = true)
 |-- date_of_last_change: string (nullable = true)
 |-- datum: string (nullable = true)
 |-- detection_limit: double (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
 |-- method: string (nullable = true)
 |-- method_code: string (nullable = true)
 |-- method_type: string (nullable = true)
 |-- parameter: string (nullable = true)
 |-- parameter_code: string (nullable = true)
 |-- poc: long (nullable = true)
 |-- qualifier: string (nullable = true)
 |-- sample_duration: string (nullable = true)
 |-- sample_frequency: string (nullable = true)
 |-- sample_measurement: double (nullable = true)
 |-- site_number: string (nullable = true)
 |-- state: string (nullable = true)
 |-- state_code: string (nullable = true)
 |-- time_gmt: string (nullable = true)
 |-- time_local: string (nullable = true)
 |-- uncertainty: string (nullable = true)
 |-- units_of_measure: string (nullable = true)
*/


df.count
//res1: Long = 95168                                                              


//Check for null values in sample_measurement
df.filter($"sample_measurement".isNotNull).count()
//res2: Long = 91447


//Remove null values
val dfNotNull = df.filter($"sample_measurement".isNotNull)

dfNotNull.count()
//res3: Long = 91447

//Check negative values
dfNotNull.filter($"sample_measurement" < 0).count()
//res4: Long = 5091

//Remove negative values
val dfFinal = dfNotNull.filter($"sample_measurement" >= 0)

dfFinal.count()
//res5: Long = 86356


//Save as csv file
dfFinal.write.option("header", "true").format("csv").save("projectData/finalcsv")


val relevantData = dfFinal.select("latitude", "longitude", "date_gmt", "date_local", "time_gmt", "time_local", "sample_duration", "sample_frequency", "sample_measurement")

val df = spark.read.format("csv").option("header", "true").load("projectData/langLongZip.csv")

val temp = relevantData.join(df, "latitude")

val relDataZip = temp.select("latitude", "longitude", " zipcode", "county", "county_code", "date_gmt", "date_local", "time_gmt", "time_local", "sample_duration", "sample_frequency", "sample_measurement")

val finalDataZip = relDataZip.withColumnRenamed(" zipcode", "zipcode")

val sortData = finalDataZip.sort("date_gmt", "time_gmt")

val weather = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs:///user/dx374/BDAD_Shared/Weather2019.csv")



//Profile values
val selectedColumnName = dfFinal.columns(0)
dfFinal.agg(min(selectedColumnName), max(selectedColumnName)).show()

val selectedColumnName = dfNotNull.columns(19)
dfFinal.agg(min(selectedColumnName), max(selectedColumnName)).show()




