import org.apache.spark.mllib.linalg.Vector 
import org.apache.spark.mllib.regression.LabeledPoint

// load the data
val rawData = sc.textFile("purple_air_data_final_190806_1220.csv")

// split the csv
val splitData = rawData.map(line => line.split(","))

def get_feature(input_map: Map[String, Any], feature: Array[String]): double = {
    val map_value = input_map.get(feature)
    val output = (map_value: @switch) match {
        case " "  => 0
        case ""  => 0
        case _  => map_value.toDouble
    }
    return output
}

def get_feature_space(input_map: Map[String, Any], feature_mapping: Array[String]): Array[double] = {
    return for (feature <- feature_mapping) yield get_feature(input_map, feature)
}

def create_data_mapping(input_arr: Array[Any]): Map[String, Any] = {
    return Map(
        "0_3um_dl" -> input_arr(1),
        "0_5um_dl" -> input_arr(2),
        "1_0um_dl" -> input_arr(3),
        "10_0um_dl" -> input_arr(4),
        "2_5um_dl" -> input_arr(5),
        "5_0um_dl" -> input_arr(6),
        "ADC" -> input_arr(7),
        "Humidity_perc" -> input_arr(8),
        "PM1_0_CF_1_ug_m3" -> input_arr(9),
        "PM1_0_CF_ATM_ug_m3" -> input_arr(10),
        "PM10_0_CF_ATM_ug_m3" -> input_arr(11),
        "PM10_CF_1_ug_m3" -> input_arr(12),
        "PM2_5_CF_1_ug_m3" -> input_arr(13),
        "PM2_5_CF_ATM_ug_m3" -> input_arr(14),
        "Pressure_hpa" -> input_arr(15),
        "RSSI_dbm" -> input_arr(15),
        "Temperature_F" -> input_arr(16),
        "Unnamed: 10" -> input_arr(18),
        "UptimeMinutes" -> input_arr(19),
        "county" -> input_arr(20),
        "created_at" -> input_arr(21),
        "end_period" -> input_arr(22),
        "entry_id" -> input_arr(23),
        "lat" -> input_arr(24),
        "location" -> input_arr(25),
        "long" -> input_arr(26),
        "start_period" -> input_arr(27),
        "type" -> input_arr(28),
        "zipcode" -> input_arr(29),
        "date" -> input_arr(30),
        "hour" -> input_arr(31),
        "RSSI_dbm" -> input_arr(32)
    )
}

// define a order for the feature_space
val features = ["Temperature_F", "hour", "Pressure_hpa", "Humidity_perc", "5_0um_dl", "10_0um_dl"]
val label = "0_5um_dl"

// create a array of mapped data
val mappedData = splitData.map(line => create_data_mapping(line))

//create an array of labelled points (label (int), Array[double])
val labelledPoints = data.map{line => LabeledPoint(line(label).toDouble, get_feature_space(line))}

// train the data
val numIterations = 20
val model = SVMWithSGD.train(parsedData, numIterations)

// predict on a new set of data
val valuesAndPreds = data.map{ point =>
  val prediction = model.predict(point.features)
  (prediction, point.label)
}

// Instantiate metrics object
val metrics = new RegressionMetrics(valuesAndPreds)


import math._
import org.apache.spark.sql.functions._
import spark.implicits._


def get_mean_for_column(data: DataFrame, desired_col: String): DataFrame = {
    return data.select(Seq(desired_col).map(mean(_)): _*).show()
}

def calculate_rmse(trues: Array[Double], preds: Array[Double]): Double = {
    // get list of tuples
    val list_tup = trues.zip(preds)
    // get list of tuples
    val list_errors = for (local_tup <- list_tup) yield math.pow((local_tup._1-local_tup._2), 2)
    // get sum of squared errors
    val sum_of_sqr = list_errors.sum
    // reutrn sqrt of the 
    return math.sqrt(sum_of_sqr / trues.size)
}


def isna(value: Double, limit: Double, default: Double): Double = {
    value match {
      case value if (value < 0) => return default
      case value if (value > limit) => return default
      case _ => return value
    }
}

def impute_na(values: Array[Double], limit: Double, default: Double): Array[Double] = {
    val list_tup = for (value <- values) yield isna(value, limit,default)
    return list_tup
}


def get_confidence(avg_error: Double, pred_i: Double, target_i: Double): Double = {
    val this_confidence = 1 / (math.abs(target_i - pred_i)/ avg_error)
    if (this_confidence > 1) {
        return 1
    }
    else {
        return this_confidence
    }
}




