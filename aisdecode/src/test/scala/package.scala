package uk.gov.dft.ais.decode.test

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.scalatest._
import uk.gov.dft.ais.decode.decode123.transform
import uk.gov.dft.ais.decode.test.utils.{prepareQaData, renameSelectMap}



class TestDecode123 extends FunSuite with BeforeAndAfter with DataFrameSuiteBase {

  var renamed_selected: DataFrame = _
  var dataTargetSelected: DataFrame = _

  before {
    // Configure implicits
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // Grab the QA csv
    val (dataTarget, dataIn) = prepareQaData(spark,
      "/Users/willbowditch/projects/ds-ais/QA/123.csv")

    // Apply the transformation used in the main script
    val dataOut = transform(spark, dataIn)

    // Generate a map to rename and select columns (oldname -> newname)
    val lookup = Map[String, String](
      "id" -> "id",
      "decoded_repeate" -> "repeat_indicator",
      "decoded_MMSI" -> "mmsi",
      "longitude" -> "x",
      "latitude" -> "y",
      "navigation_status" -> "nav_status",
      "position_accuracy" -> "position_accuracy",
      "course_over_ground" -> "cog",
      "true_heading" -> "true_heading",
      "timestamp_seconds" -> "timestamp",
      "manoeuvre_indicator" -> "special_manoeuvre",
      "raim" -> "raim",
      "rate_of_turn" -> "rot",
      "speed_over_ground" -> "sog"
    )

    // Apply the above map to the dataframe
    renamed_selected= renameSelectMap(lookup, dataOut)


    // Select only column names in renamed_selected for comparison
    dataTargetSelected = dataTarget.select(renamed_selected.columns.map(m => col(m)): _*)

    println("Data from Spark decoder")
    renamed_selected.printSchema()
    renamed_selected.show()


    println("Data from QA dataset")
    dataTargetSelected.printSchema()
    dataTargetSelected.show()
  }

  test("Test 123 lat longs are very similar") {

//    val spark = SparkSession.builder().getOrCreate()
//    import spark.implicits._

    // Apply more stringent checks to lat and long, as they're most important
    assertDataFrameApproximateEquals(
      dataTargetSelected.select("x", "y"),
      renamed_selected.select("x", "y"),
      .000000001
    )
  }

  test("Test 123 Decoding matches QA dataset"){

    // Check that dataframeas are approximately equal
    assertDataFrameApproximateEquals(dataTargetSelected, renamed_selected, .001)


  }
}

