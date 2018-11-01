package uk.gov.dft.ais.decode.test

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest._
import uk.gov.dft.ais.decode.Decode123.transform
import uk.gov.dft.ais.decode.test.TestUtils.{prepareQaData, renameSelectMap}



class Decode123 extends FunSuite with BeforeAndAfter with DataFrameSuiteBase {

  var matchedSparkDecodedData: DataFrame = _
  var matchedQaData: DataFrame = _

  before {
    // Configure implicits
    val spark = SparkSession.builder().getOrCreate()

    // Grab the QA csv
    val (dataTarget, dataIn) = prepareQaData(spark,
      "/Users/willbowditch/projects/ds-ais/QA/123.csv")

    // Apply the transformation used in the main script
    val dataOut = transform(spark, dataIn)

    // Generate a map to rename and select columns (old name -> new name)
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

    // Apply the above map to the data frame
    matchedSparkDecodedData= renameSelectMap(lookup, dataOut)


    // Select only column names in matchedSparkDecodedData for comparison
    matchedQaData = dataTarget.select(
      matchedSparkDecodedData.columns.map(m => col(m)): _*
    )

    // Printing out some info about the comparison data frames to aid diagnosis of any problems
    println("Data from Spark decoder")
    matchedSparkDecodedData.printSchema()
    matchedSparkDecodedData.show()

    println("Data from QA data frame")
    matchedQaData.printSchema()
    matchedQaData.show()
  }

  test("Test msg123 lat longs equal to ten decimal points") {
    // Apply more stringent checks to lat and long floats, as they're most important
    assertDataFrameApproximateEquals(
      matchedQaData.select("x", "y"),
      matchedSparkDecodedData.select("x", "y"),
      .00000000001
    )
  }

  test("Test that msg123 data frame is same as QA data frame (to 3 decimal points for floats) "){
    // Check that data frames are approximately equal
    assertDataFrameApproximateEquals(matchedQaData, matchedSparkDecodedData, .001)
  }
}

