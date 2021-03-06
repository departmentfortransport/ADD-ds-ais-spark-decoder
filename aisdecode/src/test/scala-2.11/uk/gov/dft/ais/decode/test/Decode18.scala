package uk.gov.dft.ais.decode.test

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}
import uk.gov.dft.ais.decode.test.TestUtils.{prepareQaData, renameSelectMap}
import uk.gov.dft.ais.decode.Decode18.transform

class Decode18 extends FunSuite with BeforeAndAfter with DataFrameSuiteBase{

  var matchedSparkDecodedData: DataFrame = _
  var matchedQAData: DataFrame = _

  before{
    // Configure spark
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    // Grab the QA csv
    val (dataTarget, dataIn) = prepareQaData(spark,
      sys.env("QA_CSV_PATH") + "/18.csv")

    // Apply the same transformation we use in code
    val dataOut = transform(spark, dataIn)

    // Generate map to rename and select columns (old name -> new name)
    val lookup = Map[String, String](
      "id" -> "id",
      "repeat_indicator" -> "repeat_indicator",
      "mmsi" -> "mmsi",
      "speed_over_ground" -> "sog",
      "position_accuracy" -> "position_accuracy",
      "latitude" -> "y",
      "longitude" -> "x",
      "true_heading" -> "true_heading",
      "timestamp_seconds" -> "timestamp",
      "cs_unit" -> "unit_flag",
      "dsc_flag" -> "dsc_flag",
      "display_flag" -> "display_flag",
      "band_flag" -> "band_flag",
      "message_22_flag" -> "m22_flag",
      "raim" -> "raim"
    )

    // Apply the above map to the data frame
    matchedSparkDecodedData = renameSelectMap(lookup, dataOut)

    // Select only columns we've matched on above from QA set
    matchedQAData = dataTarget.select(
      matchedSparkDecodedData.columns.map(m => col(m)): _*
    )


    println("Data from Spark decoder")
    matchedSparkDecodedData.printSchema()
    matchedSparkDecodedData.show()

    println("Data from QA data frame")
    matchedQAData.printSchema()
    matchedQAData.show()

  }

  test("Msg18 data frame is same as QA data frame (to 3 decimal points)"){
    assertDataFrameApproximateEquals(matchedQAData, matchedSparkDecodedData, .001)
  }

  test("Msg18 lat longs equal to ten decimal points"){
    assertDataFrameApproximateEquals(
      matchedQAData.select("x", "y"),
      matchedSparkDecodedData.select("x", "y"),
      .00000000001
    )
  }

}
