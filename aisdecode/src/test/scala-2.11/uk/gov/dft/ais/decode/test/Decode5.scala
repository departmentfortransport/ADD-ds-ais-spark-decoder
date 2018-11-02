package uk.gov.dft.ais.decode.test
import org.apache.spark.sql.functions.udf
import uk.gov.dft.ais.decode.test.TestUtils.normaliseStringsLibAis

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}
import uk.gov.dft.ais.decode.test.TestUtils.{prepareQaData, renameSelectMap}
import uk.gov.dft.ais.decode.Decode5.transform
import org.apache.spark.sql.functions.col



class Decode5 extends FunSuite with BeforeAndAfter with DataFrameSuiteBase {

  var matchedSparkDecodedData: DataFrame = _
  var matchedQAData: DataFrame = _

  before {
    // Configure spark
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    // Grab the QA csv
    val (dataTarget, dataIn) = prepareQaData(spark,
      "/Users/willbowditch/projects/ds-ais/QA/5.csv", testChecksum = false)

    // Apply transformation used in main script
    val dataOut = transform(spark, dataIn)

    dataIn.show()
    dataTarget.show()

    // Generate map to rename and select columns (old name -> new name)
    val lookup = Map[String, String](
      "id" -> "id",
    "decoded_repeate" -> "repeat_indicator",
    "decoded_MMSI" -> "mmsi",
    "aisVersion" -> "ais_version",
    "IMO" -> "imo_num",
    "callSign" -> "callsign",
    "shipName" -> "name",
    "shipType" -> "type_and_cargo",
    "to_bow" -> "dim_a",
    "to_stern" -> "dim_b",
    "to_port" -> "dim_c",
    "to_starboard" -> "dim_d",
    "epfd" -> "fix_type",
    "ETA_month" -> "eta_month",
    "ETA_day" -> "eta_day",
    "ETA_hour" -> "eta_hour",
    "ETA_min" -> "eta_minute",
    "Draught" -> "draught",
    "Dest" -> "destination",
    "DTE" -> "dte"
    )

    // Apply the above map to the data frame
    matchedSparkDecodedData = renameSelectMap(lookup, dataOut)

    // Apply the fix to string columns
    val stringFixUDF = udf(normaliseStringsLibAis _)
    matchedSparkDecodedData = matchedSparkDecodedData
        .withColumn("destination", stringFixUDF($"destination"))
        .withColumn("name", stringFixUDF($"name"))

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



  test("Msg5 data frame is same as QA data frame (to 3 decimal points"){
    assertDataFrameApproximateEquals(matchedQAData, matchedSparkDecodedData, .1)
  }
}
