package uk.gov.dft.ais.decode.test

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}
import uk.gov.dft.ais.decode.test.TestUtils.{normaliseStringsLibAis, prepareQaData, renameSelectMap}
import uk.gov.dft.ais.decode.Decode24.transform

class Decode24B extends FunSuite with BeforeAndAfter with DataFrameSuiteBase{

  var matchedSparkDecodedData: DataFrame = _
  var matchedQAData: DataFrame = _

  before{
    // Configure spark
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    // Grab the QA csv
    val (dataTarget, dataIn) = prepareQaData(spark,
      "/Users/willbowditch/projects/ds-ais/QA/24_1.csv")

    // Apply the same transformation we use in code
    val dataOut = transform(spark, dataIn)

    // Generate map to rename and select columns (old name -> new name)
    val lookup = Map[String, String](
      "id" -> "id" ,
      "repeat_indicator" -> "repeat_indicator" ,
      "mmsi" -> "mmsi" ,
      "part_number" -> "part_num" ,
      "ship_type" -> "type_and_cargo" ,
      "vendor_id" -> "vendor_id" ,
      "call_sign" -> "callsign" ,
      "to_bow" -> "dim_a" ,
      "to_stern" -> "dim_b" ,
      "to_port" -> "dim_c" ,
      "to_starboard" -> "dim_d"
    )

    // Apply the above map to the data frame
    matchedSparkDecodedData = renameSelectMap(lookup, dataOut)

    // Apply a fix to string columns. libais (our QA decoder) converts some
    // strings to _ instead of their actual value. This function replicates
    // that behaviour so the dataframes match up.
    val stringFixUDF = udf(normaliseStringsLibAis _)
    matchedSparkDecodedData = matchedSparkDecodedData
      .withColumn("vendor_id", stringFixUDF($"vendor_id"))
      .withColumn("callsign", stringFixUDF($"callsign"))

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

  test("Msg24 Part B data frame is same as QA data frame (to 3 decimal points)"){
    assertDataFrameApproximateEquals(matchedQAData, matchedSparkDecodedData, .001)
  }

}
