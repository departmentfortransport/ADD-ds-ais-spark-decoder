package uk.gov.dft.ais.decode.test

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, udf}
import org.scalatest.{BeforeAndAfter, FunSuite}
import uk.gov.dft.ais.decode.Decode24.{transformPartNumber, transformPartA}
import uk.gov.dft.ais.decode.test.TestUtils.{normaliseStringsLibAis, prepareQaData, renameSelectMap}

class Decode24A extends FunSuite with BeforeAndAfter with DataFrameSuiteBase {

  var matchedSparkDecodedData: DataFrame = _
  var matchedQAData: DataFrame = _

  before {
    // Configure spark
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    // Grab the QA csv
    val (dataTarget, dataIn) = prepareQaData(spark,
      "/Users/willbowditch/projects/ds-ais/QA/24_0.csv", testChecksum = false)

    // Apply the same transformations to the target
    val distinctDataTarget = dataTarget
      .select("name", "mmsi")
      .distinct

    // Apply transformation used in main script
    val addPartNumbers = transformPartNumber(spark, dataIn)
    val dataOut = transformPartA(spark, addPartNumbers)




    // Generate map to rename and select columns (old name -> new name)
    val lookup = Map[String, String](
      "ship_name" -> "name" ,
      "decoded_mmsi" -> "mmsi"
    )

    // Apply the above map to the data frame
    matchedSparkDecodedData = renameSelectMap(lookup, dataOut)
        .select("name", "mmsi")
        .distinct()
        .sort("name", "mmsi")


    // Apply a fix to string columns. libais (our QA decoder) converts some
    // strings to _ instead of their actual value. This function replicates
    // that behaviour so the dataframes match up.
    val stringFixUDF = udf(normaliseStringsLibAis _)
    matchedSparkDecodedData = matchedSparkDecodedData
      .withColumn("name", stringFixUDF($"name"))

    // Select only columns we've matched on above from QA set
    matchedQAData = distinctDataTarget
        .select(matchedSparkDecodedData.columns.map(m => col(m)): _*)
        .sort("name", "mmsi")

    println("Data from Spark decoder")
    matchedSparkDecodedData.printSchema()
    matchedSparkDecodedData.show()

    println("Data from QA data frame")
    matchedQAData.printSchema()
    matchedQAData.show()
  }

  test("Msg24 Part A ship names decode correctly"){
    assertDataFrameEquals(matchedQAData, matchedSparkDecodedData)
  }

}
