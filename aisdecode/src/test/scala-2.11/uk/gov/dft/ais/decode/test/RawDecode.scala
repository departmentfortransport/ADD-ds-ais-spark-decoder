package uk.gov.dft.ais.decode.test

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.sql.DataFrame

import uk.gov.dft.ais.decode.RawDecode.transform

class RawDecode extends FunSuite with BeforeAndAfter with DataFrameSuiteBase {

  var transformed: DataFrame = _

  before{
    // Configure spark
    val spark = SparkSession.builder().getOrCreate()

    // Get the examples
    val examplesParrallel = spark.sparkContext.textFile(getClass.getResource("/raw_test.dat").getPath)

    // Transform the messages
    transformed = transform(spark, examplesParrallel)

    transformed.show()
  }

  test("Test that merge generated three rows from 6 sentences"){
    assert(transformed.count() === 3)
  }

  test("All 1 part messages assembled correctly"){
    import spark.implicits._
    transformed
      .filter($"fragment_count" === 1)
      .select($"data")
      .as[String]
      .collect()
      .map(x => assert(x == "PART?A"))
  }

  test("All 2 part messages assembled correctly"){
    import spark.implicits._
    transformed
      .filter($"fragment_count" === 2)
      .select($"data")
      .as[String]
      .collect()
      .map(x => assert(x == "PART?APART?B"))
  }

  test("All 3 part messages assembled correctly"){
    import spark.implicits._
    transformed
      .filter($"fragment_count" === 3)
      .select($"data")
      .as[String]
      .collect()
      .map(x => assert(x == "PART?APART?BPART?C"))
  }
}
