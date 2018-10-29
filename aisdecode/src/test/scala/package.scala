package uk.gov.dft.ais.decode.test

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.scalatest._
import uk.gov.dft.ais.decode.RawAISPacket
import uk.gov.dft.ais.decode.decode123.transform
import uk.gov.dft.ais.decode.utils.{ais_to_binary, returnMessageType}
import uk.gov.dft.ais.decode.test.utils.prepare_qa_data



class TestDecode123 extends FunSuite with DataFrameSuiteBase {

  test("Get repeat"){

    // Configure implicits
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val (dataTarget, dataIn) = prepare_qa_data(spark,
      "/Users/willbowditch/projects/ds-ais/QA/123.csv")

    val dataOut = transform(spark, dataIn)

    // Rehshape the dataOut so if looks like our QA set
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

    // Map to rename and select columns
    val x: PartialFunction[String, Column] = {
       name: String => lookup.get(name) match {
        case Some(newname) => col(name).as(newname): Column
      }
    }

    // Apply the above PartialFunction
    val cols = dataOut.columns.collect{x}
    val renamed_selected = dataOut.select(cols: _*)


    // Select only column names in renamed_selected for comparison
    val dataTargetSelected = dataTarget.select(renamed_selected.columns.map(m => col(m)): _*)

    println("Data from Scala")
    renamed_selected.show()

    println("Data from python libais")
    dataTargetSelected.show()

    assertDataFrameApproximateEquals(dataTarget, renamed_selected, 0)

    println("BREAK!")

  }
}

