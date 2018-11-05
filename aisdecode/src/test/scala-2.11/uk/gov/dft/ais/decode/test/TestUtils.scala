package uk.gov.dft.ais.decode.test

import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import uk.gov.dft.ais.decode.RawAISPacket
import uk.gov.dft.ais.decode.Utils.{ais_to_binary, process_checksum, returnMessageType}

object TestUtils {
  def prepareQaData(spark: SparkSession, csv_location: String, testChecksum: Boolean = true): (DataFrame, DataFrame) = {

    import spark.implicits._

    // Read in test dataset
    val QA_data = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .load(csv_location)

    // Get a starting dataset
    val messages = QA_data.select("rawInput").map(r => r.getString(0))

    val passed_checksum =  if(testChecksum){
      messages.filter(v => process_checksum(v))
    } else {
      println("Not checking checksums are valid")
      messages
    }

    val ds = passed_checksum.map(l => RawAISPacket.parseAISString(l))

    // Register UDFs for extracting data binary and tagging the id
    val binary_message_udf = udf(ais_to_binary _)
    val message_type_udf = udf(returnMessageType _)

    // Construct final dataframe (this is akin to what we would get from
    // uk.gov.dft.ais.raw
    val constructed_raw_data = ds
      .withColumn("dataBinary", binary_message_udf($"data"))
      .withColumn("id", message_type_udf($"dataBinary"))

    (QA_data, constructed_raw_data)
  }

  def renameSelectMap(lookupMap: Map[String, String], dataFrame: DataFrame): DataFrame = {
    val x: PartialFunction[String, Column] = {
      name: String => lookupMap.get(name) match {
        case Some(newname) => col(name).as(newname): Column
      }
    }

    // Apply the above PartialFunction
    val cols = dataFrame.columns.collect{x}

    dataFrame.select(cols: _*)
  }

  def normaliseStringsLibAis(s: String): String = {
    s.replaceAll("_", "-")
  }

}
