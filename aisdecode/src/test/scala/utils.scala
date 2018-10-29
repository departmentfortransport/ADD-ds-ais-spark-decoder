package uk.gov.dft.ais.decode.test
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.dft.ais.decode.RawAISPacket
import uk.gov.dft.ais.decode.utils.{ais_to_binary, returnMessageType}

object utils {
  def prepare_qa_data(spark: SparkSession, csv_location: String): (DataFrame, DataFrame) = {

    import spark.implicits._

    // Read in test dataset
    val QA_data = spark.read.format("csv")
      .option("header", "true")
      .option("interSchema", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .load(csv_location)

    // Get a starting dataset
    val messages = QA_data.select("rawInput").map(r => r.getString(0))

    //
    val ds = messages.map(l => RawAISPacket.parseAISString(l))

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

}
