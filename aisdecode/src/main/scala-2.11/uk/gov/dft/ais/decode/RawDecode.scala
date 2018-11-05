package uk.gov.dft.ais.decode

import Utils.{TimestampParse, ais_to_binary, process_checksum}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{collect_list, concat_ws, row_number, udf, _}

object RawDecode {
  /**
   * Decode raw AIS messages
   * args - expects two arguments:
   *  input file e.g. 'path/to/files/(asterix).dat'
   *  output file e.g. 'path/to/output/folder/'
   */
  def main (args:Array[String]): Unit = {

    // Start a spark context
    val spark = SparkSession
      .builder()
      .appName("AIS-raw-decode")
      .master("yarn")
      .config("spark.executor.cores", "2")
      .config("spark.executor.memory", "1g")
      .config("spark.default.parallelism", "36500")
      .config("spark.sql.shuffle.partitions", "36500")
      .getOrCreate()

    // This import has to go after the spark val exists.
    import spark.implicits._

    // Read from the location given in args (0) as RDD
    val bucket = args(0)

    val lines = spark.sparkContext.textFile(bucket)

    val all_messages = transform(spark, lines)

    // write out to a parquet
    all_messages.write.partitionBy("id").parquet(args(1))

  }

  def transform(spark: SparkSession, lines: RDD[String]): DataFrame = {
    import spark.implicits._

    // Only keep messages that pass both checksums
    val passed_checksum = lines.filter(v => process_checksum(v))

    // Parse AIS strings into a structure
    val ds = passed_checksum.map(l => RawAISPacket.parseAISString(l)).toDS

    // Loop through and generate a unique ID for multi part messages
    // This method is tollerant of messages of any length and has been tested
    // on messages between 1-3 sentences.
    val window = Window
      .partitionBy($"mmsi", $"timestamp", $"fragmentCount")
      .orderBy("fragmentCount")

    val multi_part_messages_with_index = ds
      .withColumn("GeneratedID", $"fragmentN" - row_number.over(window))

    // Using the unique ID generated above, merge the messages by concatinating
    // the strings
    val merged_messages = multi_part_messages_with_index
      .groupBy(
        "packetType",
        "fragmentCount",
        "radioChannel",
        "padding",
        "s",
        "port",
        "mmsi",
        "timestamp",
        "GeneratedID")
      .agg(concat_ws("", collect_list("data")) as "data")

    // Drop the Generated ID
    var all_messages = merged_messages.drop("GeneratedID")

    // Convert ais message to binary string
    val binary_message_udf = udf(ais_to_binary _)
    all_messages = all_messages
      .withColumn("dataBinary", binary_message_udf($"data"))

    // Extract message type from binary string
    def returnMessageType(as_binary: String): Int = {
      Integer.parseInt(as_binary.slice(0,6), 2)
    }

    val messageTypeUDF = udf(returnMessageType _)
    all_messages.
      withColumn("id", messageTypeUDF($"dataBinary"))
  }
}
