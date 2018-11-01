package uk.gov.dft.ais.decode

// This add jar magic is useful when running in a notebook, it allows for the import of functions from this library.
// %addJar file:///Users/willbowditch/projects/ds-ais/decode/aisdecode/target/scala-2.11/aisdecode_2.11-0.1.0.jar

import java.sql.Timestamp

import Utils.{TimestampParse, ais_to_binary, process_checksum}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{collect_list, concat_ws, row_number, udf, _}

import scala.util.Try
import scala.util.matching.Regex


// Define a class for the raw data structure
case class RawAISPacket(
  packetType: String="",
  fragmentCount: Option[Int]=None,
  fragmentN: Option[Int]=None,
  messageID: Option[Int]=None,
  radioChannel: String="",
  data: String="",
  padding: Option[Int]=None,
  checksumPacket: String="",
  s: String="",
  port: Option[Int]=None,
  mmsi: Option[Int]=None,
  timestamp: Option[Timestamp]=None,
  checksumMeta: String=""
)

object RawAISPacket{

  // Define the regex for the first part of the string.
  // Assuming this stays the same in future messages
  // sample: \s:ASM//Port=89//MMSI=2320706,c:1470182400*7E\
  val regexMMSI: Regex = "(?<=MMSI=)\\d*".r
  val regexTimestamp: Regex = "(?<=c:)\\d*".r
  val regexPort: Regex = "(?<=Port=)\\d*".r
  val regexS: Regex = "(?<=s:).*(?=\\/\\/P)".r
  val regexChecksumMeta: Regex = "((?<=\\*)[\\d\\w]*)".r

  def parseAISString(rawInputString: String): RawAISPacket = {
    // Split metadata!AIS packet
    val messageParts = rawInputString.split("!")
    val meta = messageParts(0)
    // Split AIS packet into an array (like a csv)
    val aisArray = messageParts(1).split(",")

    // The checksum uses the * separator, instead of ',' so
    // we need to sub that out
    val aisPaddingChecksum = aisArray(6).split("\\*")

    /**
     * Regex returns Option[String] which will be None when there is no
     * String. So this function safely converts it to an Option[Int] or
     * None.
     */
    def someStringParse(s: Option[String]): Option[Int] = {
      try{Some(Integer.parseInt(s.get.trim))}catch{
        case _: Exception => None}
    }

    /**
     * Parse a binary string into an integer or None
     */
    def tryIntSafe(s: String): Option[Int] = {
      Try(s.toInt).toOption
    }


    // Generate the structure from the above
    RawAISPacket(
      packetType = aisArray(0),
      fragmentCount = tryIntSafe(aisArray(1)),
      fragmentN = tryIntSafe(aisArray(2)),
      messageID = tryIntSafe(aisArray(3)),
      radioChannel = aisArray(4),
      data = aisArray(5),
      padding = tryIntSafe(aisPaddingChecksum(0)),
      checksumPacket = aisPaddingChecksum(1),
      s = regexS.findAllIn(meta).mkString,
      port = someStringParse(regexPort.findFirstIn(meta)),
      mmsi = someStringParse(regexMMSI.findFirstIn(meta)),
      timestamp = TimestampParse(regexTimestamp.findFirstIn(meta)),
      checksumMeta = regexChecksumMeta.findFirstIn(meta).mkString
    )
  }
}

object rawdecode {
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
    all_messages = all_messages.
      withColumn("id", messageTypeUDF($"dataBinary"))

    // write out to a parquet
    all_messages.write.partitionBy("id").parquet(args(1))

  }
}
