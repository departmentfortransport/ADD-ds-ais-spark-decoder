package uk.gov.dft.ais.decode

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import Utils.{extractInt, extractString, parseIntWScale, stringLength}
import RawClean.removeUnused

object Decode24 {
  /**
   * Decode type 24 messages
   * params 0 - read bucket location
   * params 1 - write bucket location (parquet file)
   */
  def main (args:Array[String]): Unit = {

    // Start a spark context
    val spark = SparkSession
      .builder()
      .appName("AIS-decode-24")
      .master("yarn")
      .config("spark.executor.cores", "2")
      .config("spark.executor.memory", "1g")
      .config("spark.default.parallelism", "36500")
      .config("spark.sql.shuffle.partitions", "36500")
      .getOrCreate()

    // this import has to go after val spark.
    import spark.implicits._

    // Read in the parquet files from first argument location
    val binary_decoded_messages = spark
      .read.format("parquet")
      .load(args(0))

    val joined_up = transform(spark, binary_decoded_messages)
    val out = removeUnused(spark, joined_up)

    // write out to parquet files, location specified in the second argument
    out.write.parquet(args(1))

  }

  def transform(spark: SparkSession, binaryDecodedMessages: DataFrame): DataFrame = {

    import spark.implicits._

    val add_part_number = transformPartNumber(spark, binaryDecodedMessages)
    val part_a_subset = transformPartA(spark, add_part_number)
    val part_b_decoded = transformPartB(spark, add_part_number)

    // Add part a, where available to the part b
    part_b_decoded.join(part_a_subset,
      usingColumns = Seq("decoded_mmsi", "timestamp"),
      joinType = "left"
    )
  }

  def transformPartA(spark: SparkSession, dataWithPartNumber: DataFrame): DataFrame = {
    import spark.implicits._

    // Separate out the message by parts.
    // Part A only contains ship name, so these can be merged onto the info in
    // Part B later
    val part_a = dataWithPartNumber.where($"part_number"===0)

    // Part A - Message can be 168 bits, but the last 8 are not used.
    val getShipName = udf [Option[String] , String] { x=>
      extractString(x, 40, 160)
    }

    val part_a_decoded = part_a
      .withColumn("ship_name", getShipName(part_a("dataBinary")))

    // Each ship can only have one name per mssi and timestamp, so reduce the
    // data set to distinct
    part_a_decoded
      .select("ship_name", "decoded_mmsi", "timestamp")
      .distinct
  }

  def transformPartB(spark: SparkSession, dataWithPartNumber: DataFrame): DataFrame = {
    import spark.implicits._

    // Part B
    val getShipType = udf [Option[Int] , String] ( x=> extractInt(x,40,48))

    //  Bits 48-89 are as described in ITU-R 1371-4. In earlier versions to 1371-3 this was one sixbit-encoded 42-bit
    // (7-character) string field, the name of the AIS equipment vendor. The last 4 characters of the string are
    // reinterpreted as a model/serial numeric pair. It is not clear that field practice has caught up with this
    // incompatible change. Implementations would be wise to decode that but span in both ways and trust human eyes
    // to detect when the final 4 characters of the string or the model and serial fields are garbage.
    val getVendorID = udf [Option[String] , String] { x=>
      // Vendor ID is 3 6bit characters
      extractString(x, 48, 90)
    }


    val getCallSign = udf [Option[String] , String] { x=>
      extractString(x, 90,132)
    }

    val getToBow = udf [Option[Int] , String] ( x=> extractInt(x,132,141))

    val getToStern = udf [Option[Int] , String] ( x=> extractInt(x,141,150))

    val getToPort = udf [Option[Int] , String] ( x=> extractInt(x,150,156))

    val getToStarboard = udf [Option[Int] , String] ( x=> extractInt(x,156,162))

    val getMothershipMMSI = udf [Option[Int] , String] {
      x=> extractInt(x,132,162)
    }

    // Bits 162-167 are not used ('spare')

//    val part_b = dataWithPartNumber
//      .where($"part_number"===1 ||
//        // Following libais some devices report part A as part B,
//        // but the bit length gives it away
//        $"part_number"===0 && stringLength($"dataBinary") === 162)

    val part_b = dataWithPartNumber
      .where($"part_number"===1 ||
        // Following libais some devices report part A as part B,
        // but the bit length gives it away
        $"part_number"===0 && stringLength($"dataBinary") === 162)


    // Process the part b messages
    part_b
      .withColumn("ship_type", getShipType(part_b("dataBinary")))
      .withColumn("vendor_id", getVendorID(part_b("dataBinary")))
      .withColumn("call_sign", getCallSign(part_b("dataBinary")))
      .withColumn("to_bow", getToBow(part_b("dataBinary")))
      .withColumn("to_stern", getToStern(part_b("dataBinary")))
      .withColumn("to_port", getToPort(part_b("dataBinary")))
      .withColumn("to_starboard", getToStarboard(part_b("dataBinary")))
      .withColumn("mothership_mmsi", getMothershipMMSI(part_b("dataBinary")))
  }

  def transformPartNumber(spark: SparkSession, binaryDecodedMessages: DataFrame): DataFrame ={
    import spark.implicits._
    // The section below defines user defined functions to extract data from
    // the binary string generated by rawdecode
    val getRepeat = udf [Option[Int], String] (x => extractInt(x,6,8))

    val getMMSI = udf [Option[Int] , String] (x=> extractInt(x,8,38))

    val getPartNumber = udf [Option[Int] , String] {
      // If 0, the rest of the message is Part A
      // if 1, the rest of the message is Part B
      x=> extractInt(x,38,40)
    }

    // Notes:
    //    From this point on the message might be type A or B. They are
    //    broadcast together, but may not be together in our DB. So we need
    //    to decode, match them up, then output them together. Because of this
    //    this is proably going to involve some shuffling.

    val msg_24_raw = binaryDecodedMessages
      // Filter just messages 24
      // this should be fast as data is partitioned my id
      .where($"id" === 24)
      // Only keep valid string lengths
      .where(
      stringLength($"dataBinary") === 168 ||
        stringLength($"dataBinary") === 162
    )

    // For each UDF run it on the dataset.
    msg_24_raw
      .withColumn("decoded_repeate", getRepeat(msg_24_raw("dataBinary")))
      .withColumn("decoded_mmsi", getMMSI(msg_24_raw("dataBinary")))
      .withColumn("part_number", getPartNumber(msg_24_raw("dataBinary")))
  }
}
