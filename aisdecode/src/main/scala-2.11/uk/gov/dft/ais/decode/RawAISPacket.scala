package uk.gov.dft.ais.decode

import uk.gov.dft.ais.decode.Utils.TimestampParse

import java.sql.Timestamp

import scala.util.Try
import scala.util.matching.Regex

object RawAISPacket{

  // Define the regex for the first part of the string.
  // Assuming this stays the same in future messages
  // sample: \s:ASM//Port=89//MMSI=2320706,c:1470182400*7E\
  val regexMMSI: Regex = "(?<=MMSI=)\\d*".r
  val regexTimestamp: Regex = "(?<=c:)\\d*".r
  val regexPort: Regex = "(?<=Port=)\\d*".r
  val regexS: Regex = "(?<=s:).*(?=\\/\\/P)".r
  val regexchecksum_meta: Regex = "((?<=\\*)[\\d\\w]*)".r

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
      packet_type = aisArray(0),
      fragment_count = tryIntSafe(aisArray(1)),
      fragment_n = tryIntSafe(aisArray(2)),
      message_id = tryIntSafe(aisArray(3)),
      radio_channel = aisArray(4),
      data = aisArray(5),
      padding = tryIntSafe(aisPaddingChecksum(0)),
      checksum_packet = aisPaddingChecksum(1),
      s = regexS.findAllIn(meta).mkString,
      port = someStringParse(regexPort.findFirstIn(meta)),
      port_mmsi = someStringParse(regexMMSI.findFirstIn(meta)),
      timestamp = TimestampParse(regexTimestamp.findFirstIn(meta)),
      checksum_meta = regexchecksum_meta.findFirstIn(meta).mkString
    )
  }
}

// Define a class for the raw data structure
case class RawAISPacket(
                         packet_type: String="",
                         fragment_count: Option[Int]=None,
                         fragment_n: Option[Int]=None,
                         message_id: Option[Int]=None,
                         radio_channel: String="",
                         data: String="",
                         padding: Option[Int]=None,
                         checksum_packet: String="",
                         s: String="",
                         port: Option[Int]=None,
                         port_mmsi: Option[Int]=None,
                         timestamp: Option[Timestamp]=None,
                         checksum_meta: String=""
)