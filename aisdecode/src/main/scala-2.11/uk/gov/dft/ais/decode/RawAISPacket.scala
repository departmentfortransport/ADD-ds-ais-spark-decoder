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