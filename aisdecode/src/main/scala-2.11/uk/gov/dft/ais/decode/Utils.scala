package uk.gov.dft.ais.decode

import java.sql.Timestamp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.math.pow
import scala.util.Try

object Utils {
  /**
   * The data payload is an ASCII-encoded bit vector.
   * Each character represents six bits of data.
   * To recover the six bits, subtract 48 from the ASCII character
   * value; if the result is greater than 40 subtract 8.
   * According to [IEC-PAS], the valid ASCII characters for
   * this encoding begin with "0" (64) and end with "w" (87);
   * however, the intermediate range "X" (88) to "\_" (95)
   * is not used.
   * [encodedMessage ASCII encoded bit vector]
   */
  def ais_to_binary(encodedMessage:String): String = {
    encodedMessage
      // This step implements the above
      .map(ch => ch.toInt)
      .map(e => e - 48)
      .map {case e if e > 40 => e - 8; case e => e}
      // This step converts to a long binary string
      .map(e => String
        .format("%6s", Integer.toBinaryString(e))
        .replace(' ', '0')
      ).mkString
  }

    /**
     * To decode text encoded in AIS we need to convert
     * Six bit ASCII into regular ASCI
     * see: http://www.catb.org/gpsd/AIVDM.html#_ais_payload_data_types
     * [binarystring a string containing binary 6bit ASCII]
     */
    def ais_6bit_asci(binarystring:String): String = {
    binarystring
      // split the string into 6 bit chunks
      .split("(?<=\\G.{6})")
      // convert binary to Ints
      .map(c => Integer.parseInt(c, 2))
      // implement conversion of strange 6bit ASCII
      .map {case i if i <= 31 => i + 64;
            case i if i>31 => i;
            case i => i}
      // now we have regular ASCII integers convert to char
      .map {i => i.toChar}.mkString
  }


  /**
    * Calculate the XOR checksum for AIS arrays.
    * @param stringChecksumPair An array of the form [String, Checksum]
    * @return boolean True (checksum match) or False
    */
   def validate_ais_checksum(stringChecksumPair: Array[String]): Boolean =
     stringChecksumPair match {
       case Array(string, checksum_match) =>
         var checksum = 0
         // Map over the string performing XOR checksum calculation
         string.foreach{  char => checksum = checksum ^ char.toInt }
         if( f"$checksum%02X".toUpperCase == checksum_match.toUpperCase){
           true //Checksum matches!
         } else {
           println(f"""Error in checksum.
             Expected $checksum_match%h
             Found $checksum%02x
             For string: $string""")
           false //Checksum doesn't match
         }
       case _ => println("Error! Not an array!"); false
     }


    /**
     * Split and clean the checksums parts of the message then calculate result
     * Only returns true if both pass.
     * [message a string from the ais data file]
     */
    def process_checksum(message: String): Boolean = {
      // Split the strings using the !
      val validation_segments = message.split("!")
      // clean the leading and training \ from the AIS message
      // note that these are escpaed twice... \\\\ is \\ in regex
      val cleaned_messages = validation_segments
        .map { s => s.replaceAll("(^\\\\)|(\\\\$)", "")}
      // split messages and checksums into array [message, checksum]
      val split_messages = cleaned_messages.map{ pair => pair.split("\\*")}
      // For each checksum in a message pair see if it passes
      val result = split_messages.map{pair => validate_ais_checksum(pair)}
      // Only return true if both parts of the message pass checksum test
      result.forall(_ == true)
    }

    /**
     * Parse a binary string into an integer or None
     */
    def parseIntSafe(s: String): Option[Int] = {
      Try(Integer.parseInt(s,2)).toOption
    }

    /**
     * Parse integers that have a scale (i.e. negative numbers too)
     */
    def parseIntWScale(s: String): Option[Double] = {
      // parse the int as per usual
      val pInt= parseIntSafe(s).getOrElse(0)

      // if the integer is larger than it should be given the length then it
      // is a negative number (i.e. the scale)
      if (pInt> pow (2, s.length-1) ) {
        Some(pInt - pow(2, s.length))
      }
      // if not then it is a regular Int
      else {
        Some(pInt)
      }
    }

    /**
     * Convert a string encoded integer to a Java timestamp
     * Note - Multipying by 1,000 as Java timestamps are millisecond.
     */
    def TimestampParse(s: Option[String]): Option[Timestamp] = {
      Try(new Timestamp(Integer.parseInt(s.get.trim).toLong * 1000)).toOption
    }

    /**
     * Convenience function to take a slice of a string and parseIntSafe
     */
    def extractInt(input: String, start: Int, end: Int): Option[Int] = {
      parseIntSafe(input.slice(start, end))
    }

    /**
     * Convenience function to safely slice a string and convert from 6bit asci
     */
    def extractString(input: String, start: Int, end: Int): Option [String] = {
      val safe_start = if(input.length < start){input.length} else start
      val safe_end = if(input.length < end){input.length} else end
      Try(ais_6bit_asci(input.slice(safe_start, safe_end))).toOption
    }

    /**
     * Quick string length udf
     */
    val stringLength: UserDefinedFunction = udf [Int, String] {
      x => x.length()
      }


    /**
      * Extract message type from binary string
      */
    def returnMessageType(as_binary: String): Option[Int] = {
      Try(Integer.parseInt(as_binary.slice(0,6), 2)).toOption
    }

}
