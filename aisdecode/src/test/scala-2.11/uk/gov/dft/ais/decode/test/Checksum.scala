package uk.gov.dft.ais.decode.test

import org.scalatest.FunSuite
import uk.gov.dft.ais.decode.Utils.process_checksum

class Checksum  extends FunSuite {
  // Just test a couple here
  val should_pass: String = "\\s:ASM//Port=102//MMSI=2320767,c:1459470764*49\\" +
    "!AIVDM,1,1,,A,13aEOK?P00PD2wVMdLDRhgvL289?,0*26"
  val should_fail: String = "\\s:ASM//Port=102//MMSI=2320767,c:1459470764*42\\" +
    "!AIVDM,1,1,,A,13aEOK?P00PD2wVMdLDRhgvL289?,0*28"

  test("Correct checksums pass") {
    assert(process_checksum(should_pass))
  }

  test("Incorrect checksums fail"){
    assert(!process_checksum(should_fail))
  }

}
