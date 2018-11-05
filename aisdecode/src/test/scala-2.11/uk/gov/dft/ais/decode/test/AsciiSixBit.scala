package uk.gov.dft.ais.decode.test

import org.scalatest.FunSuite
import uk.gov.dft.ais.decode.Utils.ais_6bit_asci

class AsciiSixBit extends FunSuite {
  val alphabet = List(
    List("000000",0,"@"),
    List("010000",16,"P"),
    List("100000",32," "),
    List("110000",48,"0"),
    List("000001",1,"A"),
    List("010001",17,"Q"),
    List("100001",33,"!"),
    List("110001",49,"1"),
    List("000010",2,"B"),
    List("010010",18,"R"),
    List("100010",34,"\""),
    List("110010",50,"2"),
    List("000011",3,"C"),
    List("010011",19,"S"),
    List("100011",35,"#"),
    List("110011",51,"3"),
    List("000100",4,"D"),
    List("010100",20,"T"),
    List("100100",36,"$"),
    List("110100",52,"4"),
    List("000101",5,"E"),
    List("010101",21,"U"),
    List("100101",37,"%"),
    List("110101",53,"5"),
    List("000110",6,"F"),
    List("010110",22,"V"),
    List("100110",38,"&"),
    List("110110",54,"6"),
    List("000111",7,"G"),
    List("010111",23,"W"),
    List("100111",39,"\'"),
    List("110111",55,"7"),
    List("001000",8,"H"),
    List("011000",24,"X"),
    List("101000",40,"("),
    List("111000",56,"8"),
    List("001001",9,"I"),
    List("011001",25,"Y"),
    List("101001",41,")"),
    List("111001",56,"9"),
    List("001010",10,"J"),
    List("011010",26,"Z"),
    List("101010",42,"*"),
    List("111010",58,":"),
    List("001011",11,"K"),
    List("011011",27,"["),
    List("101011",43,"+"),
    List("111011",59,";"),
    List("001100",12,"L"),
    List("011100",28,"\\"),
    List("101100",44,","),
    List("111100",60,"<"),
    List("001101",13,"M"),
    List("011101",29,"]"),
    List("101101",45,"-"),
    List("111101",61,"="),
    List("001110",14,"N"),
    List("011110",30,"^"),
    List("101110",46,"."),
    List("111110",62,">"),
    List("001111",15,"O"),
    List("011111",31,"_"),
    List("101111",47,"/"),
    List("111111",63,"?")
  )

  test("Test 6 bit ascii conversion matches table"){
    alphabet.foreach{
      case List(binaryString: String, _: Int, targetString: String) =>
        assert(ais_6bit_asci(binaryString) === targetString)
    }
  }
}
