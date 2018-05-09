package au.org.ala.biocache

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.Assertions._
import au.org.ala.biocache.parser.CollectorNameParser


@RunWith(classOf[JUnitRunner])
class CollectorParserTest extends FunSuite {
//
//  test("Surname, FirstName combinations"){
//    val result = CollectorNameParser.parse("Beauglehole, A.C.")
//    expectResult("Beauglehole, A.C."){result.get}
//    expectResult("Beauglehole, A.C. Atest"){CollectorNameParser.parse("Beauglehole, A.C. Atest").get}
//    expectResult("Beauglehole, A. Atest"){CollectorNameParser.parse("Beauglehole, Atest").get}
//    expectResult("Field, P. Ross"){CollectorNameParser.parse("Field, Ross P.").get}
//    expectResult("Robinson, A.C. Tony"){CollectorNameParser.parse(""""ROBINSON A.C. Tony"""").get}
//    expectResult(List("Graham, K.L. Kate")){CollectorNameParser.parseForList("GRAHAM K.L. Kate").get}
//    expectResult(List("natasha.carter@csiro.au")){CollectorNameParser.parseForList("natasha.carter@csiro.au").get}
//    expectResult(List("Gunness, A.G.")){CollectorNameParser.parseForList("A.G.Gunness et. al.").get}
//  }
//
//  test("FirstName, Surname"){
//    expectResult("Starr, S. Simon"){CollectorNameParser.parse("Simon Starr").get}
//    expectResult("Starr, S.S. Simon"){CollectorNameParser.parse("Simon S.S Starr").get}
//  }
//
//  test("Surname initials"){
//    expectResult(List("Wilson, P.J.")){CollectorNameParser.parseForList(""""WILSON P.J. N/A"""").get}
//  }
//
//  test("hyphen names"){
//    expectResult(List("Kenny, S.D. Sue","Wallace-Ward, D. Di")){CollectorNameParser.parseForList(""""KENNY S.D. Sue;WALLACE-WARD D. Di"""").get}
//    expectResult(List("Russell-Smith, J.")){CollectorNameParser.parseForList("""Russell-Smith, J.""").get}
//    expectResult(List("Davies, R.J-P. Richard")){CollectorNameParser.parseForList(""""DAVIES R.J-P. Richard"""").get}
//  }
//
//  test("title test"){
//    expectResult(List("Dittrich")){CollectorNameParser.parseForList("""Dittrich, Lieutenant""").get}
//  }
//
//  test("preffix surnames"){
//    expectResult(List("van Leeuwen, S.")){CollectorNameParser.parseForList("""van Leeuwen, S.""").get}
//    expectResult(List("van der Leeuwen, S. Simon")){CollectorNameParser.parseForList("""van der Leeuwen, Simon""").get}
//    expectResult(List("von Blandowski, J.W.T.L.")){CollectorNameParser.parseForList("""Blandowski, J.W.T.L. von""").get}
//  }
//
//  test("ignore brackets"){
//    expectResult(List("Kinnear, A.J.")){CollectorNameParser.parseForList(""""KINNEAR A.J. (Sandy)"""").get}
//    expectResult(Some("Ratkowsky, D. David")){CollectorNameParser.parse("David Ratkowsky (2589)")}
//  }
//
//  test("Initials then surname"){
//    expectResult("Kirby, N.L."){CollectorNameParser.parse("NL Kirby").get}
//    expectResult(List("Annabell, R. Graeme")){CollectorNameParser.parseForList("Annabell, Mr. Graeme R").get}
//    expectResult(List("Kaspiew, B.")){CollectorNameParser.parseForList("B Kaspiew (Professor)").get}
//    expectResult(List("Hegedus, Ms Alexandra - Australian Museum - Science")){CollectorNameParser.parseForList("Hegedus, Ms Alexandra - Australian Museum - Science").get}
//    expectResult(List("Hegedus, Ms Alexandra Danica - Australian Museum - Science")){CollectorNameParser.parseForList("Hegedus, Ms Alexandra Danica - Australian Museum - Science").get}
//  }
//
//  test("Unknown/Anonymous"){
//    expectResult(List("UNKNOWN OR ANONYMOUS")){CollectorNameParser.parseForList("No data").get}
//    expectResult(List("UNKNOWN OR ANONYMOUS")){CollectorNameParser.parseForList("[unknown]").get}
//    expectResult(List("UNKNOWN OR ANONYMOUS")){CollectorNameParser.parseForList(""""NOT ENTERED - SEE ORIGINAL DATA  -"""").get}
//    expectResult(List("UNKNOWN OR ANONYMOUS")){CollectorNameParser.parseForList(""""ANON  N/A"""").get}
//  }
//
//  test("Organisations"){
//    //println(CollectorNameParser.parse("Canberra Ornithologists Group"))
//    expectResult(List("Canberra Ornithologists Group")){CollectorNameParser.parseForList("Canberra Ornithologists Group").get}
//    expectResult(List(""""SA ORNITHOLOGICAL ASSOCIATION  SAOA"""")){CollectorNameParser.parseForList(""""SA ORNITHOLOGICAL ASSOCIATION  SAOA"""").get}
//    expectResult(List("Macquarie Island summer and wintering parties")){CollectorNameParser.parseForList("Macquarie Island summer and wintering parties").get}
//    expectResult("test Australian Museum test"){CollectorNameParser.parse("test Australian Museum test").get}
//    expectResult(List(""""NPWS-(SA) N/A"""")){CollectorNameParser.parseForList(""""NPWS-(SA) N/A"""").get}
//    expectResult(List("UNKNOWN OR ANONYMOUS")){CollectorNameParser.parseForList(""""NOT ENTERED - SEE ORIGINAL DATA -"""").get}
//  }
//
  test("Multiple collector tests"){
//    expectResult(List("Spillane, N. Nicole", "Jacobson, P. Paul")){CollectorNameParser.parseForList("Nicole Spillane & Paul Jacobson").get}
//    expectResult(List("Fisher, K. Keith","Fisher, L. Lindsay")){CollectorNameParser.parseForList("Keith & Lindsay Fisher").get}
    expectResult(List("Spurgeon, P. Pauline","Spurgeon, A. Arthur")){CollectorNameParser.parseForList("Pauline and Arthur Spurgeon").get}
//    expectResult(List("Andrews-Goff, V. Virginia","Spinks, J. Jim")){CollectorNameParser.parseForList("Virginia Andrews-Goff and Jim Spinks").get}
//    expectResult(List("Kemper, C.M. Cath","Carpenter, G.A. Graham")){CollectorNameParser.parseForList(""""KEMPER C.M. Cath""CARPENTER G.A. Graham"""").get}
//    expectResult(List("James, D. David","Scofield, P. Paul")){CollectorNameParser.parseForList("""David James, Paul Scofield""").get}
//    expectResult(List("Simmons, J.G.","Simmons, M.H.")){CollectorNameParser.parseForList("""Simmons, J.G.; Simmons, M.H.""").get}
//    expectResult(List("Hedley, C.","Starkey","Kesteven, H.L.")){CollectorNameParser.parseForList("""C.Hedley, Mrs.Starkey & H.L.Kesteven""").get}
//    expectResult(List("Gomersall, N.","Gomersall, V.")){CollectorNameParser.parseForList("""N.& V.Gomersall""").get}
//    expectResult(List("Kenny, S.D. Sue", "Wallace-Ward, D. Di")){CollectorNameParser.parseForList(""""KENNY S.D. Sue""WALLACE-WARD D. Di"""").get}
  }
//
//  test("separated by ampersands"){
//    expectResult(List("Aedo, C.", "Ulloa, C.")){ CollectorNameParser.parseForList("""C. Aedo & C. Ulloa""").get}
//  }

//  test("A. Greenland"){
//    expectResult(List("Greenand, A.")){ CollectorNameParser.parseForList("""A. Greenand""").get}
//    expectResult(List("Greenland, A.")){ CollectorNameParser.parseForList("""A. Greenland""").get}
//    expectResult(List("Marsland, A.")){ CollectorNameParser.parseForList("""A Marsland""").get}
//  }

//  test("museum victoria example"){
//    val str = """Dr Martin F. Gomon - Museums Victoria; Paul Armstrong - skipper, FV "Aquarius", Port Fairy; Peter Forsyth"""
//    expectResult(List("Dr Martin F. Gomon - Museums Victoria", "Forsyth, P. Peter")){ CollectorNameParser.parseForList(str).get}
//  }
}