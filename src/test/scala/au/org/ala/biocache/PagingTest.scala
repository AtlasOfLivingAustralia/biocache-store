package au.org.ala.biocache

object PagingTest {
	
	def main(args : Array[String]) : Unit = {
		OccurrenceDAO.pageOverAll(Version.Raw, fullRecord => { 
				val occurrence = fullRecord.get._1
				val classification = fullRecord.get._2
				val location = fullRecord.get._3
				val event = fullRecord.get._4
				println(occurrence.uuid+"\t"+classification.genus+"\t"+classification.specificEpithet+"\t"+classification.scientificName)
			}
		)
	}
}