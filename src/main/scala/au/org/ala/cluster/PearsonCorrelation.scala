package au.org.ala.cluster

import java.io._


object PearsonCorrelation extends Application {
	
	import au.org.ala.util.FileHelper._
	
	println("Starting comparison")
	
	val spVsSpWriter = new OutputStreamWriter(new FileOutputStream("/data/clustering/species-v-species.txt"))
	val br = new BufferedReader(new FileReader("/data/clustering/oc-list.txt"))
	
	//line for line, read in a list of the values
	while(br.ready){ 
		
		val parts1 = br.readLine.split("\t")
		spVsSpWriter.write(parts1(0))
		
		//iterate through entire file
		val br2 = new BufferedReader(new FileReader("/data/clustering/oc-list.txt"))

		//load each line into a list
		while(br2.ready){ 
			
			//run pearson correlation comparison
			val parts2 = br2.readLine.split("\t")
			val correlation = pearsonCorrelation(parts1,parts2)
			
			//write value into matrix of species vs species
			spVsSpWriter.write("|")
			spVsSpWriter.write(correlation.toString)
		}
		spVsSpWriter.write("\n")
	}
	
	println("finished")
	
	def pearsonCorrelation(speciesList1:Array[String], speciesList2:Array[String]):Int = -1
}
