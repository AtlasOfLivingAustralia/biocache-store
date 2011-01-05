package au.org.ala.util

import java.io.File
import org.wyki.cassandra.pelops.{ Mutator, Pelops, Policy, Selector }
import scala.collection.mutable.{ LinkedList, ListBuffer }
import org.apache.cassandra.thrift.{ Column, ConsistencyLevel, ColumnPath, SlicePredicate, SliceRange }

/**
 * Loads an export from the old portal database of point lookups.
 * 
 * 
select p.id, p.latitude, p.longitude, g.id, g.name, g.region_type, gt.name from point p 
inner join point_geo_region pg on p.id=pg.point_id 
inner join geo_region g on g.id=pg.geo_region_id 
inner join geo_region_type gt on gt.id=g.region_type 
into outfile '/tmp/points3.txt';
 * 
 * @author Dave Martin (David.Martin@csiro.au)
 */
object PointLoader {

  def main(args: Array[String]): Unit = {
    import FileHelper._
    println("Starting Location Loader....")
    val file = new File("/data/points3.txt")
    val hosts = Array { "localhost" }
    val keyspace = "occurrence"
    val columnFamily = "location"
    val poolName = "test-pool"
    Pelops.addPool(poolName, hosts, 9160, false, keyspace, new Policy());
    
    var counter=0
    file.foreachLine { line => {
    		counter+=1
    		//add point with details to 
    		val parts = line.split('\t')
    		val latitude = (parts(1).toFloat)/10000
		 	val longitude = (parts(2).toFloat)/10000
    		val geoRegionName = parts(4)
    		val geoRegionTypeId = parts(5).toInt
    		val geoRegionTypeName = parts(6)
    		val guid = latitude +"|"+longitude
    		val mutator = Pelops.createMutator(poolName, keyspace)
    		mutator.writeColumn(guid, "location", mutator.newColumn("decimalLatitude", latitude.toString))
    		mutator.writeColumn(guid, "location", mutator.newColumn("decimalLongitude", longitude.toString))
    		if(geoRegionTypeId >=1 && geoRegionTypeId<= 2){ 
    			mutator.writeColumn(guid, "location", mutator.newColumn("stateProvince", geoRegionName)) 
    		} else if(geoRegionTypeId >=3 && geoRegionTypeId<= 12){ 
    			mutator.writeColumn(guid, "location", mutator.newColumn("lga", geoRegionName)) 
    		} else if(geoRegionTypeId == 2000){ 
    			mutator.writeColumn(guid, "location", mutator.newColumn("ibra", geoRegionName))
    			mutator.writeColumn(guid, "location", mutator.newColumn("habitat", "terrestrial"))
    		} else if(geoRegionTypeId >=3000 && geoRegionTypeId< 4000){ 
    			mutator.writeColumn(guid, "location", mutator.newColumn("imcra", geoRegionName))
    			mutator.writeColumn(guid, "location", mutator.newColumn("habitat", "marine"))
    		}
    		mutator.execute(ConsistencyLevel.ONE)
    		if(counter % 1000 == 0) println(counter)
    	}
    }
    println(counter)
    Pelops.shutdown
  }
}