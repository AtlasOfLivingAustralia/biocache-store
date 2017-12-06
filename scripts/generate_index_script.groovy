
import groovy.json.JsonSlurper
statusUrl = new URL("http://52.59.26.46:8983/solr/admin/collections?action=clusterstatus&wt=json")
slurper = new JsonSlurper()
json = slurper.parseText(statusUrl.text)

collectionName = "biocache"

println("Parsing")

json.cluster.collections.biocache.shards.each { shard ->
    shardID = shard.key.replaceAll("shard", "")
    println("#############  shard ${shardID}")
    shard.getValue().replicas.each { replica ->
        address = replica.getValue().node_name.replaceAll(":8983_solr","")
        coreName = replica.getValue().core
        println("""ssh -o StrictHostKeyChecking=no    ${address}  "rm -Rf /data/solr-backup/${coreName}"  \\""")
        println("""&& ssh -o StrictHostKeyChecking=no ${address}  "mkdir /data/solr-backup/${coreName}"  \\""")
        println("""&& ssh -o StrictHostKeyChecking=no ${address}  "mv /data/solr/${coreName}/* /data/solr-backup/${coreName}/" """)
        println("""\n\nscp -o StrictHostKeyChecking=no -r /data/solr/biocache/* ${address}:/data/solr/${coreName}""")

        println()
        println()
    }
}