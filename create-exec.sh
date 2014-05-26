mvn clean install -DskipTests=true
cp biocache.sh target/biocache
cat target/biocache-store-1.2-SNAPSHOT.jar >> target/biocache
chmod 777 target/biocache
tar zcvf biocache.tgz target/biocache target/lib
