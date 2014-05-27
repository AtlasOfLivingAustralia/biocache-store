mvn clean install -DskipTests=true
cp biocache.sh target/biocache
cat target/biocache-store-1.2-SNAPSHOT.jar >> target/biocache
chmod 777 target/biocache
cd target
tar zcvf ../biocache.tgz biocache lib
cd ..

