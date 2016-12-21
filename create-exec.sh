mvn clean install -DskipTests=true
cp biocache.sh target/biocache
cat target/biocache-store-*.jar >> target/biocache
chmod 777 target/biocache
cd target
cp ../src/main/resources/log4j.xml .
tar zcvf ../biocache.tgz biocache lib log4j.xml
cd ..