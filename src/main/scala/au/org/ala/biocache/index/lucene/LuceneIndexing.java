package au.org.ala.biocache.index.lucene;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.solr.schema.IndexSchema;

import java.io.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

/**
 * Used to create a SOLR compatible index.
 * <p>
 * Obtain a DocBuilder with LuceneIndexing.getDocBuilder() for adding new documents.
 * <p>
 * To use without consumer threads set commitThreadCount=0
 * <p>
 * Usage:
 * <p>
 * <pre>
 * docBuilder = luceneIndexing.getDocBuilder()
 *
 * for each document
 *   try {
 *     docBuilder.newDoc(uniqueId)
 *
 *     for each field and value
 *       docBuilder.addField(fieldName, value)
 *
 *   } finally {
 *     docBuilder.index() or docBuilder.release()
 *   }
 *
 * //Merge index parts. The merged index can be used directly in the SOLR datadir.
 * LuceneIndexing.merge(luceneIndexing.getOutputDirectories(), targetDir)
 * </pre>
 */
public class LuceneIndexing {

    final private static Logger logger = Logger.getLogger(LuceneIndexing.class);

    private IndexSchema schema;
    private Long maxIndexSize;
    private String outputPath;
    private Integer ramBufferSize;
    private Integer docCacheSize;
    private Integer commitBatchSize;
    private Integer commitThreadCount;

    private StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);
    private List<File> outputDirectories = new ArrayList<File>();
    private int directoryCounter = 0;
    public LinkedBlockingQueue<RecycleDoc> documents;
    private LinkedBlockingQueue<RecycleDoc> docPool;
    private List<Consumer> consumers = new ArrayList();

    private IndexWriter writer = null;
    private Directory directory = null;

    public long count = 0;

    List<RecycleDoc> noThreadBatch;

    AtomicLong time = new AtomicLong(0L);


    /**
     * For quickly creating a SOLR compatible index.
     * <p>
     * maxIndexSize is the size (approx) before a second SOLR index is created.
     * <p>
     * When >1 index is created the outputPath used is outputPath + '_N' where m is a number.
     * <p>
     * IndexSchema e.g.
     * <pre>
     *   CoreContainer cc = CoreContainer.createAndLoad("/data/biocache-reindex/solr-template/biocache",
     *     new File("/data/biocache-reindex/solr-template/biocache/solr.xml"));
     *   IndexSchema schema = IndexSchemaFactory.buildIndexSchema("schema.xml",
     *   SolrConfig.readFromResourceLoader(cc.getResourceLoader(), "solrconfig.xml"));
     * </pre
     *
     * @param solrSchema
     * @param maxIndexSize Number of documents in the index. A new index is created when the limit is reached.
     *                     outputDirectories() lists the directory of each index created.
     * @param outputPath Directory for the first index. Write access to the parent directory is also required.
     * @param ramBufferSize Soft memory target for the LuceneIndex.
     * @param docCacheSize Number of documents that can exist at one time.
     * @param commitBatchSize Number of documents that are committed to the index at one time.
     * @param commitThreadCount set to 0 for an instance without Consumer threads.
     */
    public LuceneIndexing(IndexSchema solrSchema, Long maxIndexSize, String outputPath, Integer ramBufferSize,
                          Integer docCacheSize, Integer commitBatchSize, Integer commitThreadCount) throws InterruptedException {
        this.schema = solrSchema;
        this.maxIndexSize = maxIndexSize;
        this.outputPath = outputPath;
        this.ramBufferSize = ramBufferSize;
        this.docCacheSize = docCacheSize;
        this.commitBatchSize = Math.min(docCacheSize, commitBatchSize);
        this.commitThreadCount = commitThreadCount;

        initDocumentQueues();

        if (commitThreadCount > 0) {
            Consumer c = new Consumer();
            consumers.add(c);
            c.start();
        } else {
            noThreadBatch = new ArrayList<RecycleDoc>(commitBatchSize);
        }
    }

    public LuceneIndexing(IndexSchema solrSchema, String outputPath) throws InterruptedException {
        this.schema = solrSchema;
        this.outputPath = outputPath;
        this.maxIndexSize = Integer.MAX_VALUE - 100L;
        this.ramBufferSize = 100;
        this.docCacheSize = 5000;
        this.commitBatchSize = 5000;
        this.commitThreadCount = 1;

        initDocumentQueues();

        Consumer c = new Consumer();
        consumers.add(c);
        c.start();
    }

    private void initDocumentQueues() throws InterruptedException {

        this.documents = new LinkedBlockingQueue<RecycleDoc>(docCacheSize);
        this.docPool = new LinkedBlockingQueue<RecycleDoc>(docCacheSize);

        for (int i = 0; i < docCacheSize; i++) {
            this.docPool.put(new RecycleDoc(schema));
        }
    }

    public long getCount() {
        return count;
    }

    /**
     * list all output directories that were created.
     *
     * @return
     */
    public List<File> getOutputDirectories() {
        return outputDirectories;
    }

    /**
     * Merge a list of index directories.
     *
     * @param directories
     * @param outputDirectory
     * @param ramBufferSize
     * @param numberOfSegments
     * @throws IOException
     */
    public static void merge(List<File> directories, File outputDirectory, Integer ramBufferSize, Integer numberOfSegments) throws IOException {
        logger.info("Begin merge");

        //test directories
        List<File> validDirectories = new ArrayList<File>();
        for (int i=directories.size()-1; i>=0;i--) {
            File dir = directories.get(i);

            Directory d = null;
            boolean valid = false;
            //test for valid lucene directory
            if (dir.isDirectory()) {
                try {
                    d = FSDirectory.open(dir);
                    CheckIndex ci = new CheckIndex(d);
                    ci.setFailFast(true);
                    ci.checkIndex();

                    valid = true;
                    validDirectories.add(dir);
                    logger.info("including '" + dir.getPath() + "' in index merge");
                } catch (Exception e) {
                    //failed fast
                } finally {
                    if (d != null) {
                        try {
                            d.close();
                        } catch (Exception e) {
                            logger.error("Failed to close lucene directory '" + dir.getPath() + "'");
                            valid = false;
                        }
                    }
                }
            }
            if (!valid) {
                logger.error("not including '" + dir.getPath() + "' in index merge");
            }
        }

        if (!outputDirectory.exists()) outputDirectory.mkdirs();

        Directory directory = FSDirectory.open(outputDirectory);

        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_42, new StandardAnalyzer(Version.LUCENE_42));
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        config.setRAMBufferSizeMB(ramBufferSize);

        IndexWriter writer = new IndexWriter(directory, config);

        List<Directory> dirs = new ArrayList();
        for (File f : validDirectories) {
            dirs.add(FSDirectory.open(f));
        }
        writer.addIndexes(dirs.toArray(new Directory[0]));

        writer.forceMerge(numberOfSegments);
        writer.commit();
        writer.close();
        directory.close();

        logger.info("Finished merge: " + outputDirectory.getPath());
    }

    /**
     * @return
     */
    public DocBuilder getDocBuilder() {
        return new DocBuilder(schema, this);
    }

    public Long getTiming() {
        return time.get();
    }

    /**
     * recycling documents
     */
    protected RecycleDoc nextDoc() throws InterruptedException {
        RecycleDoc doc;
        int count = 0;
        while ((doc = docPool.poll(2L, TimeUnit.MINUTES)) == null) {
            logger.warn(count * 2 + " minutes waiting for a free document... are all DocBuilder.newDoc() objects released or indexed?");
        }

        return doc;
    }

    private int noThreadBatchIdx = -1;

    protected void addDoc(RecycleDoc doc) throws InterruptedException, IOException {
        addDoc(doc, false);
    }

    protected void addDoc(RecycleDoc doc, boolean force) throws InterruptedException, IOException {
        if (commitThreadCount > 0) {
            documents.put(doc);
        } else {
            if (doc.get("id") != null) {
                if (noThreadBatchIdx < 0) noThreadBatch.add(doc);
                else {
                    noThreadBatch.set(noThreadBatchIdx, doc);
                    noThreadBatchIdx++;
                }
            }

            if (force || (noThreadBatchIdx < 0 && noThreadBatch.size() >= commitBatchSize) || noThreadBatchIdx >= commitBatchSize) {
                count += commitBatchSize;

                //last put
                if (force && noThreadBatchIdx < noThreadBatch.size()) {
                    //remove items
                    while (noThreadBatch.size() > noThreadBatchIdx)
                        noThreadBatch.remove(noThreadBatch.size());
                }

                long t1 = System.nanoTime();
                setup();

                try {
                    writer.addDocuments(noThreadBatch);
                    if (logger.isDebugEnabled()) {
                        logger.debug(outputPath + " >>> numDocs=" + writer.numDocs() + " ramDocs=" + writer.numRamDocs() +
                                " ramBytes=" + writer.ramBytesUsed());
                    }
                } catch (IOException e) {
                    logger.error("Error committing batch. " + e.getMessage(), e);
                } catch (Exception e) {
                    logger.error("failed to index: " + e.getMessage(), e);
                    if (logger.isDebugEnabled()) {
                        //Dump the batch that contains the faulty documents to tmp dir
                        logDebugError(noThreadBatch, e);
                    }
                } finally {
                    //recycle documents
                    for (RecycleDoc d : noThreadBatch) {
                        release(d);
                    }
                }

                //reuse noThreadBatch
                noThreadBatchIdx = 0;

                newIndexTest(null);

                time.addAndGet(System.nanoTime() - t1);
            }
        }
    }

    /**
     * Dump document values to a file.
     *
     * @param file
     * @param documents
     * @throws IOException
     */
    private void writeDocString(File file, List<RecycleDoc> documents) throws IOException {
        OutputStream out;
        if (file.getName().endsWith(".gz")) {
            out = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
        } else {
            out = new FileOutputStream(file);
        }
        CSVWriter csv = new CSVWriter(new OutputStreamWriter(out));
        for (RecycleDoc d : documents) {
            List<String> line = new ArrayList();
            Iterator<IndexableField> i = d.iterator();
            while (i.hasNext()) {
                line.add(i.next().stringValue());
            }
            csv.writeNext(line.toArray(new String[0]));
        }
        out.close();
    }

    private void logDebugError(List list, Exception e) {
        try {
            File f = File.createTempFile("indexing.error", "");
            FileUtils.writeStringToFile(f, e.getMessage() + "\n");
            writeDocString(new File(f.getPath() + ".data.gz"), list);
            logger.error("ERROR indexing: see " + f.getPath() + " for error and " + f.getPath() + ".data for data: " + e.getMessage());
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    /**
     * Checks if this index directory has reached the max size. If it has, open a new index.
     *
     * @param commitLock
     * @throws InterruptedException
     */
    private void newIndexTest(Semaphore commitLock) throws InterruptedException {
        if (writer.numDocs() > maxIndexSize) {
            try {
                //aquire all locks before closing the index
                if (commitLock != null) commitLock.acquire(commitThreadCount);
                closeIndex();
            } catch (IOException e) {
                logger.error("Error committing batch. " + e.getMessage(), e);
            } finally {
                //release all locks
                if (commitLock != null) commitLock.release(commitThreadCount);
            }
        }
    }

    /**
     * Close open index.
     *
     * @throws IOException
     */
    public void close(boolean wait) throws IOException, InterruptedException {
        if (commitThreadCount > 0) {
            if (consumers.size() > 0) {
                DocBuilder db = getDocBuilder();
                db.newDoc("exit");

                if (!wait) {
                    //interrupt
                    for (Consumer t : consumers) {
                        t.interrupt();
                    }
                } else {
                    //wait to close
                    for (Consumer t : consumers) db.index();
                    for (Consumer t : consumers) t.join();
                }
            }
        } else {
            addDoc(new RecycleDoc(null), true);
        }

        closeIndex();
    }

    private void closeIndex() throws IOException {
        if (writer != null) {
            writer.commit();
            writer.close();
            directory.close();
            writer = null;
            directory = null;
        }
    }

    /**
     * release a document without adding it to the index.
     *
     * @param doc
     */
    public void release(RecycleDoc doc) throws InterruptedException {
       docPool.put(doc);
    }

    /**
     * Put processed documents into batches before committing to the index.
     */
    class Consumer extends Thread {
        List<CommitThread> commitThreads = new ArrayList();
        LinkedBlockingQueue<List<RecycleDoc>> batches = new LinkedBlockingQueue<List<RecycleDoc>>();

        Semaphore commitLock = new Semaphore(commitThreadCount, true);

        List<RecycleDoc> batch = new ArrayList<RecycleDoc>(commitBatchSize);

        @Override
        public void run() {
            for (int i = 0; i < commitThreadCount; i++) {
                CommitThread t = new CommitThread();
                commitThreads.add(t);
                t.start();
            }

            try {
                while (true) {
                    RecycleDoc d = documents.take();

                    if (d.get("id") != null && d.get("id")[0].equals("exit")) {
                        //finish remainder
                        if (batch.size() > 0) {
                            batches.add(batch);
                        }

                        //wait and close commit threads
                        for (CommitThread t : commitThreads) {
                            batches.add(new ArrayList());
                        }
                        for (CommitThread t : commitThreads) {
                            t.join();
                        }
                        break;
                    }

                    if (d.get("id") != null) {
                        batch.add(d);
                    }

                    if (batch.size() >= commitBatchSize) {
                        count += commitBatchSize;
                        batches.add(batch);
                        batch = new ArrayList<RecycleDoc>(commitBatchSize);
                    }
                }
            } catch (InterruptedException e) {
                for (CommitThread t : commitThreads) {
                    t.interrupt();
                }
            }
        }

        /**
         * Commits document batches to the index.
         */
        class CommitThread extends Thread {
            @Override
            public void run() {
                try {
                    while (true) {
                        List<RecycleDoc> batch = batches.take();

                        if (batch.size() == 0) {
                            break;
                        }

                        long t1 = System.nanoTime();

                        try {
                            //safely access index writer
                            commitLock.acquire();

                            setup();

                            writer.addDocuments(batch);

                            if (logger.isDebugEnabled()) {
                                logger.debug(outputPath + " >>> numDocs=" + writer.numDocs() + " ramDocs=" + writer.numRamDocs() + " ramBytes=" + writer.ramBytesUsed());
                            }
                        } catch (IOException e) {
                            logger.error("Error committing batch. " + e.getMessage(), e);
                        } catch (Exception e) {
                            if (logger.isDebugEnabled()) {
                                //Dump the batch with the faulty documents to tmp dir
                                logDebugError(batch, e);
                            }
                        } finally {
                            commitLock.release();

                            //recycle documents
                            for (RecycleDoc d : batch) {
                                release(d);
                            }

                        }

                        newIndexTest(commitLock);

                        time.addAndGet(System.nanoTime() - t1);
                    }
                } catch (InterruptedException e) {

                }
            }
        }
    }

    /**
     * setup index writer if it does not exist.
     * <p>
     * If the index writer is closed, another index directory will be created.
     *
     * @throws IOException
     */
    private synchronized void setup() throws IOException {
        if (directory == null || writer == null) {
            File file;
            if (directoryCounter == 0) {
                file = new File(outputPath);
            } else {
                file = new File(outputPath + "_" + directoryCounter);
            }
            file.mkdirs();
            directory = FSDirectory.open(file);
            directoryCounter = directoryCounter + 1;

            outputDirectories.add(file);

            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_42, analyzer);
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

            config.setRAMBufferSizeMB(ramBufferSize);

            writer = new IndexWriter(directory, config);
        }
    }

    public int getThreadCount() {
        return commitThreadCount;
    }

    public int getQueueSize() {
        if (commitThreadCount > 0 && documents != null) {
            return documents.size();
        } else {
            return 0;
        }
    }

    public int getBatchSize() {
        if (commitThreadCount > 0) {
            if (consumers == null || consumers.size() < 1 || consumers.get(0).batch == null) {
                return 0;
            }
            return consumers.get(0).batch.size();
        } else {
            if (noThreadBatch == null) return 0;
            return Math.max(noThreadBatchIdx, noThreadBatch.size());
        }
    }

    public int ramDocs() {
        try {
            if (writer == null) return 0;
            return writer.numRamDocs();
        } catch (Exception e) {
        }
        return 0;
    }

    public long ramBytes() {
        try {
            if (writer == null) return 0;
            return writer.ramBytesUsed();
        } catch (Exception e) {
        }
        return 0;
    }

    public int getCommitThreadCount() {
        return commitThreadCount;
    }

    public static void main(String [] args)  throws Exception {
        LuceneIndexing.merge(Arrays.asList(new File("/data/biocache-reindex/solr-create/biocache-thread-0").listFiles()),
                new File("/data/merge"), 2000, 8);
    }

}
