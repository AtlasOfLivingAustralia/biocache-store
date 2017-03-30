package au.org.ala.biocache.index.lucene;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
    final private static int MERGE_SEGMENTS = 8;

    private IndexSchema schema;
    private Long maxIndexSize;
    private String outputPath;
    private Integer ramBufferSize;
    private Integer docCacheSize;
    private Integer commitBatchSize;
    private Integer commitThreadCount;

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

        //include space for thread termination docs
        this.docPool = new LinkedBlockingQueue<RecycleDoc>(docCacheSize + commitThreadCount + 1);

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
    public static void merge(List<File> directories, File outputDirectory, Integer ramBufferSize,
                             Integer numberOfSegments, boolean skipTest) throws IOException {
        logger.info("Begin merge: " + outputDirectory.getPath());

        //test directories
        List<File> validDirectories = new ArrayList<File>();
        for (int i = directories.size() - 1; i >= 0; i--) {
            File dir = directories.get(i);

            Directory d = null;
            boolean valid = false;
            //test for valid lucene directory
            if (dir.isDirectory()) {
                if (skipTest) {
                    if (new File(directories.get(i).getPath() + "/segments.gen").exists()) {
                        logger.info("including '" + dir.getPath() + "' in index merge");
                        validDirectories.add(dir);
                        valid = true;
                    }
                } else {
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
            }
            if (!valid) {
                logger.error("not including '" + dir.getPath() + "' in index merge");
            }
        }

        if (!outputDirectory.exists()) outputDirectory.mkdirs();

        Directory directory = FSDirectory.open(outputDirectory);

        IndexWriterConfig config = new IndexWriterConfig(Version.LATEST, null);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        config.setRAMBufferSizeMB(ramBufferSize);

        config.setMergeScheduler(new ConcurrentMergeScheduler());
        ((ConcurrentMergeScheduler) config.getMergeScheduler()).setMaxMergesAndThreads(numberOfSegments, numberOfSegments);
        ((ConcurrentMergeScheduler) config.getMergeScheduler()).setMergeThreadPriority(3);

        IndexWriter writer = new IndexWriter(directory, config);

        List<Directory> dirs = new ArrayList();
        for (File f : validDirectories) {
            dirs.add(FSDirectory.open(f));
        }
        writer.addIndexes(dirs.toArray(new Directory[0]));

        if (numberOfSegments > directories.size()) {
            writer.forceMerge(numberOfSegments);
        }

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

    private AtomicInteger additionalDocs = new AtomicInteger(0); //memory leak tracking

    //true while waiting for the writer to release documents and stop.
    private AtomicInteger waitingForWriter = new AtomicInteger(0);

    private Object nextDocLock = new Object();
    /**
     * recycling documents
     */
    protected RecycleDoc nextDoc() throws InterruptedException {
        synchronized (nextDocLock) {
            RecycleDoc doc;
            int timeout = 0;
            boolean waiting = false;
            while (waiting || (doc = docPool.poll(timeout, TimeUnit.MILLISECONDS)) == null) {
                if (waitingForWriter.get() == 0 && documents.size() == 0) {
                    additionalDocs.incrementAndGet();
                    logger.debug("Memory leak. " + additionalDocs.get() + " additional RecycleDocs created. Are all DocBuilder.newDoc() objects released or indexed?");
                    doc = new RecycleDoc(schema);
                    break;
                } else {
                    timeout = 10;
                    waiting = true;
                }
            }

            return doc;
        }
    }

    private int noThreadBatchIdx = -1;

    protected void addDoc(RecycleDoc doc) throws InterruptedException, IOException {
        addDoc(doc, false);
    }

    private Object noThreadAddLock = new Object();

    protected void addDoc(RecycleDoc doc, boolean force) throws InterruptedException, IOException {
        if (commitThreadCount > 0) {
            documents.put(doc);
        } else {
            synchronized (noThreadAddLock) {
                waitingForWriter.incrementAndGet();
                if (doc.get("id") != null) {
                    if (noThreadBatchIdx < 0) noThreadBatch.add(doc);
                    else {
                        noThreadBatch.set(noThreadBatchIdx, doc);
                        noThreadBatchIdx++;
                    }
                } else if (doc.schema != null){ //schema == null when finishing index, do not release doc
                    release(doc);
                }

                if (force || (noThreadBatchIdx < 0 && noThreadBatch.size() >= commitBatchSize) || noThreadBatchIdx >= commitBatchSize) {
                    count += Math.min(noThreadBatch.size(), commitBatchSize);

                    //last put
                    if (force && noThreadBatchIdx >= 0 && noThreadBatchIdx < noThreadBatch.size()) {
                        //remove items
                        while (noThreadBatch.size() > noThreadBatchIdx && noThreadBatch.size() > 0)
                            noThreadBatch.remove(noThreadBatch.size() - 1);
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
                waitingForWriter.decrementAndGet();
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
        close(wait, false);
    }


    private Object closeLock = new Object();
    public void close(boolean wait, boolean merge) throws IOException, InterruptedException {
        synchronized (closeLock) {
            if (commitThreadCount > 0) {
                if (consumers.size() > 0) {
                    DocBuilder db = getDocBuilder();
                    db.newDoc("exit");
                    db.addField("id", "exit");

                    int alive = 0;
                    for (Consumer t : consumers) {
                        if (t.isAlive()) alive++;
                    }

                    if (alive > 0) {
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
                }
            } else {
                addDoc(new RecycleDoc(null), true);
            }

            closeIndex();

            //do merge
            if (merge && outputDirectories.size() > 1) {
                //setup adds a new dir
                setup();
                File output = new File(outputPath + "_merged");
                documents = null;
                docPool = null;
                consumers = null;
                System.gc();

                int mem = (int) Math.max((Runtime.getRuntime().freeMemory() * 0.75) / 1024 / 1024, ramBufferSize);
                merge(outputDirectories, output, mem, MERGE_SEGMENTS, true);

                //delete output directories that were merged
                for (File d : outputDirectories) {
                    d.delete();
                }
                outputDirectories.clear();
                outputDirectories.add(output);
            }
        }
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
        synchronized (additionalDocs) {
            if (additionalDocs.get() > 0) {
                additionalDocs.decrementAndGet();
                //additional docs created, do not return them to the pool.
            } else {
                docPool.put(doc);
            }
        }
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

                    waitingForWriter.incrementAndGet();

                    //close thread if requested
                    if (d.get("id") != null && d.get("id")[0].equals("exit")) {
                        //finish remainder
                        if (batch.size() > 0) {
                            batches.put(batch);
                        }

                        //wait and close commit threads
                        for (CommitThread t : commitThreads) {
                            batches.put(new ArrayList());
                        }
                        for (CommitThread t : commitThreads) {
                            t.join();
                        }
                        waitingForWriter.decrementAndGet();
                        break;
                    }

                    if (d.get("id") != null) {
                        batch.add(d);
                    } else {
                        release(d);
                    }

                    if (batch.size() >= commitBatchSize) {
                        count += commitBatchSize;
                        batches.put(batch);
                        batch = new ArrayList<RecycleDoc>(commitBatchSize);
                    } else {
                        waitingForWriter.decrementAndGet();
                    }
                }
            } catch (InterruptedException e) {
                for (CommitThread t : commitThreads) {
                    if (t.isAlive())
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
                        } catch (InterruptedException e) {
                            throw e;
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

                        //waitingForWriter was incremented by Consumer thread for each batch
                        waitingForWriter.decrementAndGet();

                        newIndexTest(commitLock);

                        time.addAndGet(System.nanoTime() - t1);
                    }
                } catch (InterruptedException e) {

                }
            }
        }
    }


    private Object setupLock = new Object();
    /**
     * setup index writer if it does not exist.
     * <p>
     * If the index writer is closed, another index directory will be created.
     *
     * @throws IOException
     */
    private void setup() throws IOException {
        synchronized (setupLock) {
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
                IndexWriterConfig config = new IndexWriterConfig(Version.LATEST, schema.getIndexAnalyzer());
                config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

                config.setRAMBufferSizeMB(ramBufferSize);

                writer = new IndexWriter(directory, config);
            }
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

    public static void main(String[] args) throws Exception {
        System.out.println("usage: LuceneIndexing [-skipTest | -removeDuplicates] sourceDirectory destinationDirectory\n\n" +
                "Default: merge source subdirectories into destinationDirectory after testing each subdirectory index.\n\n");

        boolean skipTest = false;
        String src;
        String dst;

        boolean merge = true;
        if ("-skipTest".equals(args[0])) {
            skipTest = true;
            src = args[1];
            dst = args[2];
        } else if ("-removeDuplicates".equals(args[0])) {
            src = args[1];
            dst = args[2];

            merge = false;
        } else {
            src = args[0];
            dst = args[1];
        }

        if (merge) {
            int numThreads = 8;
            int mem = (int) (Runtime.getRuntime().freeMemory() * 0.75) / 1024 / 1024;

            LuceneIndexing.merge(Arrays.asList(new File(src).listFiles()),
                    new File(dst), mem, numThreads, skipTest);
        } else {
            LuceneIndexing.removeDuplicates(src, dst);
        }
    }

    /**
     * remove duplicates by id of docs in src that appear in dst
     *
     * @param src
     * @param dst
     */
    private static void removeDuplicates(String src, String dst) throws IOException {
        Directory srcDir = FSDirectory.open(new File(src));
        IndexReader srcReader = DirectoryReader.open(srcDir);
        Set<String> srcIds = new HashSet<String>();
        for (int i = 0; i < srcReader.maxDoc(); i++) {
            srcIds.add(srcReader.document(i).get("id"));
        }
        srcReader.close();
        srcDir.close();

        Directory dstDir = FSDirectory.open(new File(dst));
        IndexReader dstReader = DirectoryReader.open(dstDir);
        Set<String> dstIds = new HashSet<String>();
        for (int i = 0; i < dstReader.maxDoc(); i++) {
            Document doc = dstReader.document(i);
            if (srcIds.contains(doc.get("id")))
                dstIds.add(doc.get("id"));

        }
        dstReader.close();

        System.out.println("DELETING " + dstIds.size() + " documents");

        IndexWriterConfig config = new IndexWriterConfig(Version.LATEST, null);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        IndexWriter writer = new IndexWriter(dstDir, config);
        for (String id : dstIds) {
            writer.deleteDocuments(new Term("id", id));
        }
        writer.commit();
        writer.close();
        dstDir.close();
    }

}
