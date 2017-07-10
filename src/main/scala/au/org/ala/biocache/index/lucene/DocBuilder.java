package au.org.ala.biocache.index.lucene;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.schema.CopyField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Build a lucene document for use with LuceneIndexing.
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
 * </pre>
 * <p>
 * DocBuilder is not thread safe.
 */
public class DocBuilder {

    final private static Logger logger = Logger.getLogger(DocBuilder.class);

    private LuceneIndexing index;

    private IndexSchema schema;

    private Map<String, SchemaObject> schemaMap = new HashMap<String, SchemaObject>();

    private String currentId = "";

    private RecycleDoc doc;

    private boolean closed = true;

    DocBuilder(IndexSchema schema, LuceneIndexing index) {
        this.schema = schema;
        this.index = index;
    }

    public String getId() {
        return currentId;
    }

    class SchemaObject {

        private final SchemaField fieldOrNull;
        private final List<CopyField> copyFieldsList;

        SchemaObject(SchemaField fieldOrNull, List<CopyField> copyFieldsList) {
            this.fieldOrNull = fieldOrNull;
            this.copyFieldsList = copyFieldsList;
        }
    }


    /**
     * Start a new document.
     * <p>
     * After using newDoc() either release() or index() must be called.
     *
     * @param id for logging
     * @throws InterruptedException
     */
    public void newDoc(String id) throws InterruptedException {
        currentId = id;

        //reuse documents that are not closed
        if (closed) {
            doc = index.nextDoc();
        } else {
            logger.warn("Document was not indexed or released before calling DocBuilder.newDoc again.");
        }

        doc.reset();
        closed = false;
    }

    /**
     * Make the current RecycleDoc available for reuse.
     *
     * Do not call before index().
     */
    public void release() throws InterruptedException {
        if (!closed) {
            index.release(doc);
            closed = true;
        }
    }

    /**
     * add this document to the index.
     *
     * @return
     */
    public void index() throws IOException, InterruptedException {
        if (closed) {
            logger.error("DocBuilder.index() was called before newDoc() or after release(). Document not indexed.");
        } else {
            index.addDoc(doc);
            closed = true;
        }
    }

    /**
     * add a field to the current document
     * <p>
     * ignores boost
     * skips multivalue check
     * skips norms/boost check
     *
     * @param field
     * @param value
     */
    public void addField(String field, Object value) {
        if (value == null || value.toString().length() == 0)
            return;

        //reuse fields
        boolean reused = false;
        if (doc.setField(field, value)) reused = true;

        boolean used = false;

        SchemaObject s = schemaMap.computeIfAbsent(field, f ->
                new SchemaObject(schema.getFieldOrNull(f), schema.getCopyFieldsList(f)));

        SchemaField sf = s.fieldOrNull;
        List<CopyField> copyField = s.copyFieldsList;

        // load each field value
        try {
            if (sf != null && !reused) {
                used = true;
                addField(doc, sf, value);
            }

            // Check if we should copy this field value to any other fields.
            // This could happen whether it is explicit or not.
            if (copyField != null) {
                for (CopyField cf : copyField) {
                    SchemaField destinationField = cf.getDestination();
                    used = true;

                    // Perhaps trim the length of a copy field
                    if (value instanceof String && cf.getMaxChars() > 0) {
                        value = cf.getLimitedValue((String) value);
                    }

                    if (!doc.setField(destinationField.getName(), value)) {
                        addField(doc, destinationField, value);
                    }
                }
            }

        } catch (Exception ex) {
            logger.error("ERROR: Error adding field to id:'" + currentId + "' '" + field + "'='" + value + "' msg=" + ex.getMessage());
        }

        if (!used && !reused) {
            logger.error("ERROR: unknown field id:'" + currentId + "' '" + field + "'");
        }
    }

    private void addField(RecycleDoc doc, SchemaField field, Object val) {
        try {
            if (val instanceof IndexableField) {
                ((Field) val).setBoost(1f);
                doc.add(field, (Field) val);
            } else {

                for (IndexableField f : field.getType().createFields(field, val, 1f)) {
                    if (f != null) {
                        doc.add(field, f);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("ERROR adding one field '" + currentId + "' '" + field.getName() + "' = '" + val.toString() + "' > " + e.getMessage());
        }
    }

    public RecycleDoc getDoc() {
        return doc;
    }
}
