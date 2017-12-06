package au.org.ala.biocache.index.lucene;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.schema.CopyField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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

    private static ConcurrentHashMap<String, String> additionalSchemaEntries = new ConcurrentHashMap<String, String>();

    private static Boolean isIndexing = true;

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

        private final SchemaField field;
        private final List<CopyField> copyFieldsList;

        SchemaObject(SchemaField fieldOrNull, List<CopyField> copyFieldsList) {
            this.field = fieldOrNull;
            this.copyFieldsList = copyFieldsList;
        }
    }

    /**
     * Start a new document.
     * <p>
     * After using newDoc() either release() or index() must be called.
     *
     * @param id for logging
     * @throws InterruptedException when interrupted while waiting for the next document object
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
     * <p>
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
     */
    public void index() throws IOException, InterruptedException {
        if (closed) {
            logger.error("DocBuilder.index() was called before newDoc() or after release(). Document not indexed.");
        } else {
            index.addDoc(doc);
            closed = true;
        }
    }

    public void addField(String field, Object value) {
        addField(field, value, true);
    }

    /**
     * add a field to the current document
     * <p>
     * ignores boost
     * skips multivalue check
     * skips norms/boost check
     *
     * @param field name
     * @param value of the field
     * @param index value when this is a new field for the schema
     */

    public void addField(String field, Object value, Boolean index) {
        if (value == null || value.toString().length() == 0)
            return;

        //reuse fields
        boolean reused = false;
        if (doc.setField(field, value)) reused = true;

        boolean used = false;

        SchemaObject s = schemaMap.get(field);
        if (s == null) {
            SchemaField f = schema.getFieldOrNull(field);
            List<CopyField> cf = schema.getCopyFieldsList(field);

            if (f == null) {
                if (DocBuilder.additionalSchemaEntries.get(field) == null) {
                    makeField(field, index);
                }
                return;
            }

            s = new SchemaObject(f, cf);
            schemaMap.put(field, s);
        }

        SchemaField sf = s.field;
        List<CopyField> copyField = s.copyFieldsList;

        // load each field value
        if (DocBuilder.isIndexing) {
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
                logger.info("unknown field id:'" + currentId + "' '" + field + "'");
            }
        }
    }

    /**
     * Make a new field for the schema.
     *
     * @param field
     * @return
     */
    private SchemaField makeField(String field, Boolean index) {
        //org.apache.solr.schema.FieldProperties, indexed=$index stored=true omitNorms=true
        int properties = (index ? 1 : 0) + 4 + 16;

        SchemaField sf;

        //TODO: should the _{type} be removed from the field name to make it clean?

        if (field.matches("^el[0-9]*$")) {
            sf = new SchemaField(field, schema.getFieldTypeByName("tfloat"), properties, null);
        } else if (field.endsWith("_i") || field.endsWith("_i_rng")) {
            sf = new SchemaField(field, schema.getFieldTypeByName("tint"), properties, null);
        } else if (field.endsWith("_l")) {
            sf = new SchemaField(field, schema.getFieldTypeByName("tlong"), properties, null);
        } else if (field.endsWith("_d") || field.endsWith("_d_rng")) {
            sf = new SchemaField(field, schema.getFieldTypeByName("tdouble"), properties, null);
        } else if (field.endsWith("_f")) {
            sf = new SchemaField(field, schema.getFieldTypeByName("tfloat"), properties, null);
        } else if (field.endsWith("_dt")) {
            sf = new SchemaField(field, schema.getFieldTypeByName("tdate"), properties, null);
        } else {
            sf = new SchemaField(field, schema.getFieldTypeByName("string"), properties, null);
        }

        synchronized (schema) {
            String exists = DocBuilder.additionalSchemaEntries.get(field);

            if (exists == null) {
                DocBuilder.additionalSchemaEntries.put(field, "<field name=\"" + field + "\" type=\"" + sf.getType().getTypeName() + "\" indexed=\"" + index + "\" stored=\"true\" omitNorms=\"true\" />");
            }
        }

        return sf;
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

    public static Collection<String> getAdditionalSchemaEntries() {
        return new ArrayList<String>(additionalSchemaEntries.values());
    }

    public static void setIsIndexing(Boolean isIndexing) {
        DocBuilder.isIndexing = isIndexing;
    }

    public static Boolean isIndexing() {
        return DocBuilder.isIndexing;
    }
}
