package au.org.ala.biocache.index.lucene;


import org.apache.log4j.Logger;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.BytesTermAttribute;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.spatial.prefix.CellToBytesRefIterator;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.util.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.*;
import org.apache.solr.schema.TextField;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.util.SpatialUtils;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.shape.Shape;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

import static org.apache.solr.schema.NumberType.INTEGER;

/**
 * A lucene document that recycles it's own fields.
 */
public class RecycleDoc implements Iterable<IndexableField> {
    public static final Logger logger = Logger.getLogger(RecycleDoc.class);

    List<Boolean> fieldEnabled = new ArrayList<Boolean>();
    List<SchemaField> schemaFields = new ArrayList<SchemaField>();
    List<List<IndexableField>> fields = new ArrayList<List<IndexableField>>();

    Map<String, List<Integer>> fieldOrder = new HashMap<String, List<Integer>>();
    int size = 0;

    IndexSchema schema;

    public RecycleDoc(IndexSchema schema) {
        this.schema = schema;
    }

    public void add(SchemaField schemaField, List<IndexableField> field) throws Exception {
        List<Integer> list = fieldOrder.computeIfAbsent(schemaField.getName(), k -> new ArrayList<Integer>());

        // Prevent multiple values being added to a field with multiValued=false. The exception will be thrown when
        // there is a fault in the schema or the indexing configuration. e.g. a column in Cassandra occ.occ has the
        // same SOLR formatted name as another field populated during indexing.
        if (list.size() > 0 && !schemaField.multiValued()) {
            String[] ids = get("id");
            if (ids != null && ids.length > 0) {
                throw new Exception("Attempting to add > 1 values for schema field '" + schemaField.getName() + "' when it is not multivalued. doc id=" + ids[0]);
            } else {
                throw new Exception("Attempting to add > 1 values for schema field '" + schemaField.getName() + "' when it is not multivalued.");
            }
        }
        list.add(fields.size());

        schemaFields.add(schemaField);
        fieldEnabled.add(true);
        fields.add(field);

        size++;
    }

    public void reset() {
        for (int i = fieldEnabled.size() - 1; i >= 0; i--) {
            fieldEnabled.set(i, false);
        }
        size = 0;
    }

    public String[] get(String name) {
        List<Integer> idx = fieldOrder.get(name);

        if (idx != null) {
            int pos = 0;
            for (Integer i : idx) {
                if (fieldEnabled.get(i)) pos++;
            }

            if (pos > 0) {
                String[] s = new String[pos];

                pos = 0;
                for (Integer i : idx) {
                    if (fieldEnabled.get(i))
                        // when there are multiple fields associated with this schemaField, both string values are assumed to be identical
                        s[pos++] = fields.get(i).get(0).stringValue();
                }
                return s;
            }
        }

        return null;
    }

    /**
     * Attempts to reuse an existing field for the new value.
     *
     * @param name  field name
     * @param value value to set the field if reuse is possible
     * @return false if reuse failed and a new field must be created. true if reuse succeeded and a new field
     * is not required.
     */
    public boolean setField(String name, Object value) {

        boolean found = false;

        List<Integer> idx = fieldOrder.get(name);

        if (idx == null) return false;

        int count = 0;
        while (count < idx.size() && fieldEnabled.get(idx.get(count))) count++;

        if (count < idx.size()) {
            int i = idx.get(count);
            SchemaField sf = schemaFields.get(i);
            org.apache.solr.schema.FieldType ft = sf.getType();
            for (IndexableField f : fields.get(i)) {

                //Set all data types that are in use
                try {
                    if (ft instanceof StrField) {
                        if(isSortedDocValuesField(f)) {
                            setSortedDocValuesField(f, value);
                        } else {
                            ((Field) f).setStringValue(String.valueOf(value));
                        }
                        found = true;
                    } else if (ft instanceof TrieField) {
                        if (f instanceof SortedSetDocValuesField) {
                            //multivalued=true docvalues=true
                            //TrieField creates LegacyFloatField (etc) first (fields.get(i).get(0))
                            ((Field) f).setBytesValue(storedToIndexed(fields.get(i).get(0)).get());

                            found = true;
                        } else if (f instanceof NumericDocValuesField) {
                            //multivalued=false docvalues=true
                            //TrieField creates LegacyFloatField (etc) first (fields.get(i).get(0))
                            ((Field) f).setLongValue(formatNumericDocValuesFieldValue(fields.get(i).get(0)));

                            found = true;
                        } else {
                            switch (((TrieField) ft).getNumberType()) {
                                case INTEGER:
                                    Integer valueAsInt = value instanceof Number ? ((Number) value).intValue() : Integer.parseInt((String) value);
                                    ((Field) f).setIntValue(valueAsInt);
                                    found = true;
                                    break;
                                case LONG:
                                    ((Field) f).setLongValue(value instanceof Number ? ((Number) value).longValue() : Long.parseLong((String) value));
                                    found = true;
                                    break;
                                case FLOAT:
                                    ((Field) f).setFloatValue(value instanceof Number ? ((Number) value).floatValue() : Float.parseFloat((String) value));
                                    found = true;
                                    break;
                                case DOUBLE:
                                    ((Field) f).setDoubleValue(value instanceof Number ? ((Number) value).doubleValue() : Double.parseDouble((String) value));
                                    found = true;
                                    break;
                                case DATE:
                                    ((Field) f).setLongValue(value instanceof Date ? ((Date) value).getTime() : DateMathParser.parseMath(null, value.toString()).getTime());
                                    found = true;
                                    break;
                                default:
                                    org.apache.lucene.document.FieldType.LegacyNumericType type = ((TrieField) ft).getNumericType();
                                    throw new RuntimeException("TrieField not recognised or supported. Ordinal value: " + type.ordinal() + ", " + type.getClass().getName());
                            }
                        }
                    } else if (ft instanceof TextField) {
                        ((Field) f).setStringValue((String) value);
                        found = true;
                    } else if (ft instanceof SpatialTermQueryPrefixTreeFieldType ||
                            ft instanceof SpatialRecursivePrefixTreeFieldType) {

                        PrefixTreeStrategy strategy = (ft instanceof SpatialTermQueryPrefixTreeFieldType) ?
                                ((SpatialTermQueryPrefixTreeFieldType) ft).getStrategy(name) :
                                ((SpatialRecursivePrefixTreeFieldType) ft).getStrategy(name);
                        Double distErrPct = strategy.getDistErrPct();
                        SpatialPrefixTree grid = strategy.getGrid();
                        SpatialContext ctx = strategy.getSpatialContext();

                        String str = String.valueOf(value);

                        if (f instanceof StoredField) {
                            ((StoredField) f).setStringValue(String.valueOf(str));

                            found = true;
                        } else {
                            Shape shape;
                            if (value instanceof Shape) {
                                shape = (Shape) value;
                            } else if (str.length() > 0 && Character.isLetter(str.charAt(0))) {
                                shape = ((WKTReader) ctx.getFormats().getWktReader()).parse(str);
                            } else {
                                shape = SpatialUtils.parsePointSolrException(str, ctx);
                            }

                            if (shape != null) {
                                double distErr = SpatialArgs.calcDistanceFromErrPct(shape, distErrPct, ctx);
                                int detailLevel = grid.getLevelForDistance(distErr);

                                Iterator<Cell> cells = grid.getTreeCellIterator(shape, detailLevel);
                                CellToBytesRefIterator cellToBytesRefIterator = new CellToBytesRefIterator();
                                cellToBytesRefIterator.reset(cells);

                                SpatialTokenStream tokenStream = new SpatialTokenStream();
                                tokenStream.setBytesRefIterator(cellToBytesRefIterator);
                                ((Field) f).setTokenStream(tokenStream);

                                fieldEnabled.set(idx.get(count), true);

                                //interate
                                count++;
                            }

                            if (sf.indexed()) {
                                found = true;
                            }
                        }
                    } else {
                        logger.error("MISSING FIELD " + name + " = " + value.toString() + ", " + ft.getClass().getName());
                    }
                } catch (NumberFormatException e) {
                    logger.error("NumberFormatException setting field " + name + " = " + value.toString() + ", " + ft.getClass().getName() + " : " + e.getMessage());
                } catch (Exception e) {
                    logger.error("FIELD EXCEPTION " + name + " = " + value.toString() + ", " + ft.getClass().getName() + " : " + e.getMessage(), e);
                }
            }

            if (found) {
                fieldEnabled.set(i, true);
            }
        }
        return found;
    }

    //source: org.apache.lucene.spatial.prefix.BytesRefIteratorTokenStream
    static class SpatialTokenStream extends TokenStream {
        private final BytesTermAttribute bytesAtt = this.addAttribute(BytesTermAttribute.class);
        private BytesRefIterator bytesIter = null;

        SpatialTokenStream() {
        }

        public BytesRefIterator getBytesRefIterator() {
            return this.bytesIter;
        }

        public SpatialTokenStream setBytesRefIterator(BytesRefIterator iter) {
            this.bytesIter = iter;
            return this;
        }

        public void reset() throws IOException {
            if (this.bytesIter == null) {
                throw new IllegalStateException("call setBytesRefIterator() before usage");
            }
        }

        public final boolean incrementToken() throws IOException {
            if (this.bytesIter == null) {
                throw new IllegalStateException("call setBytesRefIterator() before usage");
            } else {
                BytesRef bytes = this.bytesIter.next();
                if (bytes == null) {
                    return false;
                } else {
                    this.clearAttributes();
                    this.bytesAtt.setBytesRef(bytes);
                    return true;
                }
            }
        }
    }

    public boolean isSortedDocValuesField(IndexableField f) {
        return f instanceof SortedSetDocValuesField || f instanceof SortedDocValuesField;
    }

    public void setSortedDocValuesField(IndexableField f, Object value) throws UnsupportedEncodingException {
        if(f instanceof SortedSetDocValuesField) {
            byte[] valueAsBytes = String.valueOf(value).getBytes("UTF-8");
            ((SortedSetDocValuesField) f).setBytesValue(valueAsBytes);
        } else if(f instanceof SortedDocValuesField) {
            byte[] valueAsBytes = String.valueOf(value).getBytes("UTF-8");
            ((SortedDocValuesField) f).setBytesValue(valueAsBytes);
        } else {
            throw new RuntimeException("Problem indexing field: " + f.name() +" : class not recognised: " + f.getClass().getName() );
        }
    }

    // from org.apache.solr.schema.TrieField.createFields
    private long formatNumericDocValuesFieldValue(IndexableField field) {
        long bits;
        if (!(field.numericValue() instanceof Integer) && !(field.numericValue() instanceof Long)) {
            if (field.numericValue() instanceof Float) {
                bits = (long) Float.floatToIntBits(((Number) field.numericValue()).floatValue());
            } else {
                bits = Double.doubleToLongBits(((Number) field.numericValue()).doubleValue());
            }
        } else {
            bits = ((Number) field.numericValue()).longValue();
        }

        return bits;
    }

    // from org.apache.solr.schema.TrieField.storedToIndexed
    private BytesRefBuilder storedToIndexed(IndexableField f) {
        Number val = f.numericValue();
        BytesRefBuilder bytes = new BytesRefBuilder();
        if (val != null) {
            switch (INTEGER) {
                case INTEGER:
                    LegacyNumericUtils.intToPrefixCoded(val.intValue(), 0, bytes);
                    break;
                case FLOAT:
                    LegacyNumericUtils.intToPrefixCoded(NumericUtils.floatToSortableInt(val.floatValue()), 0, bytes);
                    break;
                case LONG:
                case DATE:
                    LegacyNumericUtils.longToPrefixCoded(val.longValue(), 0, bytes);
                    break;
                case DOUBLE:
                    LegacyNumericUtils.longToPrefixCoded(NumericUtils.doubleToSortableLong(val.doubleValue()), 0, bytes);
                    break;
                default:
                    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + f.name());
            }

        } else {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid field contents: " + f.name());
        }

        return bytes;
    }

    public Iterator<IndexableField> iterator() {
        //find first pos
        final int len = fieldEnabled.size();
        int i = 0;
        while (i < len && !fieldEnabled.get(i)) i++;
        final int start = i;

        return new Iterator<IndexableField>() {
            int pos = start;
            int offset = 0;

            public boolean hasNext() {
                return pos < len;
            }

            public IndexableField next() {
                IndexableField f = fields.get(pos).get(offset);

                //find next
                if (++offset >= fields.get(pos).size()) {
                    offset = 0;
                    while (++pos < len && !fieldEnabled.get(pos)) ;
                }

                return f;
            }

            public void remove() {
                //cannot remove
            }
        };
    }
}
