package au.org.ala.biocache.index.lucene;


import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.io.LegacyShapeReadWriterFormat;
import com.spatial4j.core.shape.Shape;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.solr.schema.*;

import java.util.*;

/**
 * A lucene document that recycles it's own fields.
 *
 */
public class RecycleDoc implements Iterable<IndexableField> {
    public static final Logger logger = Logger.getLogger(RecycleDoc.class);

    List<Boolean> fieldEnabled = new ArrayList<Boolean>();
    List<SchemaField> schemaFields = new ArrayList<SchemaField>();
    List<IndexableField> fields = new ArrayList<IndexableField>();

    Map<String, List<Integer>> fieldOrder = new HashMap<String, List<Integer>>();
    int size = 0;

    IndexSchema schema;

    public RecycleDoc(IndexSchema schema) {
        this.schema = schema;
    }

    public void add(SchemaField schemaField, IndexableField field) {
        List<Integer> list = fieldOrder.get(field.name());
        if (list == null) {
            list = new ArrayList<Integer>();
            fieldOrder.put(field.name(), list);
        }
        list.add(fields.size());

        schemaFields.add(schemaField);
        fieldEnabled.add(true);
        fields.add(field);

        size ++;
    }

    public void reset() {
        for (int i=fieldEnabled.size() - 1;i >= 0;i--) {
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
                        s[pos++] = fields.get(i).stringValue();
                }
                return s;
            }
        }

        return null;
    }

    /**
     * Attempts to reuse an existing field for the new value.
     *
     * @param name field name
     * @param value value to set the field if reuse is possible
     * @return false if reuse failed and a new field must be created. true if reuse succeeded and a new field
     * is not required.
     */
    public boolean setField(String name, Object value) {

        boolean found = false;

        List<Integer> idx = fieldOrder.get(name);

        if (idx == null) return false;

        int count = 0;
        while(count < idx.size() && fieldEnabled.get(idx.get(count))) count++;

        if (count < idx.size()) {
            int i = idx.get(count);
            SchemaField sf = schemaFields.get(i);
            org.apache.solr.schema.FieldType ft = sf.getType();
            IndexableField f = fields.get(i);

            //Set all data types that are in use
            try {
                if (ft instanceof StrField) {
                    ((Field) f).setStringValue(String.valueOf(value)); found=true;
                } else if (ft instanceof TrieField) {
                    switch (((TrieField) ft).getType().ordinal()) {
                        case 0:
                            ((Field) f).setIntValue(value instanceof Number ? ((Number) value).intValue() : Integer.parseInt((String) value)); found=true;
                            break;
                        case 1:
                            ((Field) f).setLongValue(value instanceof Number ? ((Number) value).longValue() : Long.parseLong((String) value)); found=true;
                            break;
                        case 2:
                            ((Field) f).setFloatValue(value instanceof Number ? ((Number) value).floatValue() : Float.parseFloat((String) value)); found=true;
                            break;
                        case 3:
                            ((Field) f).setDoubleValue(value instanceof Number ? ((Number) value).doubleValue() : Double.parseDouble((String) value)); found=true;
                            break;
                        case 4:
                            ((Field) f).setLongValue(value instanceof Date ? ((Date)value).getTime():((DateField) ft).parseMath(null, (String) value).getTime()); found=true;
                            break;
                        default:
                    }
                } else if (ft instanceof TextField) {
                    ((Field) f).setStringValue((String) value); found=true;
                } else if (ft instanceof TrieDateField) {
                    ((Field) f).setLongValue(value instanceof Date ? ((Date)value).getTime():((DateField) ft).parseMath(null, (String) value).getTime()); found=true;
                } else if (ft instanceof SpatialTermQueryPrefixTreeFieldType ||
                        ft instanceof SpatialRecursivePrefixTreeFieldType) {

                    PrefixTreeStrategy strategy = (ft instanceof SpatialTermQueryPrefixTreeFieldType) ?
                            ((SpatialTermQueryPrefixTreeFieldType)ft).getStrategy(name) :
                            ((SpatialRecursivePrefixTreeFieldType)ft).getStrategy(name);
                    Double distErrPct = strategy.getDistErrPct();
                    SpatialPrefixTree grid = strategy.getGrid();
                    SpatialContext ctx = strategy.getSpatialContext();

                    String str = String.valueOf(value);
                    Shape shape = null;
                    if(value instanceof Shape) {
                        shape = (Shape)value;
                    } else {
                        shape = LegacyShapeReadWriterFormat.readShapeOrNull(str, ctx);
                        shape = shape != null?shape:ctx.readShapeFromWkt(value.toString());
                    }

                    if(shape != null) {
                        if(sf.indexed()) {
                            double distErr = SpatialArgs.calcDistanceFromErrPct(shape, distErrPct, ctx);
                            int detailLevel = grid.getLevelForDistance(distErr);
                            List cells = grid.getCells(shape, detailLevel, true, /*this.simplifyIndexedCells*/ false);
                            ((Field) f).setTokenStream(new SpatialTokenStream(cells.iterator()));

                            fieldEnabled.set(idx.get(count), true);

                            //interate
                            count++;
                        }

                        if(sf.stored()) {
                            //use next field if also indexed
                            if (count < idx.size()) {
                                i = idx.get(count);
                                sf = schemaFields.get(i);
                                ft = sf.getType();
                                f = fields.get(i);
                            }
                            ((StoredField) f).setStringValue(String.valueOf(str));
                        }

                        found = true;
                    }
                } else {
                    logger.error("MISSING FIELD " + name + " = " + value.toString() + ", " + ft.getClass().getName());
                }
            } catch (Exception e) {
                logger.error("FIELD EXCEPTION " + name + " = " + value.toString() + ", " + ft.getClass().getName() + " : " + e.getMessage(), e);
            }
        }
        if (found) {
            fieldEnabled.set(idx.get(count), true);
        }
        return found;
    }

    //source: org.apache.lucene.spatial.prefix.PrefixTreeStrategy.CellTokenStream
    static class SpatialTokenStream extends TokenStream {
        private final CharTermAttribute termAtt = (CharTermAttribute)this.addAttribute(CharTermAttribute.class);
        private Iterator<Cell> iter = null;
        CharSequence nextTokenStringNeedingLeaf = null;

        public SpatialTokenStream(Iterator<Cell> tokens) {
            this.iter = tokens;
        }

        public boolean incrementToken() {
            this.clearAttributes();
            if(this.nextTokenStringNeedingLeaf != null) {
                this.termAtt.append(this.nextTokenStringNeedingLeaf);
                this.termAtt.append('+');
                this.nextTokenStringNeedingLeaf = null;
                return true;
            } else if(this.iter.hasNext()) {
                Cell cell = (Cell)this.iter.next();
                String token = cell.getTokenString();
                this.termAtt.append(token);
                if(cell.isLeaf()) {
                    this.nextTokenStringNeedingLeaf = token;
                }

                return true;
            } else {
                return false;
            }
        }
    }

    public Iterator<IndexableField> iterator() {
        //find first pos
        final int len = fieldEnabled.size();
        int i=0;
        while (i < len && !fieldEnabled.get(i)) i++;
        final int start = i;

        return new Iterator<IndexableField>() {
            int pos = start;

            public boolean hasNext() {
                return pos < len;
            }

            public IndexableField next() {
                IndexableField f = fields.get(pos);

                //find next
                while (++pos < len && !fieldEnabled.get(pos));

                return f;
            }

            public void remove() {
                //cannot remove
            }
        };
    }
}
