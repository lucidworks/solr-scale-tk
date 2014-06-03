package com.lucidworks.pig;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import com.lucidworks.IndexingSampler;
import com.lucidworks.IndexingSampler.FieldSpec;

import org.apache.solr.common.util.DateUtil;

/**
 * A Pig UDF that creates synthetic documents for Solr performance benchmarking.
 */
public class SyntheticDoc extends EvalFunc<DataBag> {

    private BagFactory bagFactory = BagFactory.getInstance();
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    private IndexingSampler datagen;
    private Random random;
    private int numDocs;
    private FieldSpec[] fields;
    
    public SyntheticDoc(String numDocsPerKey, String seed) {
      datagen = new IndexingSampler();
      datagen.setup(new HashMap<String,String>());
      random = new Random();
      
      fields = datagen.getFields();      
      numDocs = Integer.parseInt(numDocsPerKey);
    }
    
    public DataBag exec(Tuple input) throws IOException {
        DataBag outputBag = bagFactory.newDefaultBag();        
        String idBase = (String)input.get(0);        
        for (int d=0; d < numDocs; d++) {
          String docId = String.format("%s_%d", idBase, d);
          SolrInputDocument doc = datagen.buildSolrInputDocument(docId, random);
          Tuple outputTuple = tupleFactory.newTuple(fields.length + 1);
          outputTuple.set(0, docId);
          int tuplePos = 0;
          for (FieldSpec f : fields)
            outputTuple.set(++tuplePos, toPigValue(doc.getField(f.name)));            
          outputBag.add(outputTuple);
        }
        return outputBag;
    }
    
    @Override
    public Schema outputSchema(Schema input) {
        StringBuilder sb = new StringBuilder();
        for (int f=0; f < fields.length; f++) {
          sb.append(",").append(fields[f].name).append(":").append(toPigType(fields[f].name));
        }
        
        String serializedSchema = "docs: bag {t: tuple(id:chararray"+sb.toString()+")}";
        Schema schema = null;
        try {
            schema = Utils.getSchemaFromString(serializedSchema);
        } catch (Exception e) {         
            if (e instanceof RuntimeException) {                    
                throw (RuntimeException)e;
            } else {
                throw new IllegalStateException(e);
            }
        }
        return schema;
    }
    
    protected Object toPigValue(SolrInputField field) {
      if (field == null)
        return null;
      
      String name = field.getName();
      Object val = field.getValue();
      if (val == null)
        return val;
      
      String suffix = name.substring(name.lastIndexOf("_")+1);      
      if (val instanceof Collection) {
        Collection vals = (Collection)val;
        // TODO: support multi-valued fields
        return null;
      } else {
        if ("tdt".equals(suffix))
          return DateUtil.getThreadLocalDateFormat().format((Date)val);
        else if ("b".equals(suffix))
          return String.valueOf(val);
        else
          return val;
      }                        
    }
    
    protected String toPigType(String name) {
      String suffix = name.substring(name.lastIndexOf("_")+1);
      if ("i".equals(suffix))
        return "int";
      else if ("f".equals(suffix))
        return "float";
      else if ("d".equals(suffix))
        return "double";
      else if ("l".equals(suffix))
        return "long";
      else
        return "chararray";
    }
}
