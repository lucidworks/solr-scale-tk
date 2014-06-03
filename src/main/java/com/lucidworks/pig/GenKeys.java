package com.lucidworks.pig;

import java.io.IOException;
import java.util.Random;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;

/**
 * A Pig UDF that creates synthetic keys for Solr performance benchmarking.
 */
public class GenKeys extends EvalFunc<DataBag> {

    private BagFactory bagFactory = BagFactory.getInstance();
    private TupleFactory tupleFactory = TupleFactory.getInstance();    
    private int numKeys;
    private Random random;
    private int maxRandom;
    
    public GenKeys(String numKeys, String seed, String maxRandom) {
      this.numKeys = Integer.parseInt(numKeys);
      this.random = new Random(Long.parseLong(seed));
      this.maxRandom = Integer.parseInt(maxRandom);
    }
    
    public DataBag exec(Tuple input) throws IOException {
        DataBag outputBag = bagFactory.newDefaultBag();        
        String idBase = (String)input.get(0);        
        for (int k=0; k < numKeys; k++) {
          String key = idBase+k;
          int key_bucket = random.nextInt(maxRandom);
          Tuple next = tupleFactory.newTuple(2);
          next.set(0, key);
          next.set(1, key_bucket);
          outputBag.add(next);
        }
        return outputBag;
    }
    
    @Override
    public Schema outputSchema(Schema input) {
        String serializedSchema = "keys: bag {t: tuple(key:chararray, key_bucket:int)}";
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
}
