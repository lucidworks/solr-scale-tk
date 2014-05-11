package com.lucidworks;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.NoHttpResponseException;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.log.Logger;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import sdsu.algorithms.data.Zipf;

public class IndexingSampler extends AbstractJavaSamplerClient implements
    Serializable {
  private static final long serialVersionUID = 1L;

  // keeps track of how many tests are running this sampler and when there are
  // none,
  // a final hard commit is sent.
  private static AtomicInteger refCounter = new AtomicInteger(0);
  
  private static List<String> englishWords = null;

  protected Logger log;
  protected CloudSolrServer cloudSolrServer;
  //protected Random rand;
  protected FieldSpec[] fields;
  protected boolean commitAtEnd = true;

  private static final MetricRegistry metrics = new MetricRegistry();
  private static final Timer sendBatchToSolrTimer = metrics.timer("sendBatchToSolr");
  private static final Timer constructBatch = metrics.timer("constructBatch");
  private static ConsoleReporter reporter = null;
  
  private static long dateBaseMs = 1368045398000l;
  
  private static ThreadLocal<Random> rands = new ThreadLocal<Random>() {
    
    final AtomicInteger inits = new AtomicInteger(0);
    
    @Override
    protected Random initialValue() {
      return new Random(5150+inits.incrementAndGet());
    }    
  };
  
  private static ThreadLocal<Zipf> zipf = new ThreadLocal<Zipf>() {
    @Override
    protected Zipf initialValue() {
      return new Zipf(30000);
    }
  };
  
  @Override
  public Arguments getDefaultParameters() {
    Arguments defaultParameters = new Arguments();
    defaultParameters.addArgument("ZK_HOST", "localhost:2181");
    defaultParameters.addArgument("COLLECTION", "cloud");
    defaultParameters.addArgument("BATCH_SIZE", "100");
    defaultParameters.addArgument("NUM_DOCS_PER_LOOP", "10000");
    defaultParameters.addArgument("THREAD_ID", "${__threadNum}");
    defaultParameters.addArgument("ID_PREFIX", "id-");
    defaultParameters.addArgument("RANDOM_SEED", "5150");
    defaultParameters.addArgument("WORD_LIST", "100K_words_en.txt");
    defaultParameters.addArgument("COMMIT_AT_END", "true");
    return defaultParameters;
  }

  @Override
  public void setupTest(JavaSamplerContext context) {
    super.setupTest(context);

    log = getLogger().getChildLogger("LW-IndexingSampler");
    
    commitAtEnd = "true".equals(context.getParameter("COMMIT_AT_END"));
    
    int ref = refCounter.incrementAndGet();
    
    // setup for data generation
    synchronized (this.getClass()) {
      if (englishWords == null) {
        try {
          englishWords = loadWords(context.getParameter("WORD_LIST"));
        } catch (Exception exc) {
          if (exc instanceof RuntimeException)
            throw (RuntimeException)exc;
          else
            throw new RuntimeException(exc);
        }
        log.info("Loaded "+englishWords.size()+" words from "+context.getParameter("WORD_LIST"));
      }
    }
    
    fields = new FieldSpec[] {
      new FieldSpec("integer1_i", "i:1:100000:u:10"),
      new FieldSpec("integer2_i", "i:1:10000:u:50"),
      //new FieldSpec("integer3_i", "i:1:1000:u:20"),
      new FieldSpec("long1_l", "l:1:10000000:u:10"),
      new FieldSpec("long2_l", "l:1:50000000:u:20"),
      //new FieldSpec("long3_l", "l:1:25000000:u:30"),
      new FieldSpec("float1_f", "f:1:2:u:10"),
      new FieldSpec("float2_f", "f:1:1:u:10"),
      new FieldSpec("double1_d", "d:1:6:u:20"),
      new FieldSpec("double2_d", "d:1:4:u:40"),
      new FieldSpec("timestamp1_tdt", "l:1:31536000:u:0"),
      new FieldSpec("timestamp2_tdt", "l:1:31536000:u:10"),
      new FieldSpec("string1_s", "s:10:20000:u:0"),
      new FieldSpec("string2_s", "s:12:5000:u:0"),
      new FieldSpec("string3_s", "s:4:1000:u:10"),
      //new FieldSpec("string4_ss", "s:10:30000:z:0"),
      new FieldSpec("string5_ss", "s:6:20000:u:30"),
      new FieldSpec("boolean1_b", "i:1:1:u:0"),
      new FieldSpec("boolean2_b", "i:1:1:u:50"),
      new FieldSpec("text1_en", "s:15:10000:u:0", 10),
      //new FieldSpec("text2_en", "s:20:109582:u:0", 30),      
      new FieldSpec("text3_en", "s:8:20000:z:0", 40)      
    };    
    
    String zkHost = context.getParameter("ZK_HOST");
    String collection = context.getParameter("COLLECTION");
    getLogger().info("Connecting to SolrCloud using zkHost: " + zkHost);
    cloudSolrServer = new CloudSolrServer(zkHost);
    cloudSolrServer.setDefaultCollection(collection);
    cloudSolrServer.connect();
    getLogger().info("Connected to SolrCloud; collection=" + collection);

    synchronized (IndexingSampler.class) {
      if (reporter == null) {
        reporter = ConsoleReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS).build();
        reporter.start(1, TimeUnit.MINUTES);
      }
    }
  }
  
  protected List<String> loadWords(String classpathRes) throws Exception {
    InputStream stream = getClass().getClassLoader().getResourceAsStream(classpathRes);
    if (stream == null)
      throw new IllegalArgumentException(classpathRes+" not found on classpath!");      

    List<String> words = null;
    BufferedReader br = null;
    String line = null;
    try {      
      br = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
      words = new ArrayList<String>();
      while ((line = br.readLine()) != null) {
        String word = line.trim();
        if (word.length() > 0)
          words.add(word);
      }      
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (Exception ignore){}
      }
    }
    
    Collections.shuffle(words, rands.get());
    
    return words;
  }

  @Override
  public void teardownTest(JavaSamplerContext context) {
    if (cloudSolrServer != null) {
      int refs = refCounter.decrementAndGet();
      if (refs == 0) {
        if (commitAtEnd) {
          log.info("Sending final commit to SolrCloud.");
          try {
            cloudSolrServer.commit();
          } catch (Exception e) {
            log.error("Failed to commit due to: " + e, e);
          }
        }

        if (reporter != null) {
          reporter.report();
          reporter.stop();
        }
      }

      try {
        cloudSolrServer.shutdown();
      } catch (Exception ignore) {}
      cloudSolrServer = null;
      log.info("Shutdown CloudSolrServer.");
    }

    super.teardownTest(context);
  }

  @Override
  public SampleResult runTest(JavaSamplerContext context) {
    SampleResult result = new SampleResult();
    result.sampleStart();

    String idPrefix = context.getParameter("ID_PREFIX");
    String threadId = context.getParameter("THREAD_ID");
    int batchSize = context.getIntParameter("BATCH_SIZE", 100);
    int numDocsPerLoop = context.getIntParameter("NUM_DOCS_PER_LOOP", 10000);
    if (numDocsPerLoop < batchSize)
      numDocsPerLoop = batchSize; // min is batchSize

    try {
      int totalDocs = index(idPrefix, threadId, numDocsPerLoop, batchSize);
      log.info("Thread " + threadId + " finished sending " + totalDocs + " docs to Solr.");
      result.setSuccessful(true);
    } catch (Exception exc) {
      log.error("Indexing sampler failed due to: " + exc, exc);
      result.setSuccessful(false);
      result.setErrorCount(1);
    }

    result.sampleEnd();
    return result;
  }

  /**
   * Build up a test document.
   */
  protected SolrInputDocument buildSolrInputDocument(String docId, Random rand) {
    SolrInputDocument inDoc = new SolrInputDocument();
    inDoc.setField("id", docId);
    for (FieldSpec f : fields) {
      if (f.name.endsWith("_ss")) {
        int numVals = rand.nextInt(20)+1;
        for (int n=0; n < numVals; n++) {
          Object val = f.next(rand);
          if (val != null) {              
            inDoc.addField(f.name, val);
          }
        }
      } else {
        Object val = f.next(rand);
        if (val != null) {
          inDoc.setField(f.name, val);
        }
      }
    }
    return inDoc;
  }

  protected int index(String idPrefix, String threadId, int numDocsPerLoop, int batchSize) throws Exception {
    log.info(String.format("Starting indexing sampler test with: threadId=%s, batchSize=%d, numDocsPerLoop=%d",
            threadId, batchSize, numDocsPerLoop));

    int totalDocs = 0;
    List<SolrInputDocument> batch = new ArrayList<SolrInputDocument>(batchSize);
    
    Random rand = rands.get();
    Timer.Context constructBatchTimerCtxt = null;    
    for (int d = 0; d < numDocsPerLoop; d++) {
      
      if (constructBatchTimerCtxt == null) {
        constructBatchTimerCtxt = constructBatch.time();
      }
      
      String docId = String.format("%s_%s_%d", idPrefix, threadId, d);
      batch.add(buildSolrInputDocument(docId, rand));

      if (batch.size() >= batchSize) {
        constructBatchTimerCtxt.stop();
        constructBatchTimerCtxt = null; // reset
        
        totalDocs += sendBatch(batch, 10, 3);
        if (totalDocs % 1000 == 0) {
          log.info("Thread " + threadId + " has sent " + totalDocs
              + " docs so far.");
        }
      }
    }

    // last batch
    if (batch.size() > 0) {
      totalDocs += sendBatch(batch, 10, 3);
    }

    return totalDocs;
  }

  protected int sendBatch(List<SolrInputDocument> batch, int waitBeforeRetry, int maxRetries) throws Exception {
    int sent = 0;
    final Timer.Context sendTimerCtxt = sendBatchToSolrTimer.time();
    try {
      cloudSolrServer.add(batch);
      sent = batch.size();
    } catch (Exception exc) {

      Throwable rootCause = SolrException.getRootCause(exc);
      boolean wasCommError = 
          (rootCause instanceof ConnectException || 
           rootCause instanceof ConnectTimeoutException || 
           rootCause instanceof NoHttpResponseException || 
           rootCause instanceof SocketException);

      if (wasCommError) {
        if (--maxRetries > 0) {
          log.warn("ERROR: " + rootCause + " ... Sleeping for "
              + waitBeforeRetry + " seconds before re-try ...");
          Thread.sleep(waitBeforeRetry * 1000L);
          sent = sendBatch(batch, waitBeforeRetry, maxRetries);
        } else {
          log.error("No more retries available! Add batch failed due to: " + rootCause);
          throw exc;
        }
      }
    } finally {
      sendTimerCtxt.stop();
    }

    batch.clear();
    return sent;
  }

  //
  // Borrowed from the Pig DataGenerator
  //

  static enum Datatype {
    INT, LONG, FLOAT, DOUBLE, STRING
  };

  static enum DistributionType {
    UNIFORM, ZIPF
  };

  protected class FieldSpec {
    String name;
    String arg;
    Datatype datatype;
    DistributionType distype;
    int avgsz;
    int card;
    RandomGenerator gen;
    int pctNull;
    Map<Integer, Object> map;
    boolean hasMapFile = false;
    int numWords;
    List<String> words;

    FieldSpec(String fieldName, String arg) {
      this(fieldName, arg, -1);
    }
    
    FieldSpec(String fieldName, String arg, int numWords) {
      this.name = fieldName;
      this.arg = arg;
      this.numWords = numWords;

      String[] parts = arg.split(":");
      if (parts.length != 5 && parts.length != 6)
        throw new IllegalArgumentException("Colspec [" + arg + "] format incorrect");

      switch (parts[0].charAt(0)) {
        case 'i':
          datatype = Datatype.INT;
          break;
        case 'l':
          datatype = Datatype.LONG;
          break;
        case 'f':
          datatype = Datatype.FLOAT;
          break;
        case 'd':
          datatype = Datatype.DOUBLE;
          break;
        case 's':
          datatype = Datatype.STRING;
          break;
        default:
          throw new IllegalArgumentException("Don't know column type " + parts[0].charAt(0));
      }
      
      avgsz = Integer.valueOf(parts[1]);
      card = Integer.valueOf(parts[2]);
      
      switch (parts[3].charAt(0)) {
      
        case 'u': 
          gen = new UniformRandomGenerator(avgsz, card);
          distype = DistributionType.UNIFORM;
          break;
        
        case 'z':
          gen = new ZipfRandomGenerator(avgsz, card);
          distype = DistributionType.ZIPF;
          break;
  
        default:
          throw new IllegalArgumentException("Don't know generator type " + parts[3].charAt(0));
      }

      pctNull = Integer.valueOf(parts[4]);
      if (pctNull < 0 || pctNull > 100)
        throw new IllegalArgumentException("Percentage null must be between 0-100, you gave" + pctNull);
      
      map = new HashMap<Integer, Object>();
    }
    
    public Object next(Random rand) {
      int pct = rand.nextInt(100)+1;
      return (pct > pctNull) ? nextNoNull(rand) : null;
    }
    
    public Object nextNoNull(Random rand) {
      
      this.words = englishWords; // hacky but i'm tired
      
      String suffix = name.split("_")[1];
      if ("en".equals(suffix)) {
        return nextText(rand);
      } else if ("tdt".equals(suffix)) {
        return nextDate(rand);
      } else if ("s".equals(suffix) || "ss".equals(suffix)) {
        return nextString(rand);
      } else if ("b".equals(suffix)) {
        return nextBoolean(rand);
      } else if ("l".equals(suffix)) {
        return nextLong(rand);
      } else if ("i".equals(suffix)) {
        return nextInt(rand);
      } else if ("f".equals(suffix)) {
        return nextFloat(rand);
      } else if ("d".equals(suffix)) {
        return nextDouble(rand);
      } else {
        throw new IllegalArgumentException("Unsupported dynamic field suffix '"+suffix+"'!");
      }
    }
    
    public Boolean nextBoolean(Random rand) {
      return rand.nextBoolean();
    }
    
    public String nextText(Random rand) {
      int numWordsInText = rand.nextInt(numWords)+1;
      StringBuilder sb = new StringBuilder();
      for (int w=0; w < numWordsInText; w++) {
        if (w > 0) sb.append(" ");
        sb.append(nextString(rand));
      }
      return sb.toString();
    }
    
    public Date nextDate(Random rand) {
      return new Date(dateBaseMs + nextLong(rand)*1000);
    }

    public int nextInt(Random rand) {
      return gen.nextInt(map, rand);
    }

    public long nextLong(Random rand) {
      return gen.nextLong(map, rand);
    }

    public double nextDouble(Random rand) {
      return gen.nextDouble(map, rand);
    }

    public float nextFloat(Random rand) {
      return gen.nextFloat(map, rand);
    }

    public String nextString(Random rand) {
      return gen.nextString(rand);
    }
  }
  
  class UniformRandomGenerator extends RandomGenerator {
    int card;

    public UniformRandomGenerator(int a, int c) {
      avgsz = a;
      card = c;
    }

    public int nextInt(Map<Integer, Object> map, Random rand) {
      return rand.nextInt(card);
    }

    public long nextLong(Map<Integer, Object> map, Random rand) {
      return rand.nextLong() % card;
    }

    public float nextFloat(Map<Integer, Object> map, Random rand) {
      int seed = rand.nextInt(card);
      Float f = (Float) map.get(seed);
      if (f == null) {
        if (!hasMapFile) {
          f = randomFloat(rand);
          map.put(seed, f);
        } else {
          throw new IllegalStateException("Number " + seed + " is not found in map file");
        }
      }
      return f;
    }

    public double nextDouble(Map<Integer, Object> map, Random rand) {
      int seed = rand.nextInt(card);
      Double d = (Double) map.get(seed);
      if (d == null) {
        if (!hasMapFile) {
          d = randomDouble(rand);
          map.put(seed, d);
        } else {
          throw new IllegalStateException("Number " + seed + " is not found in map file");
        }
      }
      return d;
    }

    public String nextString(Random rand) {
      int seed = rand.nextInt(card);
      while (seed >= englishWords.size())
        seed = rand.nextInt(card);      
      return englishWords.get(seed); 
    }
  }  

  abstract class RandomGenerator {

    protected int avgsz;
    protected boolean hasMapFile; // indicating whether a map file from
                                  // rand number to the field value is
                                  // pre-defined

    abstract public int nextInt(Map<Integer, Object> map, Random rand);

    abstract public long nextLong(Map<Integer, Object> map, Random rand);

    abstract public float nextFloat(Map<Integer, Object> map, Random rand);

    abstract public double nextDouble(Map<Integer, Object> map, Random rand);

    abstract public String nextString(Random rand);

    public String randomString(Random rand) {
      int var = (int) ((double) avgsz * 0.3);
      StringBuffer sb = new StringBuffer(avgsz + var);
      if (var < 1)
        var = 1;
      int len = rand.nextInt(2 * var) + avgsz - var;
      for (int i = 0; i < len; i++) {
        int n = rand.nextInt(122 - 65) + 65;
        sb.append(Character.toChars(n));
      }
      return sb.toString();
    }

    public float randomFloat(Random rand) {
      return rand.nextFloat() * rand.nextInt();
    }

    public double randomDouble(Random rand) {
      return rand.nextDouble() * rand.nextInt();
    }
  }

  class ZipfRandomGenerator extends RandomGenerator {
    //Zipf z;

    public ZipfRandomGenerator(int a, int c) {
      avgsz = a;
      //z = new Zipf(c);
    }

    // the Zipf library returns a rand number [1..cardinality], so we substract by 1
    // to get [0..cardinality) the randome number returned by zipf library is an 
    // integer, but converted into double
    private double next() {
      return zipf.get().nextElement() - 1;
    }

    public int nextInt(Map<Integer, Object> map, Random rand) {
      return (int) next();
    }

    public long nextLong(Map<Integer, Object> map, Random rand) {
      return (long) next();
    }

    public float nextFloat(Map<Integer, Object> map, Random rand) {
      int seed = (int) next();
      Float d = (Float) map.get(seed);
      if (d == null) {
        if (!hasMapFile) {
          d = randomFloat(rand);
          map.put(seed, d);
        } else {
          throw new IllegalStateException("Number " + seed + " is not found in map file");
        }
      }
      return d;
    }

    public double nextDouble(Map<Integer, Object> map, Random rand) {
      int seed = (int) next();
      Double d = (Double) map.get(seed);
      if (d == null) {
        if (!hasMapFile) {
          d = randomDouble(rand);
          map.put(seed, d);
        } else {
          throw new IllegalStateException("Number " + seed + " is not found in map file");
        }
      }
      return d;
    }

    public String nextString(Random rand) {
      int seed = (int) next();
      while (seed >= englishWords.size())
        seed = (int) next();      
      return englishWords.get(seed);
    }
  }
}
