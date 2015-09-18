package com.lucidworks;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.lucidworks.client.FusionPipelineClient;
import org.apache.http.NoHttpResponseException;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.log.Logger;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import org.apache.solr.common.params.ModifiableSolrParams;
import sdsu.algorithms.data.Zipf;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class IndexingSampler extends AbstractJavaSamplerClient implements Serializable {
  private static final long serialVersionUID = 1L;

  static {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

  private static final SimpleDateFormat ISO_8601_DATE_FMT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'.'S'Z'");

  // keeps track of how many tests are running this sampler and when there are
  // none, a final hard commit is sent.
  private static AtomicInteger refCounter = new AtomicInteger(0);

  private static List<String> englishWords = null;

  protected Logger log;
  protected CloudSolrClient cloudSolrServer;
  //protected Random rand;
  protected FieldSpec[] fields;
  protected boolean commitAtEnd = true;
  protected FusionPipelineClient indexPipelineClient;

  private static final MetricRegistry metrics = new MetricRegistry();
  private static final Timer sendBatchToSolrTimer = metrics.timer("sendBatchToSolr");
  private static final Timer constructDocsTimer = metrics.timer("constructDocsTimer");
  private static ConsoleReporter reporter = null;

  private static long dateBaseMs = 1368045398000l;

  private static AtomicInteger joinKeyAI = null;

  public static ThreadLocal<Random> rands = new ThreadLocal<Random>() {

    final AtomicInteger inits = new AtomicInteger(0);

    @Override
    protected Random initialValue() {
      return new Random(5150 + inits.incrementAndGet());
    }
  };

  private static ThreadLocal<Zipf> zipf = new ThreadLocal<Zipf>() {
    @Override
    protected Zipf initialValue() {
      return new Zipf(30000);
    }
  };

  public IndexingSampler() {
    fields = new FieldSpec[]{
      new FieldSpec("integer1_i", "i:1:100000:u:10"),
      new FieldSpec("integer2_i", "i:1:10000:u:50"),
      new FieldSpec("long1_l", "l:1:10000000:u:10"),
      new FieldSpec("long2_l", "l:1:50000000:u:20"),
      new FieldSpec("float1_f", "f:1:2:u:10"),
      new FieldSpec("float2_f", "f:1:1:u:10"),
      new FieldSpec("double1_d", "d:1:6:u:20"),
      new FieldSpec("double2_d", "d:1:4:u:40"),
      new FieldSpec("timestamp1_tdt", "l:1:31536000:u:0"),
      new FieldSpec("timestamp2_tdt", "l:1:31536000:u:10"),
      new FieldSpec("string1_s", "s:10:20000:u:0"),
      new FieldSpec("string2_s", "s:12:5000:u:0"),
      new FieldSpec("string3_s", "s:4:1000:u:10"),
      new FieldSpec("boolean1_b", "i:1:1:u:0"),
      new FieldSpec("boolean2_b", "i:1:1:u:50"),
      new FieldSpec("text1_en", "s:15:20000:z:0", 20),
      new FieldSpec("text2_en", "s:20:100000:z:0", 30),
      new FieldSpec("text3_en", "s:8:30000:z:0", 80)
    };

    initializeWords("100K_words_en.txt");
  }

  protected synchronized void initializeWords(String wordListResource) {
    if (englishWords == null) {
      try {
        englishWords = loadWords(wordListResource);
      } catch (Exception exc) {
        if (exc instanceof RuntimeException)
          throw (RuntimeException) exc;
        else
          throw new RuntimeException(exc);
      }
    }
  }

  public FieldSpec[] getFields() {
    return fields;
  }

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
    defaultParameters.addArgument("FUSION_INDEX_PIPELINE",
      "http://localhost:8765/lucid/api/v1/index-pipelines/conn_logging/collections/${collection}/index");
    defaultParameters.addArgument("ENDPOINT_TYPE", "fusion");
    defaultParameters.addArgument("QUEUE_SIZE", "5000");
    return defaultParameters;
  }

  @Override
  public void setupTest(JavaSamplerContext context) {
    super.setupTest(context);

    log = getLogger().getChildLogger("LW-IndexingSampler");

    Map<String, String> params = new HashMap<String, String>();
    Iterator<String> paramNames = context.getParameterNamesIterator();
    while (paramNames.hasNext()) {
      String paramName = paramNames.next();
      String param = context.getParameter(paramName);
      if (param != null)
        params.put(paramName, param);
    }
    setup(params);

    synchronized (IndexingSampler.class) {

      if (joinKeyAI == null)
        joinKeyAI = new AtomicInteger(0);

      if (reporter == null) {
        reporter = ConsoleReporter.forRegistry(metrics)
          .convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS).build();
        reporter.start(1, TimeUnit.MINUTES);
      }
    }
  }

  public void setup(Map<String, String> params) {
    commitAtEnd = "true".equals(params.get("COMMIT_AT_END"));

    // setup for data generation
    String wordListResource = params.get("WORD_LIST");
    if (wordListResource == null)
      wordListResource = "100K_words_en.txt";
    initializeWords(wordListResource);

    String type = params.get("ENDPOINT_TYPE");
    String collection = params.get("COLLECTION");
    if ("solrcloud".equals(type)) {
      String zkHost = params.get("ZK_HOST");
      if (zkHost == null || zkHost.trim().length() == 0)
        throw new IllegalArgumentException("ZK_HOST is required when using ENDPOINT_TYPE=" + type);

      getLogger().info("Connecting to SolrCloud using zkHost: " + zkHost);
      cloudSolrServer = new CloudSolrClient(zkHost);
      cloudSolrServer.setDefaultCollection(collection);
      cloudSolrServer.connect();
      getLogger().info("Connected to SolrCloud; collection=" + collection);
    } else if ("fusion".equals(type)) {
      String fusionIndexPipelineEndpoint = params.get("FUSION_INDEX_PIPELINE");
      if (fusionIndexPipelineEndpoint == null || fusionIndexPipelineEndpoint.trim().length() == 0)
        throw new IllegalArgumentException("FUSION_INDEX_PIPELINE is required when using ENDPOINT_TYPE=" + type);

      // add on the collection part
      fusionIndexPipelineEndpoint = fusionIndexPipelineEndpoint.replace("${collection}", collection);
      try {
        indexPipelineClient = new FusionPipelineClient(fusionIndexPipelineEndpoint);
      } catch (Exception exc) {
        throw new RuntimeException(exc);
      }
    } else if ("datagenonly".equals(type)) {
      // ok
    } else {
      throw new IllegalArgumentException(type + " not supported!");
    }

    refCounter.incrementAndGet();
  }

  protected List<String> loadWords(String classpathRes) throws Exception {
    InputStream stream = getClass().getClassLoader().getResourceAsStream(classpathRes);
    if (stream == null)
      throw new IllegalArgumentException(classpathRes + " not found on classpath!");

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
        } catch (Exception ignore) {
        }
      }
    }

    Collections.shuffle(words, rands.get());

    return words;
  }

  @Override
  public void teardownTest(JavaSamplerContext context) {
    int refs = refCounter.decrementAndGet();

    if (refs <= 0) {
      joinKeyAI.set(0);

      if (commitAtEnd && cloudSolrServer != null) {
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

    if (cloudSolrServer != null) {
      try {
        cloudSolrServer.shutdown();
      } catch (Exception ignore) {
      }
      cloudSolrServer = null;
      log.info("Shutdown CloudSolrClient.");
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
    int numDocsPerThread = context.getIntParameter("NUM_DOCS_PER_LOOP", 10000);
    if (numDocsPerThread < batchSize)
      numDocsPerThread = batchSize; // min is batchSize
    int queueSize = context.getIntParameter("QUEUE_SIZE", 5000);

    int totalDocs = 0;
    try {
      if (cloudSolrServer != null) {
        totalDocs = indexSolrDocument(idPrefix, threadId, numDocsPerThread, queueSize, batchSize);
      } else {
        totalDocs = indexToPipeline(idPrefix, threadId, numDocsPerThread, batchSize);
      }
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
  public SolrInputDocument buildSolrInputDocument(String docId, Random rand) {
    SolrInputDocument inDoc = new SolrInputDocument();
    inDoc.setField("id", docId);
    for (FieldSpec f : fields) {
      if (f.name.endsWith("_ss")) {
        int numVals = rand.nextInt(20) + 1;
        for (int n = 0; n < numVals; n++) {
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

    inDoc.setField("indexed_at_tdt", new Date());
    //inDoc.setField("joinkey_s", joinKeyAI.incrementAndGet());

    return inDoc;
  }

  protected int indexToPipeline(String idPrefix, String threadId, int numDocsPerThread, int batchSize) throws Exception {
    int totalDocs = 0;
    List batch = new ArrayList();

    Random rand = rands.get();
    Timer.Context constructBatchTimerCtxt = null;
    for (int d = 0; d < numDocsPerThread; d++) {

      if (constructBatchTimerCtxt == null) {
        constructBatchTimerCtxt = constructDocsTimer.time();
      }

      String docId = String.format("%s_%s_%d", idPrefix, threadId, d);
      Map<String,Object> nextDoc = buildJsonInputDocument(docId, rand);
      batch.add(nextDoc);

      if (batch.size() >= batchSize) {
        constructBatchTimerCtxt.stop();
        constructBatchTimerCtxt = null; // reset

        totalDocs += sendJsonBatch(batch, 10, 3);
        if (totalDocs % 1000 == 0) {
          log.info("Thread " + threadId + " has sent " + totalDocs
            + " docs so far.");
        }
      }
    }

    // last batch
    if (batch.size() > 0) {
      totalDocs += sendJsonBatch(batch, 10, 3);
    }

    return totalDocs;
  }

  public Map<String,Object> buildJsonInputDocument(String docId, Random rand) {
    Map<String,Object> doc = new HashMap<String,Object>();
    for (FieldSpec f : fields) {
      if (f.name.endsWith("_ss")) {
        int numVals = rand.nextInt(20) + 1;
        List vals = new ArrayList();
        for (int n = 0; n < numVals; n++) {
          Object val = f.next(rand);
          if (val != null) {
            vals.add(val);
          }
        }
        doc.put(f.name, vals);
      } else {
        Object val = f.next(rand);
        if (val != null) {
          if (val instanceof Date) {
            doc.put(f.name, ISO_8601_DATE_FMT.format((Date) val));
          } else {
            doc.put(f.name, val);
          }
        }
      }
    }

    doc.put("indexed_at_tdt", ISO_8601_DATE_FMT.format(new Date()));

    return doc;
  }

  // This class sends batches to Solr while the outer (main thread) generates documents
  class SendToSolrThread extends Thread {

    IndexingSampler sampler;
    int batchSize;
    BlockingQueue<SolrInputDocument> queue;
    int sent = 0;

    SendToSolrThread(String threadId, IndexingSampler sampler, BlockingQueue<SolrInputDocument> queue, int batchSize) {
      super("SentToSolrThread-" + threadId);
      this.setPriority(Thread.MAX_PRIORITY);
      this.queue = queue;
      this.sampler = sampler;
      this.batchSize = batchSize;
    }

    public int sent() {
      return sent;
    }

    @Override
    public void run() {
      log.info(getName() + " is running.");
      List<SolrInputDocument> batch = new ArrayList<SolrInputDocument>(batchSize);
      SolrInputDocument doc = null;
      try {
        doc = queue.poll();
        while (doc != null) {
          batch.add(doc);

          if (batch.size() >= batchSize)
            sent += sampler.sendBatch(batch, 10, 3);

          doc = queue.poll();
          if (doc == null) {
            // safety measure to allow more docs to come-in
            Thread.sleep(500);
            doc = queue.poll();
          }
        }

        // last batch
        if (batch.size() > 0)
          sent += sendBatch(batch, 10, 3);

      } catch (Exception e) {
        if (e instanceof RuntimeException) {
          throw (RuntimeException) e;
        } else {
          throw new RuntimeException(e);
        }
      }
    }

    public void blockUntilFinished() {
      while (!queue.isEmpty()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.interrupted();
          break;
        }
      }
    }
  }

  protected int indexSolrDocument(String idPrefix, String threadId, int numDocsToSend, int queueSize, int batchSize) throws Exception {
    log.info(String.format("Starting indexing sampler test with: threadId=%s, batchSize=%d, numDocsToSend=%d, queueSize=%d",
      threadId, batchSize, numDocsToSend, queueSize));

    // safe-guard to ensure the Send thread doesn't block forever waiting for more docs ...
    if (queueSize > numDocsToSend)
      queueSize = numDocsToSend;

    // at about 90% full, we'll put the create threads to sleep
    int highWaterMark = Math.round(queueSize * 0.65f);
    int lowWaterMark = Math.min(Math.round(queueSize * 0.2f), batchSize * 3);
    int nBatches = batchSize * 8;

    // start constructing and queuing up docs, but don't start sending until the queue has some docs buffered up
    BlockingQueue<SolrInputDocument> queue = new LinkedBlockingQueue<SolrInputDocument>(queueSize);
    SendToSolrThread sendThread = null;
    int totalDocs = 0;
    Random rand = rands.get();
    for (int d = 0; d < numDocsToSend; d++) {
      String docId = String.format("%s_%s_%d", idPrefix, threadId, d);
      queue.offer(buildSolrInputDocument(docId, rand));

      if (++totalDocs % nBatches == 0) {
        log.info("Thread " + threadId + " has queued " + totalDocs +
          " docs and sent " + (sendThread != null ? sendThread.sent() : 0) +
          "; queue.size=" + queue.size());
      }

      if (queue.size() > highWaterMark) {
        if (sendThread != null) {
          // queue is pretty full ... let this thread sleep a little to allow the sender to get caught up
          while (queue.size() > lowWaterMark) {
            try {
              Thread.sleep(500);
            } catch (InterruptedException ie) {
            }
          }
        } else {
          // queue is almost full, start sending documents
          sendThread = new SendToSolrThread(threadId, this, queue, batchSize);
          sendThread.start();
        }
      }
    }

    if (sendThread != null) {
      log.info("Constructed " + numDocsToSend + " docs; waiting for queue to empty, current size is: " + queue.size());
      sendThread.blockUntilFinished();
      log.info("Queue drained ... thread " + threadId + " is done sending " + totalDocs + " docs");
    }

    return totalDocs;
  }

  protected int sendJsonBatch(List batch, int waitBeforeRetry, int maxRetries) throws Exception {
    int sent = 0;
    final Timer.Context sendTimerCtxt = sendBatchToSolrTimer.time();
    try {
      indexPipelineClient.postBatchToPipeline(batch);
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
          sent = sendJsonBatch(batch, waitBeforeRetry, maxRetries);
        } else {
          log.error("No more retries available! Add batch failed due to: " + rootCause);
          throw exc;
        }
      } else {
        throw exc;
      }
    } finally {
      sendTimerCtxt.stop();
    }

    batch.clear();
    return sent;
  }

  protected int sendBatch(List<SolrInputDocument> batch, int waitBeforeRetry, int maxRetries) throws Exception {
    int sent = 0;
    final Timer.Context sendTimerCtxt = sendBatchToSolrTimer.time();
    try {
      UpdateRequest updateRequest = new UpdateRequest();
      ModifiableSolrParams params = updateRequest.getParams();
      if (params == null) {
        params = new ModifiableSolrParams();
        updateRequest.setParams(params);
      }

      if (batch.size() == 1) {
        updateRequest.add(batch.get(0));
      } else {
        updateRequest.add(batch);
      }

      cloudSolrServer.request(updateRequest);
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

  public static enum Datatype {
    INT, LONG, FLOAT, DOUBLE, STRING
  }

  ;

  static enum DistributionType {
    UNIFORM, ZIPF
  }

  ;

  public static class FieldSpec {
    public String name;
    String arg;
    public Datatype datatype;
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
      int pct = rand.nextInt(100) + 1;
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
        throw new IllegalArgumentException("Unsupported dynamic field suffix '" + suffix + "'!");
      }
    }

    public Boolean nextBoolean(Random rand) {
      return rand.nextBoolean();
    }

    public String nextText(Random rand) {
      int numWordsInText = rand.nextInt(numWords) + 1;
      StringBuilder sb = new StringBuilder();
      for (int w = 0; w < numWordsInText; w++) {
        if (w > 0) sb.append(" ");
        sb.append(nextString(rand));
      }
      return sb.toString();
    }

    public Date nextDate(Random rand) {
      return new Date(dateBaseMs + nextLong(rand) * 1000);
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

  static class UniformRandomGenerator extends RandomGenerator {
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

  static abstract class RandomGenerator {

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

  static class ZipfRandomGenerator extends RandomGenerator {
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
