package com.lucidworks;

import java.io.Serializable;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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

public class IndexingSampler extends AbstractJavaSamplerClient implements Serializable {
  private static final long serialVersionUID = 1L;

  // keeps track of how many tests are running this sampler and when there are none,
  // a final hard commit is sent.
  private static AtomicInteger refCounter = new AtomicInteger(0);
  
  private static final String alpha = "abcdefghijklmnopqrstuvwxyz";
  private static final char[] lower = alpha.toCharArray();
  private static final char[] upper = alpha.toUpperCase().toCharArray();
  private static final char[] digits = "0123456789".toCharArray();
  
  protected Logger log;
  protected CloudSolrServer cloudSolrServer;
  protected Random random;
  
  private static final MetricRegistry metrics = new MetricRegistry();
  private static final Timer sendBatchToSolrTimer = metrics.timer("sendBatchToSolr");
  private static ConsoleReporter reporter = null;
  
  @Override
  public Arguments getDefaultParameters() {
    Arguments defaultParameters = new Arguments();
    defaultParameters.addArgument("ZK_HOST", "localhost:2181");
    defaultParameters.addArgument("COLLECTION", "cloud");
    defaultParameters.addArgument("BATCH_SIZE", "100");
    defaultParameters.addArgument("NUM_DOCS_PER_LOOP", "10000");
    defaultParameters.addArgument("THREAD_ID", "${__threadNum}");
    defaultParameters.addArgument("ID_PREFIX", "id-");
    return defaultParameters;
  }
  
  @Override
  public void setupTest(JavaSamplerContext context) {
    super.setupTest(context);
    
    random = new Random(0);
    
    log = getLogger().getChildLogger("LW-IndexingSampler");
    
    String zkHost = context.getParameter("ZK_HOST");
    String collection = context.getParameter("COLLECTION");
    getLogger().info("Connecting to SolrCloud using zkHost: "+zkHost);
    cloudSolrServer = new CloudSolrServer(zkHost);
    cloudSolrServer.setDefaultCollection(collection);
    cloudSolrServer.connect();
    getLogger().info("Connected to SolrCloud; collection="+collection);
    
    refCounter.incrementAndGet();
    
    synchronized (IndexingSampler.class) {
      if (reporter == null) {
        reporter = ConsoleReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
        reporter.start(1, TimeUnit.MINUTES);             
        System.out.println(">> Started console reporter!");
      }
    }
  }
  
  @Override
  public void teardownTest(JavaSamplerContext context) {
    if (cloudSolrServer != null) {
      int refs = refCounter.decrementAndGet();
      if (refs == 0) {
        log.info("Sending final commit to SolrCloud.");
        try {
          cloudSolrServer.commit();
        } catch (Exception e) {
          log.error("Failed to commit due to: "+e, e);
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
    int batchSize = context.getIntParameter("BATCH_SIZE",100);
    int numDocsPerLoop = context.getIntParameter("NUM_DOCS_PER_LOOP",10000);
    if (numDocsPerLoop < batchSize) 
      numDocsPerLoop = batchSize; // min is batchSize
    
    try {
      int totalDocs = index(idPrefix, threadId, numDocsPerLoop, batchSize);
      log.info("Thread "+threadId+" finished sending "+totalDocs+" docs to Solr.");
      result.setSuccessful(true);
    } catch (Exception exc) {
      log.error("Indexing sampler failed due to: "+exc, exc);
      result.setSuccessful(false);
      result.setErrorCount(1);
    }
     
    result.sampleEnd();
    return result;
  }
  
  /**
   * TODO: Build up a test document. 
   */
  protected SolrInputDocument buildSolrInputDocument(String docId) {
    SolrInputDocument inDoc = new SolrInputDocument();
    inDoc.setField("id", docId);
    inDoc.setField("string_s", randomWord()); // random words between 2 and 20 chars long
    return inDoc;
  }
  
  protected String randomWord() {
    int len = random.nextInt(18) + 2;
    StringBuilder sb = new StringBuilder();    
    while (sb.length() <= len) {
      if (random.nextBoolean())
        sb.append(randomChar(lower));
      if (random.nextBoolean())
        sb.append(randomChar(upper));
      if (random.nextBoolean())
        sb.append(randomChar(digits));
    }
    return sb.toString();
  }
  
  protected char randomChar(char[] pool) {
    return pool[random.nextInt(pool.length)];
  }
  
  protected int index(String idPrefix, String threadId, int numDocsPerLoop, int batchSize) throws Exception {
    log.info(String.format("Starting indexing sampler test with: threadId=%s, batchSize=%d, numDocsPerLoop=%d", 
        threadId, batchSize, numDocsPerLoop));
        
    int totalDocs = 0;    
    List<SolrInputDocument> batch = new ArrayList<SolrInputDocument>(batchSize);
    for (int d = 0; d < numDocsPerLoop; d++) {          
      String randomUser = "u"+random.nextInt(1000);
      String docId = String.format("%s_%s!%s!%d", idPrefix, threadId, randomUser, d);
      batch.add(buildSolrInputDocument(docId));

      if (batch.size() >= batchSize) {
        totalDocs += sendBatch(batch, 10, 3);                  
        if (totalDocs % 1000 == 0) {
          log.info("Thread "+threadId+" has sent "+totalDocs+" docs so far.");
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
          log.warn("ERROR: "+rootCause+" ... Sleeping for "+waitBeforeRetry+" seconds before re-try ...");
          Thread.sleep(waitBeforeRetry*1000L);
          sent = sendBatch(batch, waitBeforeRetry, maxRetries);
        } else {
          log.error("No more retries available! Add batch failed due to: "+rootCause);
          throw exc;
        }
      }      
    } finally {
      sendTimerCtxt.stop();      
    }
    
    batch.clear();    
    return sent;
  }
}
