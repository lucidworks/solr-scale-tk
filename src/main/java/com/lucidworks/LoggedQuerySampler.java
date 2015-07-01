package com.lucidworks;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.log.Logger;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LoggedQuerySampler extends AbstractJavaSamplerClient implements Serializable {
  private static final long serialVersionUID = 1L;

  static final Map<String,String> escapes = new HashMap<String,String>();
  static {
    escapes.put("<","%3C");
    escapes.put(">","%3E");
    escapes.put("\"","%22");
    escapes.put("{","%7B");
    escapes.put("}","%7D");
    escapes.put("\\", "%5C");
    escapes.put("^", "%5E");
    escapes.put("~", "%7E");
    escapes.put("#", "%23");
    escapes.put("|", "%7C");
  }

  // keeps track of how many tests are running this sampler and when there are
  // none, a final hard commit is sent.
  private static AtomicInteger refCounter = new AtomicInteger(0);
  private static final MetricRegistry metrics = new MetricRegistry();
  private static final com.codahale.metrics.Timer queryTimer = metrics.timer("query");
  private static final com.codahale.metrics.Timer qTimeTimer = metrics.timer("QTime");
  private static final Counter slowCounter = metrics.counter("slowQueries");
  private static final Counter goodCounter = metrics.counter("goodQueries");
  private static final Counter excCounter = metrics.counter("errors");
  private static final Counter waitingCounter = metrics.counter("waitingForResponse");
  private static final Counter zeroResults = metrics.counter("zeroResults");

  private static final Counter server1 = metrics.counter("server1");
  private static final Counter server2 = metrics.counter("server2");

  private static ConsoleReporter reporter = null;
  private static CloudSolrServer cloudSolrServer = null;
  private static Set<String> queries = null;
  private static OutputStreamWriter slowQueryLogger = null;

  protected Logger log;
  protected List<String> servers;
  protected String collection;
  protected long slowQueryThresholdMs = 1500;
  protected XMLResponseParser responseParser = new XMLResponseParser();

  public SampleResult runTest(JavaSamplerContext context) {
    SampleResult result = new SampleResult();
    result.sampleStart();

    log.info("Query test running in thread: " + Thread.currentThread().getName());

    Random rand = new Random();
    List<String> randomizedQueries = new ArrayList<String>(queries.size());
    randomizedQueries.addAll(queries);
    Collections.shuffle(randomizedQueries, rand);

    int numServers = servers.size();

    HttpClient httpClient = cloudSolrServer.getLbServer().getHttpClient();
    int numQueries = 0;
    for (String nextQuery : randomizedQueries) {

      int srvrIndex = numServers > 1 ? rand.nextInt(numServers) : 0;
      String serverUrl = "http://"+servers.get(srvrIndex);

      // TODO: hacky
      if (srvrIndex == 0) {
        server1.inc();
      } else {
        server2.inc();
      }

      String queryUrlBase = serverUrl+"/"+collection+"/select?";

      final com.codahale.metrics.Timer.Context queryTimerCtxt = queryTimer.time();
      long startQ = System.currentTimeMillis();
      String queryUrl = queryUrlBase+nextQuery;
      try {
        waitingCounter.inc();
        int qTime = sendQuery(httpClient, queryUrl);
        qTimeTimer.update(qTime, TimeUnit.MILLISECONDS);

        long diffQ = (System.currentTimeMillis() - startQ);
        if (diffQ > slowQueryThresholdMs) {

          if (slowQueryLogger != null) {
            try {
              slowQueryLogger.write(queryUrl+" : "+diffQ+"\n\n");
              slowQueryLogger.flush();
            } catch (Exception ignore) {
              log.warn("Failed to write slow query ["+queryUrl+"] to slowQueryLogger file due to: "+ignore);
            }
          }

          slowCounter.inc();
        } else {
          goodCounter.inc();
        }
      } catch (Exception exc) {
        log.error("Query to ["+queryUrl+"] failed due to: "+exc);
        excCounter.inc();
      } finally {
        waitingCounter.dec();
        queryTimerCtxt.stop();
      }

      if (++numQueries % 100 == 0)
        log.info(Thread.currentThread().getName()+" has sent "+numQueries+" queries so far ...");
    }

    result.sampleEnd();

    return result;
  }

  protected int sendQuery(HttpClient httpClient, String getUrl) throws Exception {
    int qTime = -1;
    HttpResponse response = httpClient.execute(new HttpGet(getUrl));
    HttpEntity entity = null;
    try {
      entity = response.getEntity();
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode == 200) {
        InputStream instream = null;
        try {
          instream = entity.getContent();
          NamedList<Object> resp =
            responseParser.processResponse(instream, StandardCharsets.UTF_8.name());
          qTime = (Integer)((NamedList<Object>)resp.get("responseHeader")).get("QTime");

          SolrDocumentList sdl = (SolrDocumentList)resp.get("response");
          if (sdl != null && sdl.getNumFound() == 0)
            zeroResults.inc();

        } finally {
          if (instream != null) {
            try {
              instream.close();
            } catch (Exception exc) {}
          }
        }
      } else {
        StringBuilder body = new StringBuilder();
        if (entity != null) {
          InputStream instream = entity.getContent();
          String line;
          try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(instream, StandardCharsets.UTF_8));
            while ((line = reader.readLine()) != null)
              body.append(line);
          } catch (Exception ignore) {
            // squelch it - just trying to compose an error message here
          } finally {
            instream.close();
          }
        }
        String bodyText = body.toString().replaceAll("\\s+", " ");
        String errTxt = response.getStatusLine() + ": " + bodyText;
        throw new Exception(errTxt);
      }
    } finally {
      if (entity != null)
        EntityUtils.consume(entity);
    }
    return qTime;
  }

  @Override
  public Arguments getDefaultParameters() {
    Arguments defaultParameters = new Arguments();
    defaultParameters.addArgument("ZK_HOST", "dmzchozkprd01.snlnet.int:2181,dmzchozkprd03.snlnet.int:2181,dmzchozkprd05.snlnet.int:2181");
    defaultParameters.addArgument("COLLECTION", "all");
    defaultParameters.addArgument("RANDOM_SEED", "5150");
    defaultParameters.addArgument("LOG_DIR", "/tmp/solr_logs");
    defaultParameters.addArgument("SLOW_QUERY_THRESHOLD_MS", "1500");
    defaultParameters.addArgument("SERVERS", "dmzchosrchprd03.snlnet.int:8983/solr,dmzchosrchprd01.snlnet.int:8983/solr");
    return defaultParameters;
  }

  @Override
  public void setupTest(JavaSamplerContext context) {
    super.setupTest(context);

    int myRef = refCounter.incrementAndGet(); // keep track of threads using the statics in this class

    log = getLogger().getChildLogger("QuerySampler");

    log.info("Setting up test for ("+myRef+") "+Thread.currentThread().getName());

    Map<String, String> params = new HashMap<String, String>();
    Iterator<String> paramNames = context.getParameterNamesIterator();
    while (paramNames.hasNext()) {
      String paramName = paramNames.next();
      String param = context.getParameter(paramName);
      if (param != null)
        params.put(paramName, param);
    }

    log.info("Initializing test "+myRef+" with params: " + params);

    slowQueryThresholdMs = Long.parseLong(params.get("SLOW_QUERY_THRESHOLD_MS"));

    collection = params.get("COLLECTION");

    String serverList = params.get("SERVERS");
    servers = Arrays.asList(serverList.split(","));
    if (servers.isEmpty()) {
      throw new IllegalArgumentException("Must provide at least 1 server!");
    }

    synchronized (LoggedQuerySampler.class) {
      if (cloudSolrServer == null) {
        String zkHost = params.get("ZK_HOST");
        getLogger().info("Connecting to SolrCloud using zkHost: " + zkHost);
        cloudSolrServer = new CloudSolrServer(zkHost);
        cloudSolrServer.setDefaultCollection(collection);
        cloudSolrServer.connect();
        getLogger().info("Connected to SolrCloud; collection=" + collection);
      }

      if (reporter == null) {
        reporter = ConsoleReporter.forRegistry(metrics)
          .convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS).build();
        reporter.start(30, TimeUnit.SECONDS);
      }

      if (queries == null) {
        setupSharedQueries(new File(params.get("LOG_DIR")));
      }
    }
  }

  protected void setupSharedQueries(File logsDir) {
    if (!logsDir.isDirectory())
      throw new IllegalStateException(logsDir.getAbsolutePath()+" not found!");

    File[] logFiles = logsDir.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.startsWith("solr.") && (name.endsWith(".log") || name.endsWith(".log.gz"));
      }
    });

    log.info("Scanning files for queries in "+logsDir.getAbsolutePath());

    queries = new HashSet<String>();
    for (File logFile : logFiles) {
      log.info("Scanning "+logFile.getAbsolutePath()+" for top-level queries ...");
      try {
        appendQueriesFromLog(logFile);
      } catch (Exception exc) {
        log.warn("Failed to process all lines in "+logFile.getAbsolutePath()+" due to: "+exc);
      }
    }

    if (queries.size() == 0)
      throw new IllegalStateException("No top-level queries found in logs in "+logsDir.getAbsolutePath());

    log.info("Read " + queries.size() + " queries from logs in " + logsDir.getAbsolutePath());

    OutputStreamWriter osw = null;
    try {
      osw = new OutputStreamWriter(new FileOutputStream("solr_queries.txt"), StandardCharsets.UTF_8);
      int qIdx = 0;
      Iterator<String> i = queries.iterator();
      while (i.hasNext()) {
        // re-create the pattern the log line parser looks for
        osw.write(qIdx+" ["+collection+"_ path=/select params={");
        osw.write(i.next());
        osw.write("}\n");
        ++qIdx;
      }
      osw.flush();
    } catch (Exception exc) {
      log.error("Failed to write Solr queries to solr_queries.txt");
    } finally {
      if (osw != null) {
        try {
          osw.close();
        } catch (Exception ignore){}

      }
    }

    try {
      slowQueryLogger = new OutputStreamWriter(new FileOutputStream("slow_queries.txt", true), StandardCharsets.UTF_8);
    } catch (FileNotFoundException e) {
      log.error("Error opening slow_queries for writing", e);
      slowQueryLogger = null;
    }
  }

  protected void appendQueriesFromLog(File logFile) throws Exception {
    BufferedReader br = null;
    String line;
    try {
      br = new BufferedReader(new InputStreamReader(readLogFile(logFile), StandardCharsets.UTF_8));
      while ((line = br.readLine()) != null) {
        line = line.trim();
        if (line.length() == 0)
          continue;

        line = line.replaceAll("\\s+", " ");

        if (line.indexOf(" path=/select ") == -1)
          continue; // not a query

        if (line.indexOf("distrib=false") != -1)
          continue; // only want top-level distributed queries

        if (line.indexOf(" ["+collection+"_") == -1)
          continue; // not this collection

        int pos = line.indexOf("params={");
        if (pos == -1)
          continue; // no params

        String queryParams = line.substring(pos + 8);
        pos = queryParams.lastIndexOf("}");
        queryParams = queryParams.substring(0, pos);

        for (String ch : escapes.keySet())
          queryParams = queryParams.replace(ch, escapes.get(ch));

        if (queryParams.indexOf("*:*") != -1 || queryParams.indexOf("attr_collection_:*") != -1)
          continue; // skip all docs queries

        queryParams = queryParams.trim();
        if (queryParams.length() > 0) {
          queries.add(queryParams);
        }
      }
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (Exception exc){}
      }
    }
  }

  protected InputStream readLogFile(File logFile) throws Exception {
    if (logFile.getName().endsWith(".gz")) {
      return new GZIPInputStream(new FileInputStream(logFile));
    } else {
      return new FileInputStream(logFile);
    }
  }

  @Override
  public void teardownTest(JavaSamplerContext context) {
    if (cloudSolrServer != null) {
      int refs = refCounter.decrementAndGet();
      if (refs == 0) {

        if (slowQueryLogger != null) {
          try {
            slowQueryLogger.flush();
            slowQueryLogger.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }

        if (reporter != null) {
          reporter.report();
          reporter.stop();
        }

        try {
          cloudSolrServer.shutdown();
        } catch (Exception ignore) {
        }
        cloudSolrServer = null;
        log.info("Shutdown CloudSolrServer.");
      }
    }

    super.teardownTest(context);
  }

}
