package com.lucidworks;

import com.codahale.metrics.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.TermsResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class QuerySampler extends AbstractJavaSamplerClient implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final Log log = LogFactory.getLog(QuerySampler.class);

  // keeps track of how many tests are running this sampler and when there are
  // none, a final hard commit is sent.
  private static AtomicInteger refCounter = new AtomicInteger(0);
  private static final MetricRegistry metrics = new MetricRegistry();
  private static final com.codahale.metrics.Timer queryTimer = metrics.timer("queryRoundTrip");
  private static final com.codahale.metrics.Timer qTimeTimer = metrics.timer("QTime");
  private static final com.codahale.metrics.Counter noResultsCounter = metrics.counter("noresults");
  private static final com.codahale.metrics.Counter excCounter = metrics.counter("exceptions");
  private static final Counter slowCounter = metrics.counter("slowQueries");

  private static ConsoleReporter reporter = null;
  private static CloudSolrClient cloudSolrClient = null;
  private static Map<String, List<String>> termsDict = null;
  private static FusionPipelineClient fusionPipelineClient = null;
  private static String fusionQueryPipelinePath;
  private static String fusionHostList;
  private static boolean useFusion = false;

  private static long minDate;
  private static long maxDate;
  private static Integer diffSecs = null;
  private static long slowQueryThresholdMs = 150;
  private static OutputStreamWriter slowQueryLogger = null;
  private static OutputStreamWriter noResultsQueryLogger = null;
  private static Long randomSeed = null;
  private static Map<String,FieldStatsInfo> numericFieldStats = null;
  private static boolean setupOk = false;

  public static ThreadLocal<Random> rands = new ThreadLocal<Random>() {

    private AtomicInteger incr = new AtomicInteger(0);

    @Override
    protected Random initialValue() {
      int offset = incr.incrementAndGet() % 3;
      return new Random(randomSeed+offset);
    }
  };

  private static ThreadLocal<SimpleDateFormat> df = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    }
  };

  public SampleResult runTest(JavaSamplerContext context) {

    if (!setupOk)
      throw new IllegalStateException("Setup failed! Test cannot be executed.");

    SampleResult result = new SampleResult();
    result.sampleStart();

    SolrQuery query = new SolrQuery();

    Random random = rands.get();

    StringBuilder randomQuery = new StringBuilder();
    if (random.nextBoolean())
      randomQuery.append(getTermClause("string1_s")).append(" ");
    if (random.nextBoolean())
      randomQuery.append(getTermClause("string2_s")).append(" ");
    if (random.nextBoolean())
      randomQuery.append(getTermClause("text1_txt_en")).append(" ");
    if (random.nextBoolean())
      randomQuery.append(getTermClause("text3_txt_en")).append(" ");

    if (random.nextBoolean())
      randomQuery.append("boolean1_b:true").append(" ");

    String qs = randomQuery.toString().trim();
    if (qs.length() == 0)
      qs = "*:*";

    query.setQuery(qs);

    if (random.nextBoolean()) {
      if (random.nextInt(10) < 4)
        addIntRangeFilter(query, "integer1_i");

      if (random.nextInt(5) == 3)
        addIntRangeFilter(query, "integer2_i");

      if (random.nextInt(10) == 5)
        addLongRangeFilter(query, "long1_l");

      if (random.nextInt(20) == 10)
        addLongRangeFilter(query, "long2_l");

      if (random.nextInt(7) <= 2)
        query.addFilterQuery(getDateRangeFilter("timestamp1_tdt"));

      if (random.nextInt(10) == 4)
        query.addFilterQuery(getDateRangeFilter("timestamp2_tdt"));

      if (random.nextInt(40) <= 3) {
        query.addFilterQuery("double1_d:[* TO *]");
      }

      if (random.nextInt(50) <= 3) {
        query.addFilterQuery("double2_d:[* TO *]");
      }

      if (random.nextInt(100) == 0) {
        query.addFilterQuery("-id:[* TO *]");
      }
    }

    if (random.nextInt(10) <= 2) {
      query.setFields("id","text3_txt_en");
    }

    if (random.nextInt(5) < 3) {
      query.setFacet(true);
      query.setFacetMinCount(1);
      query.addFacetField("string1_s");
    }

    if (random.nextInt(10) < 3) {
      query.setFacet(true);
      query.setFacetMinCount(1);
      query.addFacetField("string2_s");
    }

    if (random.nextInt(20) < 3) {
      query.setFacet(true);
      query.setFacetMinCount(1);
      query.addFacetField("integer1_i");
    }

    if (random.nextInt(10) < 2)
      query.setRows(20);
    else
      query.setRows(10);

    // sort by date
    if (random.nextInt(10) < 4)
      query.addSort("timestamp1_tdt", SolrQuery.ORDER.desc);

    if (random.nextInt(10) < 4)
      query.addSort("integer1_i", SolrQuery.ORDER.asc);

    if (random.nextInt(10) <= 7) {
      query.setStart(0);
    } else {
      query.setStart(10);
    }

    executeQuery(query, result);

    result.sampleEnd();

    return result;
  }

  protected void executeQuery(SolrQuery query, SampleResult result) {
    final com.codahale.metrics.Timer.Context queryTimerCtxt = queryTimer.time();
    long qTime = 0;
    boolean timerStopped = false;
    try {
      QueryResponse qr = useFusion ? fusionPipelineClient.queryFusion(fusionQueryPipelinePath, query) : cloudSolrClient.query(query);
      qTime = queryTimerCtxt.stop();
      timerStopped = true;
      qTimeTimer.update(qr.getQTime(), TimeUnit.MILLISECONDS);

      if (qr.getResults().getNumFound() == 0) {
        noResultsCounter.inc();

        if (noResultsQueryLogger != null) {
          synchronized (noResultsQueryLogger) {
            try {
              noResultsQueryLogger.write(query+" : "+qTime+"\n");
              noResultsQueryLogger.flush();
            } catch (Exception ignore) {
              log.warn("Failed to write no results query ["+query+"] to the log file due to: "+ignore);
            }
          }
        }
      }

      result.setResponseOK();
    } catch (Exception solrExc) {
      log.error("Failed to execute query ["+query.toString()+"] due to: "+solrExc, solrExc);
      excCounter.inc();
    } finally {
      if (!timerStopped) {
        qTime = queryTimerCtxt.stop();
      }
    }

    long qTimeMs = TimeUnit.MILLISECONDS.convert(qTime, TimeUnit.NANOSECONDS);
    if (qTimeMs > slowQueryThresholdMs) {
      slowCounter.inc();

      if (slowQueryLogger != null) {
        synchronized (slowQueryLogger) {
          try {
            slowQueryLogger.write(query+" : "+qTimeMs+"\n");
            slowQueryLogger.flush();
          } catch (Exception ignore) {
            log.warn("Failed to write slow query ["+query+"] to slowQueryLogger file due to: "+ignore);
          }
        }
      }
    }
  }

  protected String getDateRangeFilter(String field) {
    Random random = rands.get();

    long ms1 = (random.nextInt(diffSecs) * 1000L) + minDate;
    long ms2 = (random.nextInt(diffSecs) * 1000L) + minDate;

    long minMs = Math.min(ms1, ms2);
    long maxMs = Math.max(ms1, ms2);

    SimpleDateFormat sdf = df.get();
    String lowerDate = sdf.format(new Date(minMs)) + "/HOUR";
    String upperDate = sdf.format(new Date(maxMs)) + "/HOUR";
    String cache = random.nextInt(5) <= 2 ? "" : "{!cache=false}";
    return cache + field + ":[" + lowerDate + " TO " + upperDate + "]";
  }

  protected String getTermClause(String field) {
    List<String> terms = termsDict.get(field);
    if (terms == null || terms.size() == 0)
      return "";

    Random random = rands.get();
    int termIndex = random.nextInt(terms.size());
    String term = terms.get(termIndex);

    String clause = field + ":" + term;
    if (random.nextBoolean())
      clause += "^" + (2 + random.nextInt(100));
    if (random.nextInt(5) < 3)
      clause = "+" + clause;

    return clause;
  }

  protected SolrQuery addIntRangeFilter(SolrQuery query, String field) {

    FieldStatsInfo fsi = numericFieldStats.get(field);
    Integer maxInt = (Integer)fsi.getMax();

    Random random = rands.get();
    int rand1 = random.nextInt(maxInt);
    int rand2 = random.nextInt(maxInt);
    int min = Math.min(rand1, rand2);
    int max = Math.max(rand1, rand2);
    String cache = random.nextBoolean() ? "" : "{!cache=false}";
    if (min != max)
      query.addFilterQuery(cache + field + ":[" + min + " TO " + max + "]");
    else
      query.addFilterQuery(cache + field + ":[" + min + " TO *]");

    return query;
  }

  protected SolrQuery addLongRangeFilter(SolrQuery query, String field) {

    FieldStatsInfo fsi = numericFieldStats.get(field);
    Long maxLong = (Long)fsi.getMax();

    Random random = rands.get();
    long rand1 = random.nextInt(maxLong.intValue());
    long rand2 = random.nextInt(maxLong.intValue());
    long min = Math.min(rand1, rand2);
    long max = Math.max(rand1, rand2);
    if (min != max)
      query.addFilterQuery(field + ":[" + min + " TO " + max + "]");
    else
      query.addFilterQuery(field + ":[" + min + " TO *]");

    return query;
  }

  @Override
  public Arguments getDefaultParameters() {
    Arguments defaultParameters = new Arguments();
    defaultParameters.addArgument("ZK_HOST", "localhost:9983");
    defaultParameters.addArgument("COLLECTION", "gettingstarted");
    defaultParameters.addArgument("RANDOM_SEED", "5150");
    defaultParameters.addArgument("SLOW_QUERY_THRESHOLD_MS", "150");
    defaultParameters.addArgument("TERMS_LIMIT", "3000");
    defaultParameters.addArgument("MODE", "solr");
    defaultParameters.addArgument("FUSION_QUERY_ENDPOINTS", "http://localhost:8765/api/v1/query-pipelines/${collection}-default/collections/${collection}/select");
    defaultParameters.addArgument("FUSION_USER", "admin");
    defaultParameters.addArgument("FUSION_PASS", "");
    defaultParameters.addArgument("FUSION_REALM", "native");
    defaultParameters.addArgument("FUSION_AUTH", "true");

    return defaultParameters;
  }

  @Override
  public void setupTest(JavaSamplerContext context) {
    super.setupTest(context);

    int ref = refCounter.incrementAndGet(); // keep track of threads using the statics in this class
    log.info("setupTest called for ref "+ref+" in thread: "+Thread.currentThread().getName());

    Map<String, String> params = new HashMap<String, String>();
    Iterator<String> paramNames = context.getParameterNamesIterator();
    while (paramNames.hasNext()) {
      String paramName = paramNames.next();
      String param = context.getParameter(paramName);
      if (param != null)
        params.put(paramName, param);
    }

    slowQueryThresholdMs = Long.parseLong(params.get("SLOW_QUERY_THRESHOLD_MS"));

    // initialize resources that are used by all threads
    synchronized (QuerySampler.class) {

      log.info("Initializing QuerySampler using params: "+params);

      if (cloudSolrClient == null) {
        String zkHost = params.get("ZK_HOST");
        log.info("Connecting to SolrCloud using zkHost: " + zkHost);
        String collection = params.get("COLLECTION");

        cloudSolrClient = new CloudSolrClient(zkHost);
        cloudSolrClient.setDefaultCollection(collection);
        cloudSolrClient.connect();
        HttpClientUtil.setMaxConnections(cloudSolrClient.getLbClient().getHttpClient(), 500);
        HttpClientUtil.setMaxConnectionsPerHost(cloudSolrClient.getLbClient().getHttpClient(), 100);
        log.info("Connected to SolrCloud; collection=" + collection);
      }

      String mode = params.get("MODE");

      log.info("mode="+mode);

      if ("fusion".equalsIgnoreCase(mode)) {
        useFusion = true;
        if (fusionPipelineClient == null) {
          boolean fusionAuth = "true".equals(params.get("FUSION_AUTH"));

          String fusionEndpoints = params.get("FUSION_QUERY_ENDPOINTS");
          if (fusionEndpoints == null || fusionEndpoints.trim().isEmpty())
            throw new IllegalStateException("Must provide at least 1 Fusion endpoint when running in fusion mode!");

          String collection = params.get("COLLECTION");
          fusionEndpoints = fusionEndpoints.replace("${collection}", collection);

          fusionHostList = FusionPipelineClient.extractFusionHosts(fusionEndpoints);
          log.info("Configured Fusion host and port list: "+fusionHostList);
          fusionQueryPipelinePath = FusionPipelineClient.extractPath(fusionEndpoints);
          log.info("Configured Fusion query pipeline path: "+fusionQueryPipelinePath);

          try {
            if (fusionAuth) {
              fusionPipelineClient =
                  new FusionPipelineClient(fusionHostList,
                      params.get("FUSION_USER"),
                      params.get("FUSION_PASS"),
                      params.get("FUSION_REALM"));

            } else {
              fusionPipelineClient = new FusionPipelineClient(fusionHostList);
            }
          } catch (Exception exc) {
            if (exc instanceof RuntimeException) {
              throw (RuntimeException)exc;
            } else {
              throw new RuntimeException(exc);
            }
          }
        }
      }

      if (randomSeed == null) {
        randomSeed = new Long(params.get("RANDOM_SEED"));
      }

      if (diffSecs == null) {
        try {
          Date[] minMax = getMinMaxDate(cloudSolrClient, "timestamp1_tdt");
          minDate = minMax[0].getTime();
          maxDate = minMax[1].getTime();
          diffSecs = new Integer(Math.round((maxDate - minDate) / 1000));
        } catch (Exception exc) {
          throw new RuntimeException(exc);
        }
      }

      if (termsDict == null) {
        // build terms dictionary
        int termsLimit = Integer.parseInt(params.get("TERMS_LIMIT"));
        try {
          termsDict = buildTermsDictionary(cloudSolrClient, termsLimit);
        } catch (Exception exc) {
          throw new RuntimeException(exc);
        }
      }

      if (numericFieldStats == null) {
        numericFieldStats = new HashMap<>();
        for (String f : new String[]{"long1_l","long2_l","integer1_i","integer2_i"}) {
          try {
            numericFieldStats.put(f, getNumericFieldStats(cloudSolrClient, f));
          } catch (Exception exc) {
            log.error("Failed to get field stats for "+f+" due to: "+exc, exc);
          }
        }
      }

      if (reporter == null) {
        reporter = ConsoleReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS).build();
        reporter.start(1, TimeUnit.MINUTES);
      }

      if (slowQueryLogger == null) {
        try {
          slowQueryLogger = new OutputStreamWriter(new FileOutputStream("slow_queries.txt", true), StandardCharsets.UTF_8);
        } catch (FileNotFoundException e) {
          throw new RuntimeException(e);
        }
      }

      if (noResultsQueryLogger == null) {
        try {
          noResultsQueryLogger = new OutputStreamWriter(new FileOutputStream("no_results.txt", true), StandardCharsets.UTF_8);
        } catch (FileNotFoundException e) {
          throw new RuntimeException(e);
        }
      }

      setupOk = true;
    }
  }

  protected FieldStatsInfo getNumericFieldStats(SolrClient solrClient, String numFieldName) throws Exception {
    long _startMs = System.currentTimeMillis();

    SolrQuery statsQuery = new SolrQuery("*:*");
    statsQuery.setRows(1);
    statsQuery.setStart(0);
    statsQuery.setFields(numFieldName);
    statsQuery.setSort(numFieldName, SolrQuery.ORDER.asc);
    QueryResponse qr = solrClient.query(statsQuery);
    SolrDocumentList results = qr.getResults();
    if (results.getNumFound() == 0)
      throw new IllegalStateException("Cannot get min/max for "+numFieldName);

    Object min = results.get(0).getFirstValue(numFieldName);
    long count = qr.getResults().getNumFound();

    // get max value of this field using a top 1 query
    statsQuery.setSort(numFieldName, SolrQuery.ORDER.desc);
    qr = solrClient.query(statsQuery);
    Object max = qr.getResults().get(0).getFirstValue(numFieldName);

    NamedList<Object> nl = new NamedList<Object>();
    nl.add("min", min);
    nl.add("max", max);
    nl.add("count", new Long(count));

    long _diffMs = (System.currentTimeMillis() - _startMs);
    log.info("Took " + _diffMs + " ms to lookup min/max from index for " + numFieldName);

    return new FieldStatsInfo(nl, numFieldName);
  }

  protected Date[] getMinMaxDate(SolrClient solrClient, String tdtFieldName) throws Exception {
    SolrQuery statsQuery = new SolrQuery("*:*");
    statsQuery.setRows(0);
    statsQuery.setStart(0);
    statsQuery.setGetFieldStatistics(tdtFieldName);
    QueryResponse qr = solrClient.query(statsQuery);
    FieldStatsInfo fsi = qr.getFieldStatsInfo().get(tdtFieldName);
    return new Date[]{(Date)fsi.getMin(), (Date)fsi.getMax()};
  }
  
  protected Map<String, List<String>> buildTermsDictionary(SolrClient solr, int termsLimit) throws Exception {
    Map<String, List<String>> terms = new HashMap<String, List<String>>();
    SolrQuery termsQ = new SolrQuery();
    termsQ.setParam("qt", "/terms");
    termsQ.add("terms.fl", "text1_txt_en", "text3_txt_en", "string1_s", "string2_s");
    termsQ.setParam("terms.limit", String.valueOf(termsLimit));
    QueryResponse resp = solr.query(termsQ);
    Map<String, List<TermsResponse.Term>> termsMap = resp.getTermsResponse().getTermMap();
    for (String field : termsMap.keySet()) {
      List<String> fieldTerms = new ArrayList<String>();
      terms.put(field, fieldTerms);
      for (TermsResponse.Term nextTerm : termsMap.get(field)) {
        fieldTerms.add(nextTerm.getTerm());
      }
    }

    String[] textFields = new String[]{"text1_txt_en", "text3_txt_en", "string1_s", "string2_s"};
    for (String tf : textFields) {
      List<String> termsForField = terms.get(tf);
      log.info("Loaded "+(termsForField != null ? termsForField.size()+"" : "NULL")+" terms for "+tf);
    }

    return terms;
  }

  @Override
  public void teardownTest(JavaSamplerContext context) {
    if (cloudSolrClient != null) {
      int refs = refCounter.decrementAndGet();
      if (refs == 0) {

        if (reporter != null) {
          reporter.report();
          reporter.stop();
        }

        try {
          cloudSolrClient.shutdown();
        } catch (Exception ignore) {
        }
        cloudSolrClient = null;
        log.info("Shutdown CloudSolrClient.");

        if (fusionPipelineClient != null) {
          fusionPipelineClient.shutdown();
          fusionPipelineClient = null;
        }
      }
    }

    super.teardownTest(context);
  }

}
