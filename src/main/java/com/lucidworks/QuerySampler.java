package com.lucidworks;

import com.codahale.metrics.*;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.log.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.TermsResponse;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class QuerySampler extends AbstractJavaSamplerClient implements Serializable {
    private static final long serialVersionUID = 1L;

    // keeps track of how many tests are running this sampler and when there are
    // none, a final hard commit is sent.
    private static AtomicInteger refCounter = new AtomicInteger(0);
    private static final MetricRegistry metrics = new MetricRegistry();
    private static final com.codahale.metrics.Timer queryTimer = metrics.timer("query");
    private static final com.codahale.metrics.Counter noResultsCounter = metrics.counter("noresults");
    private static final com.codahale.metrics.Counter excCounter = metrics.counter("exceptions");

    private static ConsoleReporter reporter = null;
    private static CloudSolrServer cloudSolrServer = null;
    private static Map<String,List<String>> termsDict = null;

    protected Logger log;
    private long minDate;
    private long maxDate;
    private int diffSecs;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    protected Random random;


    public SampleResult runTest(JavaSamplerContext context) {
        SampleResult result = new SampleResult();
        result.sampleStart();

        SolrQuery query = new SolrQuery();

        StringBuilder randomQuery = new StringBuilder();
        if (random.nextBoolean())
            randomQuery.append(getTermClause("string1_s")).append(" ");
        if (random.nextBoolean())
            randomQuery.append(getTermClause("string2_s")).append(" ");
        if (random.nextBoolean())
            randomQuery.append(getTermClause("text1_en")).append(" ");
        if (random.nextBoolean())
            randomQuery.append(getTermClause("text3_en")).append(" ");
        if (random.nextBoolean())
            randomQuery.append("boolean1_b:true").append(" ");

        String qs = randomQuery.toString().trim();
        if (qs.length() == 0)
            qs = "*:*";

        query.setQuery(qs);

        if (random.nextBoolean()) {
            if (random.nextBoolean())
                query.addFilterQuery("boolean2_b:" + random.nextBoolean());

            if (random.nextInt(10) < 4)
                addIntRangeFilter(query, "integer1_i", 99999);

            if (random.nextInt(5) == 3)
                addIntRangeFilter(query, "integer2_i", 9999);

            if (random.nextInt(10) == 5)
                addLongRangeFilter(query, "long1_l", 5000000);

            if (random.nextInt(20) == 10)
                addLongRangeFilter(query, "long2_l", 5000000);

            if (random.nextInt(5) <= 2)
                query.addFilterQuery(getDateRangeFilter("timestamp1_tdt"));

            if (random.nextInt(5) == 4)
                query.addFilterQuery(getDateRangeFilter("timestamp2_tdt"));
        }

        if (random.nextInt(5) < 3) {
            query.setFacet(true);
            query.addFacetField("string1_s");
        }

        if (random.nextInt(10) < 3) {
            query.setFacet(true);
            query.addFacetField("string2_s");
        }

        query.setRows(10);

        // sort by date
        if (random.nextBoolean())
          query.setSort("timestamp1_tdt", SolrQuery.ORDER.desc);

        if (random.nextInt(10) < 4)
          query.setSort("integer1_i", SolrQuery.ORDER.asc);

        if (random.nextInt(10) < 7) {
            query.setStart(0);
        } else {
            query.setStart(1);
        }


        final com.codahale.metrics.Timer.Context queryTimerCtxt = queryTimer.time();
        try {
            QueryResponse qr = cloudSolrServer.query(query);
            if (qr.getResults().getNumFound() == 0)
                noResultsCounter.inc();

            result.setResponseOK();
        } catch (SolrServerException solrExc) {
            excCounter.inc();
        } finally {
            queryTimerCtxt.stop();
        }

        result.sampleEnd();

        return result;
    }

    protected String getDateRangeFilter(String field) {
        long ms1 = random.nextInt(diffSecs)*1000 + minDate;
        long ms2 = random.nextInt(diffSecs)*1000 + minDate + (86400*1000);
        long minMs = Math.min(ms1, ms2);
        long maxMs = Math.max(ms1, ms2);
        String lowerDate = sdf.format(new Date(minMs))+"/HOUR";
        String upperDate = sdf.format(new Date(maxMs))+"/HOUR";
        String cache = random.nextBoolean() ? "" : "{!cache=false}";
        return cache+field+":["+lowerDate+" TO "+upperDate+"]";
    }

    protected String getTermClause(String field) {
        List<String> terms = termsDict.get(field);
        if (terms != null && terms.size() > 0) {
            String term = terms.get(random.nextInt(terms.size()));
            String clause = field+":"+term;
            if (random.nextBoolean())
                clause += "^"+(2+random.nextInt(100));
            if (random.nextInt(5) < 3)
                clause = "+"+clause;
        }
        return "";
    }

    protected SolrQuery addIntRangeFilter(SolrQuery query, String field, int maxInt) {
        int rand1 = random.nextInt(maxInt);
        int rand2 = random.nextInt(maxInt);
        int min = Math.min(rand1, rand2);
        int max = Math.max(rand1, rand2);
        String cache = random.nextBoolean() ? "" : "{!cache=false}";
        if (min != max)
            query.addFilterQuery(cache+field+":["+min+" TO "+max+"]");
        else
            query.addFilterQuery(cache+field+":["+min+" TO *]");

        return query;
    }

    protected SolrQuery addLongRangeFilter(SolrQuery query, String field, int maxLong) {
        long rand1 = random.nextInt(maxLong);
        long rand2 = random.nextInt(maxLong);
        long min = Math.min(rand1, rand2);
        long max = Math.max(rand1, rand2);
        if (min != max)
            query.addFilterQuery(field+":["+min+" TO "+max+"]");
        else
            query.addFilterQuery(field+":["+min+" TO *]");

        return query;
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument("ZK_HOST", "localhost:9983");
        defaultParameters.addArgument("COLLECTION", "gettingstarted");
        defaultParameters.addArgument("RANDOM_SEED", "5150");
        return defaultParameters;
    }

    @Override
    public void setupTest(JavaSamplerContext context) {
        super.setupTest(context);

        refCounter.incrementAndGet(); // keep track of threads using the statics in this class

        log = getLogger().getChildLogger("QuerySampler");

        try {
            minDate = sdf.parse("2012-05-08T20:39:13Z").getTime();
            maxDate = sdf.parse("2014-05-08T20:34:23Z").getTime();
            long diffMs = maxDate - minDate;
            diffSecs = (int)Math.round(diffMs/1000);
        } catch (Exception exc) {
            throw new RuntimeException(exc);
        }

        Map<String,String> params = new HashMap<String,String>();
        Iterator<String> paramNames = context.getParameterNamesIterator();
        while (paramNames.hasNext()) {
            String paramName = paramNames.next();
            String param = context.getParameter(paramName);
            if (param != null)
                params.put(paramName, param);
        }

        random = new Random(Long.parseLong(params.get("RANDOM_SEED")));

        synchronized (QuerySampler.class) {
            if (cloudSolrServer == null) {
                String zkHost = params.get("ZK_HOST");
                getLogger().info("Connecting to SolrCloud using zkHost: " + zkHost);
                String collection = params.get("COLLECTION");
                cloudSolrServer = new CloudSolrServer(zkHost);
                cloudSolrServer.setDefaultCollection(collection);
                cloudSolrServer.connect();
                getLogger().info("Connected to SolrCloud; collection=" + collection);

                // build terms dictionary
                try {
                    termsDict = buildTermsDictionary(cloudSolrServer);
                } catch (Exception exc) {
                    throw new RuntimeException(exc);
                }
            }

            if (reporter == null) {
                reporter = ConsoleReporter.forRegistry(metrics)
                        .convertRatesTo(TimeUnit.SECONDS)
                        .convertDurationsTo(TimeUnit.MILLISECONDS).build();
                reporter.start(1, TimeUnit.MINUTES);
            }
        }
    }

    protected Map<String,List<String>> buildTermsDictionary(SolrServer solr) throws Exception {
        Map<String,List<String>> terms = new HashMap<String,List<String>>();
        SolrQuery termsQ = new SolrQuery();
        termsQ.setParam("qt", "/terms");
        termsQ.add("terms.fl", "text1_en", "text3_en", "string1_s", "string2_s", "string3_s");
        termsQ.setParam("terms.limit", "500");
        QueryResponse resp = solr.query(termsQ);
        Map<String,List<TermsResponse.Term>> termsMap = resp.getTermsResponse().getTermMap();
        for (String field : termsMap.keySet()) {
            List<String> fieldTerms = new ArrayList<String>();
            terms.put(field,fieldTerms);
            for (TermsResponse.Term nextTerm : termsMap.get(field)) {
                fieldTerms.add(nextTerm.getTerm());
            }
        }
        return terms;
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        if (cloudSolrServer != null) {
            int refs = refCounter.decrementAndGet();
            if (refs == 0) {

                if (reporter != null) {
                    reporter.report();
                    reporter.stop();
                }

                try {
                    cloudSolrServer.shutdown();
                } catch (Exception ignore) {}
                cloudSolrServer = null;
                log.info("Shutdown CloudSolrServer.");
            }
        }

        super.teardownTest(context);
    }

}
