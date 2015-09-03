package com.lucidworks;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * To start Solr in Standalone mode ( type=solr )
 * ./bin/start -e techproducts -noprompt
 *
 * To start Solr in SolrCloud mode ( type=solrcloud )
 * ./bin/start -e cloud -noprompt
 *
 * To start Fusion ( type=fusion)
 * ./bin/fusion start
 *
 * {@link #runTest(JavaSamplerContext)} is called n times ( where n is the number of test runs you ask
 * jmeter to run and is specified in LoopController.loops in query.jmx )
 *
 * Use {@link #setupTest(JavaSamplerContext)} to create the set of queries which you then
 * want {@link #runTest(JavaSamplerContext)} to utilize in it's test run.
 *
 * This sample code always fires match all queries.
 */
public class SimpleQuerySampler extends AbstractJavaSamplerClient implements Serializable {
    private static final long serialVersionUID = 1L;

    // keeps track of how many tests are running this sampler and when there are
    // none, a final hard commit is sent.
    private static AtomicInteger refCounter = new AtomicInteger(0);
    private static final MetricRegistry metrics = new MetricRegistry();
    private static final com.codahale.metrics.Timer queryTimer = metrics.timer("query");
    private static final com.codahale.metrics.Counter noResultsCounter = metrics.counter("noresults");
    private static final com.codahale.metrics.Counter excCounter = metrics.counter("exceptions");

    private static ConsoleReporter reporter = null;
    private static SolrClient solrClient = null;

    private static Random random;
    private CloseableHttpClient httpClient;

    @Override
    public SampleResult runTest(JavaSamplerContext context) {
        SampleResult result = new SampleResult();
        result.sampleStart();

        SolrQuery query = new SolrQuery();
        query.setQuery("*:*");

        final com.codahale.metrics.Timer.Context queryTimerCtxt = queryTimer.time();
        try {
            QueryResponse qr = solrClient.query(query);
            if (qr.getResults().getNumFound() == 0)
                noResultsCounter.inc();

            result.setResponseOK();
        } catch (Exception solrExc) {
            excCounter.inc();
        } finally {
            queryTimerCtxt.stop();
        }

        result.sampleEnd();

        return result;
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument("RANDOM_SEED", "5150");
        defaultParameters.addArgument("mode", "solr");

        defaultParameters.addArgument("COLLECTION", "techproducts");
        defaultParameters.addArgument("SOLR_URL", "http://localhost:8983/solr");

        defaultParameters.addArgument("ZK_HOST", "localhost:9983");

        defaultParameters.addArgument("QUERY_PIPELINE", "http://localhost:8764/api/apollo/query-pipelines/default/collections/system_metrics");
        defaultParameters.addArgument("username", "admin");
        defaultParameters.addArgument("password", "password123");
        return defaultParameters;
    }

    @Override
    public void setupTest(JavaSamplerContext context) {
        super.setupTest(context);

        refCounter.incrementAndGet(); // keep track of threads using the statics in this class

        Map<String,String> params = new HashMap<>();
        Iterator<String> paramNames = context.getParameterNamesIterator();
        while (paramNames.hasNext()) {
            String paramName = paramNames.next();
            String param = context.getParameter(paramName);
            if (param != null)
                params.put(paramName, param);
        }

        random = new Random(Long.parseLong(params.get("RANDOM_SEED")));
        String mode = params.get("mode");

        synchronized (SimpleQuerySampler.class) {
            if (solrClient == null) {
                if ("solr".equals(mode)) {
                    String collection = params.get("COLLECTION");
                    String url = params.get("SOLR_URL");
                    if (!url.endsWith("/")) {
                        url = url + "/";
                    }
                    solrClient = new HttpSolrClient(url + collection);
                } else if ("solrcloud".equals(mode)) {
                    String collection = params.get("COLLECTION");
                    String zkString = params.get("ZK_HOST");
                    solrClient = new CloudSolrClient(zkString);
                    ((CloudSolrClient) solrClient).setDefaultCollection(collection);
                    ((CloudSolrClient) solrClient).connect();
                } else if ("fusion".equals(mode)) {
                    String username = params.get("username");
                    String password = params.get("password");
                    String url = params.get("QUERY_PIPELINE");

                    httpClient = HttpClientBuilder.create().useSystemProperties()
                            .addInterceptorLast(new PreEmptiveBasicAuthenticator(username, password))
                            .build();
                    solrClient = new HttpSolrClient(url, httpClient, new XMLResponseParser());
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

    public static class PreEmptiveBasicAuthenticator implements HttpRequestInterceptor {
        private final UsernamePasswordCredentials credentials;

        public PreEmptiveBasicAuthenticator(String user, String pass) {
            credentials = new UsernamePasswordCredentials(user, pass);
        }

        public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
            request.addHeader(new BasicScheme().authenticate(credentials, request, context));

        }
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        if (solrClient != null) {
            int refs = refCounter.decrementAndGet();
            if (refs == 0) {
                getLogger().info("Shutting down solr client");
                if (reporter != null) {
                    reporter.report();
                    reporter.stop();
                }
                try {
                    solrClient.close();
                } catch (IOException e) {
                    solrClient = null;
                }
            }
        }
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (IOException e) {
                httpClient = null;
            }
        }
        super.teardownTest(context);
    }

}
