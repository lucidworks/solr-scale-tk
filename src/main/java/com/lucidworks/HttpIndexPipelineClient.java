package com.lucidworks;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.json.simple.JSONArray;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class HttpIndexPipelineClient {

  HttpClient httpClient;
  List<String> endPoints;
  int numEndpoints;
  Random random;

  public HttpIndexPipelineClient(String endpointUrl) throws MalformedURLException {
    HttpSolrClient solrClient = new HttpSolrClient(endpointUrl);
    httpClient = solrClient.getHttpClient();
    endPoints = new ArrayList<String>(Arrays.asList(endpointUrl.split(",")));
    random = new Random();
    numEndpoints = endPoints.size();
  }

  protected String getLbEndpoint() {
    int index = (numEndpoints > 1) ? random.nextInt(numEndpoints) : 0;
    return endPoints.get(index);
  }

  public void postDocsToPipeline(JSONArray docs) throws Exception {
    String endpoint = getLbEndpoint();
    HttpPost postRequest = new HttpPost(endpoint);
    postRequest.setEntity(new StringEntity(docs.toString(),
      ContentType.create("application/json", StandardCharsets.UTF_8)));
    HttpResponse response = httpClient.execute(postRequest);
    HttpEntity entity = response.getEntity();
    int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode != 200 && statusCode != 204) {
      StringBuilder body = new StringBuilder();
      if (entity != null) {
        InputStream instream = entity.getContent();
        String line;
        try {
          BufferedReader reader = new BufferedReader(new InputStreamReader(instream));
          while ((line = reader.readLine()) != null)
            body.append(line);

        } catch (Exception ignore) {
          // squelch it - just trying to compose an error message here
          ignore.printStackTrace();
        } finally {
          instream.close();
        }
      }
      throw new Exception("POST doc to [" + endpoint + "] failed due to: "
              + response.getStatusLine() + ": " + body);
    }
    EntityUtils.consume(entity);
  }
}
