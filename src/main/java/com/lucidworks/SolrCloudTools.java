package com.lucidworks;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.URL;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.request.DirectXmlRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;

/**
 * Command-line utility for working with SolrCloud clusters.
 */
public class SolrCloudTools {
  
  /**
   * Enum of tools supported by this command-line application.
   */
  public static enum ToolType {
    healthcheck, backup, indexer
  }

  public interface Tool {
    Option[] getOptions();

    void runTool(CloudSolrClient solr, CommandLine cli) throws Exception;
  }

  public static Logger log = Logger.getLogger(SolrCloudTools.class);

  private static final DateFormat TIMESTAMP_FORMATTER = DateFormat
      .getDateTimeInstance(DateFormat.LONG, DateFormat.LONG);

  private static final SimpleDateFormat ISO_8601_DATE_FMT = 
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

  public static final String timestamp() {
    return timestamp(System.currentTimeMillis());
  }

  public static final String timestamp(long timeMs) {
    return TIMESTAMP_FORMATTER.format(new Date(timeMs));
  }

  public static final Throwable getRootCause(Throwable thr) {
    if (thr == null)
      return null;
    Throwable rootCause = thr;
    Throwable cause = thr;
    while ((cause = cause.getCause()) != null)
      rootCause = cause;
    return rootCause;
  }

  private static final String DEFAULT_BACKUP_DIR = "/tmp";

  private static final int ZK_TIMEOUT = 15000;
  private static final String ZK_HOST = "localhost:2181";
  private static final String DEFAULT_COLLECTION = "collection1";

  /**
   * Runs this tool.
   */
  public static void main(String[] args) throws Exception {
    if (args == null || args.length == 0 || args[0] == null || args[0].trim().length() == 0) {
      System.err.println("Invalid command-line args! Must pass the name of a tool to run.\nSupported tools:\n");
      displayToolOptions(System.err);
      System.exit(1);
    }
    
    // Determine the tool
    ToolType toolType = null;
    try {
      toolType = ToolType.valueOf(args[0].trim().toLowerCase());
    } catch (IllegalArgumentException iae) {
      System.err.println("Invalid command-line args! Unknown tool "+args[0]+"!.\nSupported tools:\n");
      displayToolOptions(System.err);
      System.exit(1);      
    }
    
    log.info("Running "+toolType+" tool");    
    
    String[] toolArgs = new String[args.length-1];
    System.arraycopy(args, 1, toolArgs, 0, toolArgs.length);
    
    // TODO: Be better to support dynamic tool resolution using classpath scanning
    Tool tool = newTool(toolType);
    
    // process command-line args to configure this application
    CommandLine cli = 
        processCommandLineArgs(joinCommonAndToolOptions(tool.getOptions()), toolArgs);

    String collection = cli.getOptionValue("collection", DEFAULT_COLLECTION);
    String zkHost = cli.getOptionValue("zkHost", ZK_HOST);

    log.info("Connecting to Solr cluster: " + zkHost);
    CloudSolrClient cloudSolrServer = null;
    try {
      cloudSolrServer = new CloudSolrClient(zkHost);
      cloudSolrServer.setDefaultCollection(collection);
      cloudSolrServer.connect();
      
      // run the requested tool
      tool.runTool(cloudSolrServer, cli);
    } finally {
      if (cloudSolrServer != null) {
        try {
          cloudSolrServer.shutdown();
        } catch (Exception ignore) {}
      }
    }
  } // end main
  
  /**
   * Array of options common to all tools, such as zkHost and collection name. 
   */
  @SuppressWarnings("static-access")
  public static Option[] getCommonToolOptions() {
    return new Option[] {
        OptionBuilder
            .withArgName("HOST")
            .hasArg()
            .isRequired(false)
            .withDescription("Address of the Zookeeper ensemble; defaults to: localhost:2181")
            .create("zkHost"),
        OptionBuilder
            .withArgName("COLLECTION")
            .hasArg()
            .isRequired(false)
            .withDescription("Name of collection; defaults to: collection1")
            .create("collection"),
        OptionBuilder
            .withArgName("#")
            .hasArg()
            .isRequired(false)
            .withDescription("Zookeeper timeout in milliseconds; defaults to: 15000")
            .create("zkTimeout")
    };
  }

  private static final long MS_IN_MIN = 60 * 1000L;
  private static final long MS_IN_HOUR = MS_IN_MIN * 60L;
  private static final long MS_IN_DAY = MS_IN_HOUR * 24L;

  private static final String uptime(long uptimeMs) {
    if (uptimeMs <= 0L)
      return "?";

    long numDays = (uptimeMs >= MS_IN_DAY) ? (long) Math.floor(uptimeMs
        / MS_IN_DAY) : 0L;
    long rem = uptimeMs - (numDays * MS_IN_DAY);
    long numHours = (rem >= MS_IN_HOUR) ? (long) Math.floor(rem / MS_IN_HOUR)
        : 0L;
    rem = rem - (numHours * MS_IN_HOUR);
    long numMinutes = (rem >= MS_IN_MIN) ? (long) Math.floor(rem / MS_IN_MIN)
        : 0L;
    rem = rem - (numMinutes * MS_IN_MIN);
    long numSeconds = Math.round(rem / 1000);
    return String.format("%d days, %d hours, %d minutes, %d seconds", numDays,
        numHours, numMinutes, numSeconds);
  }

  // Holds the backup state for a shard
  static class ShardBackupState {
    String shard;
    HttpSolrServer solrServer;
    Date completedAt;
    Date previousSnapshotCompletedAt;

    ShardBackupState(String shard, HttpSolrServer solrServer) {
      this.shard = shard;
      this.solrServer = solrServer;
    }

    boolean isComplete() {
      return (completedAt != null);
    }
  }

  static class ReplicaHealthReport implements Comparable<ReplicaHealthReport> {
    String shard;
    String replicaUrl;
    String status;
    long numDocs;
    boolean isLeader;
    long uptimeMs;
    String currentHeapSize;

    ReplicaHealthReport(String shard, String replicaUrl) {
      this(shard, replicaUrl, "error", -1, false, -1, "?");
    }

    ReplicaHealthReport(String shard, String replicaUrl, String status,
        long numDocs, boolean isLeader, long uptimeMs, String currentHeapSize) {
      this.shard = shard;
      this.replicaUrl = replicaUrl;
      this.numDocs = numDocs;
      this.status = status;
      this.isLeader = isLeader;
      this.uptimeMs = uptimeMs;
      this.currentHeapSize = currentHeapSize;
    }

    public String toString() {
      URL url = null;
      try {
        url = new URL(replicaUrl);
      } catch (Exception exc) {
        throw new RuntimeException(exc);
      }

      String hostAndPort = url.getHost() + ":" + url.getPort();
      StringBuilder sb = new StringBuilder();
      sb.append(shard).append(": ");
      sb.append(hostAndPort).append(isLeader ? "*" : " ");
      sb.append(" status: ").append(status);
      if (!"error".equals(status)) {
        sb.append(", numDocs: ").append(numDocs);
        sb.append(", uptime: ").append(uptime(uptimeMs)).append(", heapSize: ")
            .append(currentHeapSize);
      }
      return sb.toString();
    }

    public int hashCode() {
      return this.shard.hashCode() + (isLeader ? 1 : 0);
    }

    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (!(obj instanceof ReplicaHealthReport))
        return true;
      ReplicaHealthReport that = (ReplicaHealthReport) obj;
      return this.shard.equals(that.shard) && this.isLeader == that.isLeader;
    }

    public int compareTo(ReplicaHealthReport other) {
      if (this == other)
        return 0;
      if (other == null)
        return 1;

      int myShardIndex = Integer
          .parseInt(this.shard.substring("shard".length()));

      int otherShardIndex = Integer.parseInt(other.shard.substring("shard"
          .length()));

      if (myShardIndex == otherShardIndex) {
        // same shard index, list leaders first
        return this.isLeader ? -1 : 1;
      }

      return myShardIndex - otherShardIndex;
    }
  }

  static class ShardDocCounts {
    String shard;
    List<ReplicaHealthReport> replicas;
    boolean hasMismatch;
    ReplicaHealthReport leader;

    ShardDocCounts(String shard, List<ReplicaHealthReport> replicas) {
      this.shard = shard;
      this.replicas = replicas;

      // find leader
      this.leader = null;
      for (ReplicaHealthReport rdc : replicas) {
        if (rdc.isLeader) {
          this.leader = rdc;
          break;
        }
      }

      if (this.leader != null) {
        for (ReplicaHealthReport rdc : replicas) {
          if (!rdc.isLeader && rdc.numDocs != leader.numDocs) {
            hasMismatch = true;
          }
        }
      }
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(shard).append(": {\n");
      if (leader != null) {
        appendReplicaDocCount(leader, sb);
      } else {
        sb.append("\t*** NO LEADER FOUND ***\n");
      }
      for (ReplicaHealthReport rdc : replicas) {
        if (!rdc.isLeader) {
          appendReplicaDocCount(rdc, sb);
        }
      }
      sb.append("}");
      return sb.toString();
    }

    private void appendReplicaDocCount(ReplicaHealthReport rdc, StringBuilder sb) {
      sb.append("\t").append(rdc.replicaUrl).append(" = ").append(rdc.numDocs);
      long leaderDocs = (leader != null) ? leader.numDocs : Long.MIN_VALUE;
      sb.append(
          (rdc.isLeader ? " LEADER" : " diff:" + (leaderDocs - rdc.numDocs)))
          .append("\n");
    }
  }
    
  /**
   * Use CloudSolrClient direct updates to send synthetic documents to SolrCloud.
   */
  static class IndexerTool implements Tool {

    private static final String alpha = "abcdefghijklmnopqrstuvwxyz";
    private static final char[] lower = alpha.toCharArray();
    private static final char[] upper = alpha.toUpperCase().toCharArray();
    private static final char[] digits = "0123456789".toCharArray();
    
    private Random random = new Random(1);
    
    @SuppressWarnings("static-access")
    @Override
    public Option[] getOptions() {
      return new Option[] {
        OptionBuilder
          .withArgName("#")
          .hasArg()
          .isRequired(false)
          .withDescription("Num random docs to index")
          .create("numDocsToIndex"),
        OptionBuilder
          .withArgName("#")
          .hasArg()
          .isRequired(false)
          .withDescription("Start indexing at index")
          .create("indexOffset"),
        OptionBuilder
          .withArgName("#")
          .hasArg()
          .isRequired(false)
          .withDescription("Batch size when indexing; default 100")
          .create("batchSize"),
        OptionBuilder
          .withArgName("#")
          .hasArg()
          .isRequired(false)
          .withDescription("Send a commit after X docs; default -1 (disabled)")
          .create("commitEvery")
      };
    }
    
    @Override
    public void runTool(CloudSolrClient cloudSolrServer, CommandLine cli) throws Exception {
      HealthcheckTool healthcheck = new HealthcheckTool();
      
      int numDocs = Integer.parseInt(cli.getOptionValue("numDocsToIndex", "10000"));
      int indexOffset = Integer.parseInt(cli.getOptionValue("indexOffset", "0"));
      int commitEvery = Integer.parseInt(cli.getOptionValue("commitEvery", "-1"));
      int batchSize = Integer.parseInt(cli.getOptionValue("batchSize", "100"));
      List<SolrInputDocument> batch = new ArrayList<SolrInputDocument>(batchSize);
      for (int d = 0; d < numDocs; d++) {
        String docId = String.valueOf(d + indexOffset);
        SolrInputDocument inDoc = buildSolrInputDocument(docId);
        batch.add(inDoc);

        if (batch.size() >= batchSize) {
          sendBatch(cloudSolrServer, batch, 10, 3);
        }

        if (commitEvery > 0) {
          if (d % commitEvery == 0) {
            log.info("Sent " + d + " docs so far ... committing and then checking cloud state ...");
            cloudSolrServer.commit(true, true);
            healthcheck.runTool(cloudSolrServer, cli);
          }
        }
      }

      // last batch
      if (batch.size() > 0) {
        sendBatch(cloudSolrServer, batch, 10, 3);
      }

      log.info("Sent " + numDocs + " docs ... committing ...");
      cloudSolrServer.commit(true, true);
      log.info("Committed.");      
      healthcheck.runTool(cloudSolrServer, cli);
    }    
    
    protected int sendBatch(CloudSolrClient cloudSolrServer,
                            List<SolrInputDocument> batch, 
                            int waitBeforeRetry, 
                            int maxRetries) 
        throws Exception
    {
      int sent = 0;
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
            sent = sendBatch(cloudSolrServer, batch, waitBeforeRetry, maxRetries);
          } else {
            log.error("No more retries available! Add batch failed due to: "+rootCause);
            throw exc;
          }
        }      
      }
      batch.clear();    
      return sent;
    }    
    
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
  }
  
  /**
   * Requests health information from the cluster. 
   */
  static class HealthcheckTool implements Tool {

    @Override
    public Option[] getOptions() {
      return null;
    }

    @Override
    public void runTool(CloudSolrClient cloudSolrServer, CommandLine cli) throws Exception {
      log.info("Gathering health information from Solr cluster ...");
      
      //Map<String,AtomicInteger> collectionsPerHost = new TreeMap<String,AtomicInteger>();
      Map<String,AtomicInteger> replicasPerHost = new TreeMap<String,AtomicInteger>();
      
      Map<String, ShardDocCounts> shards = new TreeMap<String, ShardDocCounts>();
      ZkStateReader zkStateReader = cloudSolrServer.getZkStateReader();
      String collection = cli.getOptionValue("collection", DEFAULT_COLLECTION);
      SortedSet<ReplicaHealthReport> reports = new TreeSet<ReplicaHealthReport>();
      for (Slice slice : zkStateReader.getClusterState().getSlices(collection)) {
        String shardName = slice.getName();
        String leaderUrl = zkStateReader.getLeaderUrl(collection, slice.getName(), ZK_TIMEOUT);
        List<ReplicaHealthReport> replicaList = new ArrayList<ReplicaHealthReport>();
        for (Replica r : slice.getReplicas()) {
          
          AtomicInteger perHost = replicasPerHost.get(r.getNodeName());
          if (perHost == null) {
            perHost = new AtomicInteger(0);
            replicasPerHost.put(r.getNodeName(), perHost);
          }
          perHost.incrementAndGet();
          
          ZkCoreNodeProps replicaCoreProps = new ZkCoreNodeProps(r);
          String coreUrl = replicaCoreProps.getCoreUrl();
          HttpSolrServer solr = new HttpSolrServer(coreUrl);
          SolrQuery q = new SolrQuery("*:*");
          q.setRows(0);
          q.set("distrib", "false");
          try {
            QueryResponse qr = solr.query(q);
            long docCount = qr.getResults().getNumFound();
            boolean isLeader = coreUrl.equals(leaderUrl);
            long uptimeMs = 0L;
            String heapSize = "";

            // invoke admin/system report
            NamedList<Object> sysResp = solr.request(new DirectXmlRequest(
                "/admin/system", ""));
            NamedList<Object> jvm = (sysResp != null) ? (NamedList<Object>) sysResp.get("jvm") : null;
            if (jvm != null) {
              NamedList<Object> memory = (NamedList<Object>) jvm.get("memory");
              heapSize = (String) memory.get("used");
              NamedList<Object> jmx = (NamedList<Object>) jvm.get("jmx");
              uptimeMs = (jmx != null) ? ((Number) jmx.get("upTimeMS")).longValue() : -1L;
            }

            ReplicaHealthReport rhr = new ReplicaHealthReport(shardName, coreUrl,
                replicaCoreProps.getState(), docCount, isLeader, uptimeMs,
                heapSize);
            replicaList.add(rhr);

            reports.add(rhr);
          } catch (Exception exc) {
            log.error("ERROR: " + exc + " when trying to reach: " + coreUrl);

            ReplicaHealthReport rhr = new ReplicaHealthReport(shardName, coreUrl);
            replicaList.add(rhr);

            reports.add(rhr);

          } finally {
            solr.shutdown();
          }
        }

        ShardDocCounts sdc = new ShardDocCounts(shardName, replicaList);
        if (sdc.hasMismatch) {
          shards.put(shardName, sdc);
        }
      }

      // report health
      log.info("Health Report");
      for (ReplicaHealthReport rhr : reports) {
        log.info(rhr);
      }

      log.info("Found " + shards.size() + " shards with mis-matched doc counts.");
      if (shards.size() > 0) {
        log.info("At " + timestamp());
        for (ShardDocCounts sdc : shards.values()) {
          log.info(sdc.toString());
        }

        log.info("Details:");
        for (ShardDocCounts sdc : shards.values()) {
          log.info(sdc.shard.toString());
        }
      }
      
      log.info("Replica distribution across Hosts:");
      for (String host: replicasPerHost.keySet()) {
        log.info(host+": "+replicasPerHost.get(host).get());
      }
      
    }
  } // end HealthcheckTool

  /**
   * Performs a backup of every shard in a collection.
   */
  static class BackupTool implements Tool {

    @Override
    public void runTool(CloudSolrClient cloudSolrServer, CommandLine cli)
        throws Exception {

      // establish the date
      String restartDate = cli.getOptionValue("restartDate");
      Date now = null;
      if (restartDate != null) {
        now = ISO_8601_DATE_FMT.parse(restartDate);
      } else {
        Calendar rightNow = Calendar.getInstance();
        rightNow.set(Calendar.MILLISECOND, 0); // zero out millis for local
                                               // backups of small indexes
        now = rightNow.getTime();
      }
      log.info("NOW: " + ISO_8601_DATE_FMT.format(now)
          + " <-- Useful if you need to restart with -restartDate param");

      String collection = cloudSolrServer.getDefaultCollection();
      long enterMs = System.currentTimeMillis();
      log.info("Backing up collection: " + collection);

      // process command-line args to configure this application
      String backupLocation = cli.getOptionValue("backupDir",
          DEFAULT_BACKUP_DIR);
      int sleepInterval = Integer.parseInt(cli.getOptionValue("sleepInterval",
          "30"));
      int maxWaitForCompletion = Integer.parseInt(cli.getOptionValue(
          "maxWaitForCompletion", "10"));

      log.info("Doing a hard commit ...");
      long startMs = System.currentTimeMillis();
      cloudSolrServer.commit();
      long diffMs = (System.currentTimeMillis() - startMs);
      log.info("Commit succeeded, took: " + diffMs + " (ms).");

      if (cli.hasOption("optimizeDownToSegments")) {
        int numSegments = Integer.parseInt(cli
            .getOptionValue("optimizeDownToSegments"));
        log.info("Optimizing index down to " + numSegments
            + " segments before backing up.");
        startMs = System.currentTimeMillis();
        cloudSolrServer.optimize(true, true, numSegments);
        diffMs = (System.currentTimeMillis() - startMs);
        log.info("Optimize succeeded, took: " + diffMs
            + " (ms). Collecting cluster state information ...");
      }

      Map<String, String> leaders = getShardLeaders(cloudSolrServer, collection);
      
      Thread.sleep(5000L); // ensure now is before any completion times

      Map<String, ShardBackupState> leaderClients = new HashMap<String, ShardBackupState>();
      for (String shard : leaders.keySet()) {
        String leaderUrl = leaders.get(shard);
        HttpSolrServer solr = new HttpSolrServer(leaderUrl);
        if (restartDate == null) {
          HttpClient httpClient = solr.getHttpClient();
          log.info("Sending backup request to " + shard + " leader at: "
              + leaderUrl);

          String getUrl = String.format(
              "%sreplication?command=backup&location=%s/%s", leaderUrl,
              URLEncoder.encode(backupLocation, "UTF-8"), shard);
          log.info("Sending request: " + getUrl);
          sendRequest(httpClient, getUrl);
          Thread.sleep(2000L); // slight delay between requests
        }

        leaderClients.put(leaderUrl, new ShardBackupState(shard, solr));
      }

      // max wait
      long startAt = System.currentTimeMillis();
      long maxWaitMs = maxWaitForCompletion * MS_IN_MIN;

      log.info("Starting to poll each shard leader for backup status ...");

      int finished = 0;
      while ((System.currentTimeMillis() - startAt) < maxWaitMs) {
        boolean allFinished = true;

        for (String leaderUrl : leaderClients.keySet()) {

          ShardBackupState backupState = leaderClients.get(leaderUrl);
          if (backupState.isComplete())
            continue;

          Date snapshotCompletedAt = null;
          try {
            snapshotCompletedAt = requestSnapshotCompletedAt(
                backupState.solrServer.getHttpClient(), leaderUrl
                    + "replication?command=details");
          } catch (Exception exc) {
            log.error("Failed to get snapshot completed at date for shard "
                + backupState.shard + " from [" + leaderUrl + "] due to: "
                + exc, exc);
          }

          if (snapshotCompletedAt == null || snapshotCompletedAt.before(now)) {
            // keep looping as this shard leader is not done yet
            allFinished = false;

            if (snapshotCompletedAt != null) {
              if (backupState.previousSnapshotCompletedAt != null) {
                // TODO: if the completed at changed while we were running, then
                // it's very likely the
                // backup finished but the timestamps are off ... assume
                // complete?
              } else {
                backupState.previousSnapshotCompletedAt = snapshotCompletedAt;
              }
            }

            log.info("Leader for "
                + backupState.shard
                + " at ["
                + leaderUrl
                + "] is NOT finished backing up; snapshotCompletedAt=["
                + (snapshotCompletedAt != null ? ISO_8601_DATE_FMT
                    .format(snapshotCompletedAt) : "NULL") + "] now=["
                + ISO_8601_DATE_FMT.format(now) + "]");
          } else {
            ++finished;
            backupState.completedAt = snapshotCompletedAt;
            log.info("Leader for " + backupState.shard + " at [" + leaderUrl
                + "] finished backing up at: "
                + ISO_8601_DATE_FMT.format(snapshotCompletedAt));
          }
        }

        if (allFinished)
          break;

        log.info("Found " + finished + " of " + leaderClients.size()
            + " leaders finished backing up. Sleeping for " + sleepInterval
            + " seconds before checking again ...");

        try {
          Thread.sleep(sleepInterval * 1000);
        } catch (InterruptedException ie) {
          // just ignore it
        }
      }

      // shutdown connections to all Solr servers
      for (String leaderUrl : leaderClients.keySet()) {
        leaderClients.get(leaderUrl).solrServer.shutdown();
      }

      long exitMs = System.currentTimeMillis();
      log.info("BackupDriver completed successfully after "
          + Math.round((exitMs - enterMs) / 1000d) + " seconds");
    }

    /**
     * Request details from the replication handler to extract the
     * snapshotCompletedAt date for the current backup.
     */
    @SuppressWarnings({ "unchecked", "deprecation" })
    private Date requestSnapshotCompletedAt(HttpClient httpClient, String getUrl)
        throws Exception {

      NamedList<Object> details = sendRequest(httpClient, getUrl);
      if (details.get("details") != null) {
        details = (NamedList<Object>) details.get("details");
      }

      Date completedAt = null;
      NamedList<Object> backup = (NamedList<Object>) details.get("backup");
      if (backup != null) {
        String snapshotCompletedAt = (String) backup.get("snapshotCompletedAt");
        if (snapshotCompletedAt != null) {
          log.info("Replication details for " + getUrl
              + " returned snapshotCompletedAt: " + snapshotCompletedAt);
          completedAt = new Date(snapshotCompletedAt);
        }
      }

      return completedAt;
    }
    
    @SuppressWarnings("static-access")
    @Override
    public Option[] getOptions() {
      return new Option[] {
          OptionBuilder
          .withArgName("DIR")
          .hasArg()
          .isRequired(false)
          .withDescription(
              "Backup location on each host, defaults to: "
                  + DEFAULT_BACKUP_DIR).create("backupDir"),
      OptionBuilder
          .withArgName("timestamp")
          .hasArg()
          .isRequired(false)
          .withDescription(
              "Useful for a failed run where all the backup requests were queued but then app failed (yyyy-MM-ddTHH:mm:ss).")
          .create("restartDate"),
      OptionBuilder
          .withArgName("INT")
          .hasArg()
          .isRequired(false)
          .withDescription(
              "Number of seconds to sleep between status checks, defaults to: 30 seconds")
          .create("sleepInterval"),
      OptionBuilder
          .withArgName("INT")
          .hasArg()
          .isRequired(false)
          .withDescription(
              "Max minutes to wait for backups to complete, defaults to: 10")
          .create("maxWaitForCompletion"),
      OptionBuilder
          .withArgName("INT")
          .hasArg()
          .isRequired(false)
          .withDescription(
              "Do an optimize down to N segments before backing up (careful with large indexes)")
          .create("optimizeDownToSegments")
      };
    }
  } // end BackupTool
    
  private static Tool newTool(ToolType toolType) throws Exception {
    if (toolType == ToolType.healthcheck) {
      return new HealthcheckTool();
    } else if (toolType == ToolType.backup) {
      return new BackupTool();
    } else if (toolType == ToolType.indexer) {
      return new IndexerTool();
    } else {
      throw new IllegalArgumentException(toolType+" not supported!");
    }
  }

  private static final Map<String, String> getShardLeaders(
      CloudSolrClient solr, String collection) throws Exception {
    Map<String, String> leaders = new TreeMap<String, String>();
    ZkStateReader zkStateReader = solr.getZkStateReader();
    for (Slice slice : zkStateReader.getClusterState().getSlices(collection)) {
      leaders.put(slice.getName(),
          zkStateReader.getLeaderUrl(collection, slice.getName(), ZK_TIMEOUT));
    }
    return leaders;
  }

  /**
   * Send HTTP GET request to Solr.
   */
  public static NamedList<Object> sendRequest(HttpClient httpClient, String getUrl) throws Exception {
    NamedList<Object> solrResp = null;

    // Prepare a request object
    HttpGet httpget = new HttpGet(getUrl);

    // Execute the request
    // System.out.print("\nSending GET request to: " + getUrl + "  ");
    HttpResponse response = httpClient.execute(httpget);
    // log.info(response.getStatusLine() + "\n");

    // Get hold of the response entity
    HttpEntity entity = response.getEntity();
    if (response.getStatusLine().getStatusCode() != 200) {
      StringBuilder body = new StringBuilder();
      if (entity != null) {
        InputStream instream = entity.getContent();
        String line;
        try {
          BufferedReader reader = new BufferedReader(new InputStreamReader(
              instream));
          while ((line = reader.readLine()) != null) {
            body.append(line);
          }
        } catch (Exception ignore) {
          // squelch it - just trying to compose an error message here
        } finally {
          instream.close();
        }
      }
      throw new Exception("GET request [" + getUrl + "] failed due to: "
          + response.getStatusLine() + ": " + body);
    }

    // If the response does not enclose an entity, there is no need
    // to worry about connection release
    if (entity != null) {
      InputStream instream = entity.getContent();
      try {
        solrResp = (new XMLResponseParser()).processResponse(instream, "UTF-8");
      } catch (RuntimeException ex) {
        // In case of an unexpected exception you may want to abort
        // the HTTP request in order to shut down the underlying
        // connection and release it back to the connection manager.
        httpget.abort();
        throw ex;
      } finally {
        // Closing the input stream will trigger connection release
        instream.close();
      }
    }

    return solrResp;
  }
  
  private static void displayToolOptions(PrintStream out) throws Exception {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("healthcheck", getToolOptions(new HealthcheckTool()));    
    formatter.printHelp("backup", getToolOptions(new BackupTool()));    
    formatter.printHelp("indexer", getToolOptions(new IndexerTool()));    
  }
    
  private static Options getToolOptions(Tool tool) {
    Options options = new Options();
    options.addOption("h", "help", false, "Print this message");
    options.addOption("v", "verbose", false, "Generate verbose log messages");
    Option[] toolOpts = joinCommonAndToolOptions(tool.getOptions());
    for (int i = 0; i < toolOpts.length; i++)
      options.addOption(toolOpts[i]);
    return options;
  }  

  public static Option[] joinCommonAndToolOptions(Option[] toolOpts) {
    List<Option> options = new ArrayList<Option>();
    for (Option common : getCommonToolOptions())
      options.add(common);
    if (toolOpts != null) {
      for (Option mine : toolOpts)
        options.add(mine);   
    }
    return options.toArray(new Option[0]);    
  }
  
  /**
   * Parses the command-line arguments passed by the user.
   * 
   * @param tool
   * @param args
   * @return CommandLine The Apache Commons CLI object.
   */
  public static CommandLine processCommandLineArgs(Option[] customOptions, String[] args) {
    Options options = new Options();

    options.addOption("h", "help", false, "Print this message");
    options.addOption("v", "verbose", false, "Generate verbose log messages");

    if (customOptions != null) {
      for (int i = 0; i < customOptions.length; i++) {
        options.addOption(customOptions[i]);
      }
    }

    CommandLine cli = null;
    try {
      cli = (new GnuParser()).parse(options, args);
    } catch (ParseException exp) {
      boolean hasHelpArg = false;
      if (args != null && args.length > 0) {
        for (int z = 0; z < args.length; z++) {
          if ("-h".equals(args[z]) || "-help".equals(args[z])) {
            hasHelpArg = true;
            break;
          }
        }
      }
      if (!hasHelpArg) {
        System.err.println("Failed to parse command-line arguments due to: "
            + exp.getMessage());
      }
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(SolrCloudTools.class.getName(), options);
      System.exit(1);
    }

    if (cli.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(SolrCloudTools.class.getName(), options);
      System.exit(0);
    }

    return cli;
  }
}
