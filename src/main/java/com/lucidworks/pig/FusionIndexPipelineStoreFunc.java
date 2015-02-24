package com.lucidworks.pig;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.lucidworks.HttpIndexPipelineClient;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.pigstats.PigStatusReporter;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Simple Pig StoreFunc for indexing documents to a cluster of indexing pipeline endpoints.
 */
public class FusionIndexPipelineStoreFunc extends StoreFunc {

  private static final Logger log = LoggerFactory.getLogger(FusionIndexPipelineStoreFunc.class);

  // Enum for interfacing with the Hadoop counter framework
  public static enum Counters {
    NUM_DOCS_PROCESSED, NUM_DOCS_INDEXED, NUM_TUPLE_TO_JSON_CONVERSION_ERRORS,
    NUM_FAILURES, NUM_INPUT_TUPLES_NOT_MATCHING_SCHEMA, SLOW_BATCHES, SLOW_INDEX_REQUESTS,
    NUM_COMMITS, BATCHES_TIMED_OUT, BATCHES_FAILED, BATCHES_RECOVERED
  }

  static {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

  private static final SimpleDateFormat ISO_8601_DATE_FMT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'.'S'Z'");

  private static final String FIELD_NAMES_FROM_SCHEMA_PROPS_KEY = "fieldNamesFromSchema";

  protected String endpoints;
  private ExecutorService executor = null;

  protected JSONArray batch = new JSONArray();
  protected int batchSize = 100;
  protected int batchCount = 0;
  protected int docCount = 0;

  // For accessing Hadoop counters
  PigStatusReporter reporter = null;

  // For accessing the schema information provided on the front-end
  private String myUDFContextSignature = null;

  // List of field names in the input tuple
  private List<String> fieldNamesFromSchema = null;

  protected HttpIndexPipelineClient pipelineClient;

  public FusionIndexPipelineStoreFunc(String endpoints, String batchSize) throws SolrServerException, IOException {
    this.endpoints = endpoints;
    this.batchSize = Integer.parseInt(batchSize);
    //pipelineClient = new HttpIndexPipelineClient(endpoints);
  }

  public void putNext(Tuple input) throws IOException {

    // important - must save ref to the reporter so that our counters work
    try {
      this.reporter = PigStatusReporter.getInstance();
    } catch (Exception exc) {
      log.warn("Failed to get PigStatusReporter due to: " + exc, exc);
      this.reporter = null;
    }

    // We get the list of field names in the input from schema processing in the front-end
    // and then pass that to the backend using UDFContext
    if (fieldNamesFromSchema == null) {
      Properties udfProps = UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[]{myUDFContextSignature});
      fieldNamesFromSchema = (List<String>) udfProps.get(FIELD_NAMES_FROM_SCHEMA_PROPS_KEY);
      if (fieldNamesFromSchema == null || fieldNamesFromSchema.isEmpty()) {
        throw new IOException("No field names passed to backend from front-end schema processing! Expected property " + FIELD_NAMES_FROM_SCHEMA_PROPS_KEY);
      }
    }

    incrementCounter(Counters.NUM_DOCS_PROCESSED, 1L);

    if (input.size() != fieldNamesFromSchema.size()) {
      incrementCounter(Counters.NUM_TUPLE_TO_JSON_CONVERSION_ERRORS, 1L);
      log.error(String.format("Failed to convert Tuple %s to JSON object due to schema mis-match! Expected %d fields but input has %d",
              tupleAsStr(input), fieldNamesFromSchema.size(), input.size()));
      return; // don't fail the job due to bad input
    }

    JSONObject solrDoc = tupleToJsonDoc(input);
    if (solrDoc == null) {
      return; // bad input ... keep on trucking though
    }

    ++docCount;

    try {
      batch.add(solrDoc);

      if (batch.size() >= batchSize) {

        log.info(String.format("Sending batch %d to server at docCount: %d", batchCount, docCount));

        long _startMs = System.currentTimeMillis();

        if (sendBatchWithTimeout(batch, 600)) {
          incrementCounter(Counters.NUM_DOCS_INDEXED, batchSize);
        }

        long _diffMs = (System.currentTimeMillis() - _startMs);
        if (_diffMs > 10000) {
          incrementCounter(Counters.SLOW_BATCHES, 1);
        }

        ++batchCount;

        if (reporter != null) {
          reporter.progress();
          String logMsg = "Added batch " + batchCount + "; processed " + docCount + " docs so far";
          reporter.setStatus(logMsg);
          log.info(logMsg);
        }

        batch.clear();
      }
    } catch (Exception e) {
      handleSolrServerException(input, e);
    }
  }

  protected boolean sendBatchWithTimeout(final JSONArray theBatch, final int timeoutSecs) throws IOException {

    synchronized (this) {
      if (executor == null) {
        executor = Executors.newCachedThreadPool();
      }
    }

    Callable<Object> task = new Callable<Object>() {
      public Object call() {
        try {
          FusionIndexPipelineStoreFunc.this.pipelineClient.postDocsToPipeline(theBatch);
          return Boolean.TRUE;
        } catch (Exception exc) {
          log.error("{In java.util.concurrent.Callable} Failed to send batch after " + timeoutSecs + " secs due to: " + exc.toString());
          return Boolean.FALSE;
        }
      }
    };

    boolean sentBatchOk = false;
    Future<Object> future = executor.submit(task);
    try {
      Object result = future.get(timeoutSecs, TimeUnit.SECONDS);
      if (result != null) {
        Boolean tf = (Boolean) result;
        sentBatchOk = tf.booleanValue();
      }
    } catch (java.util.concurrent.TimeoutException e) {
      log.error("Send batch " + batchCount + " timed out after " + timeoutSecs + " secs due to: " + e);
      incrementCounter(Counters.BATCHES_TIMED_OUT, 1);
      try {
        future.cancel(true);
      } catch (Exception ignore) {
      }

      // re-try the task with new SolrClient
      future = executor.submit(task);
      try {
        Object result = future.get(timeoutSecs, TimeUnit.SECONDS);
        if (result != null) {
          Boolean tf = (Boolean) result;
          sentBatchOk = tf.booleanValue();
          if (sentBatchOk) {
            incrementCounter(Counters.BATCHES_RECOVERED, 1);
          }
        }
      } catch (Exception secondTryFailed) {
        log.error("Failed to send batch with timeout " + timeoutSecs + " secs due to: " + secondTryFailed);
        incrementCounter(Counters.BATCHES_FAILED, 1);
        try {
          future.cancel(true);
        } catch (Exception ignore) {
        }
      }

    } catch (Exception exc) {
      log.error("Failed to send batch with timeout " + timeoutSecs + " secs due to: " + exc);
      incrementCounter(Counters.BATCHES_FAILED, 1);
      try {
        future.cancel(true);
      } catch (Exception ignore) {
      }
    }

    return sentBatchOk;
  }

  protected JSONObject tupleToJsonDoc(Tuple input) {

    if (input.size() < fieldNamesFromSchema.size()) {
      // schema mis-match? ignore this doc so that we don't pollute our
      // index with malformed data
      incrementCounter(Counters.NUM_INPUT_TUPLES_NOT_MATCHING_SCHEMA, 1);
      return null;
    }

    JSONObject doc = new JSONObject();
    int size = input.size();
    try {
      for (int i = 0; i < size; i++) {
        Object value = input.get(i);
        if (value != null) {
          String fieldName = fieldNamesFromSchema.get(i);
          if (value instanceof DataBag) {
            DataBag bag = (DataBag) value;
            JSONArray list = new JSONArray();
            for (Tuple t : bag) {
              list.add(t.get(0));
            }
            doc.put(fieldName, list);
          } else {
            doc.put(fieldName, value);
          }
        }
      }
    } catch (ExecException ee) {
      incrementCounter(Counters.NUM_TUPLE_TO_JSON_CONVERSION_ERRORS, 1L);
      log.error(String.format("Failed to convert Tuple %s to JSON Object due to: %s",
              tupleAsStr(input), ee.getMessage()), ee);
      return null;
    }

    doc.put("created", ISO_8601_DATE_FMT.format(new Date()));

    return doc;
  }

  protected void index(JSONObject doc) throws Exception {
    JSONArray list = new JSONArray();
    list.add(doc);
    pipelineClient.postDocsToPipeline(list);
  }

  protected void commit() {
  }

  protected String tupleAsStr(Tuple input) {
    String tupleAsStr = "??";
    if (input != null) {
      try {
        tupleAsStr = input.toDelimitedString("\t");
      } catch (ExecException ee) {
      }
    }
    return tupleAsStr;
  }

  /**
   * Error handling for batch mode and single doc mode.
   */
  protected void handleSolrServerException(Tuple input, Exception e) throws IOException {
    Throwable rootCause = SolrException.getRootCause(e);

    if (batchSize > 1) {
      // for batches, all we can do is fail
      int count = (batch != null ? batch.size() : 1);
      log.error(String.format("Failed to add batch containing %d inserts due to: %s", count, rootCause.getMessage()), rootCause);

      log.error("Attempting to add batch docs one-by-one ... ");
      if (batch != null) {
        int recovered = 0; // how many in the batch were we able to recover?
        int errorsWhileRecovering = 0;
        for (int j=0; j < batch.size(); j++) {
          JSONObject doc = (JSONObject)batch.get(j);
          try {
            index(doc);
            ++recovered;
          } catch (Exception ex) {
            ++errorsWhileRecovering;

            // note: don't call handleSolrServerException here ... handle ex manually
            String docId = (String) doc.get("id");
            log.error(String.format("Failed to index document %s due to %s", docId, ex.getMessage()), ex);
            incrementCounter(Counters.NUM_FAILURES, 1);

            if (errorsWhileRecovering < 10) {
              if (reporter != null) {
                try {
                  Counter counter = reporter.getCounter("FusionIndexPipelineStoreFunc-DocError", String.valueOf(docId));
                  if (counter != null) {
                    counter.increment(1);
                  }
                } catch (Exception ignore) {
                }
              }
            } // else there's something really wrong so don't flood the jobtracker with counters!!!
          }
        }
        batch.clear();
        log.info(String.format("Recovered %d of %d docs after failed batch.", recovered, count));
      }
    } else {
      String tupleAsStr = "??";
      if (input != null) {
        try {
          tupleAsStr = input.toDelimitedString("\t");
        } catch (ExecException ee) {
        }
      }

      // skip records that produce duplicate key and foreign key errors
      String errMsg = String.valueOf(e.getMessage()).toLowerCase().replaceAll("\\s+", " ");
      String rootCauseErrMsg = String.valueOf(rootCause.getMessage()).toLowerCase().replaceAll("\\s+", " ");
      if (!errMsg.equals(rootCauseErrMsg)) {
        errMsg += " " + rootCauseErrMsg;
      }

      // we don't know why this failed ... so we'll just have to throw it out
      incrementCounter(Counters.NUM_FAILURES, 1L);

      if (reporter != null && input != null) {
        try {
          String docId = (String) input.get(0);
          Counter counter = reporter.getCounter("FusionIndexPipelineStoreFunc-DocError", String.valueOf(docId));
          if (counter != null) {
            counter.increment(1);
          }
        } catch (Exception ignore) {
        }
      }

      log.error(String.format("Failed to index document %s due to %s", tupleAsStr, rootCause.getMessage()), rootCause);
    }
  }

  class SolrOutputFormat extends OutputFormat<NullWritable, NullWritable> {

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
      // IGNORE
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
      return new OutputCommitter() {

        @Override
        public void abortTask(TaskAttemptContext context) throws IOException {
        }

        @Override
        public void commitTask(TaskAttemptContext context) throws IOException {
          log.info("Committing write to FusionIndexPipelineStoreFunc task " + context.getTaskAttemptID());
          if (FusionIndexPipelineStoreFunc.this.batch != null && FusionIndexPipelineStoreFunc.this.batch.size() > 0) {
            // we have one final batch to commit
            try {
              log.info("Executing final batch of size " + FusionIndexPipelineStoreFunc.this.batch.size());

              if (sendBatchWithTimeout(batch, 600)) {
                incrementCounter(Counters.NUM_DOCS_INDEXED, FusionIndexPipelineStoreFunc.this.batch.size());
              }

              if (reporter != null) {
                reporter.progress();
              }

              batch.clear();
            } catch (Exception e) {
              FusionIndexPipelineStoreFunc.this.handleSolrServerException(null, e);
            }
          }

          // reset status counters
          docCount = 0;
          batchCount = 0;
        }

        @Override
        public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
          return true;
        }

        @Override
        public void cleanupJob(JobContext context) throws IOException {
          // IGNORE
        }

        @Override
        public void setupJob(JobContext context) throws IOException {
          // IGNORE
        }

        @Override
        public void setupTask(TaskAttemptContext context) throws IOException {
          // IGNORE
        }
      };
    }

    @Override
    public RecordWriter<NullWritable, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
      // We don't use a record writer to write to Solr
      return new RecordWriter<NullWritable, NullWritable>() {
        @Override
        public void close(TaskAttemptContext context) {
          if (FusionIndexPipelineStoreFunc.this.batch != null && FusionIndexPipelineStoreFunc.this.batch.size() > 0) {
            log.info("In getRecordWriter.close(), committing write to FusionIndexPipelineStoreFunc task " + context.getTaskAttemptID());
            // we have one final batch to commit
            try {
              log.info("In getRecordWriter.close(), executing final batch of size " + FusionIndexPipelineStoreFunc.this.batch.size());
              if (sendBatchWithTimeout(batch, 600)) {
                incrementCounter(Counters.NUM_DOCS_INDEXED, FusionIndexPipelineStoreFunc.this.batch.size());
              }

              if (reporter != null) {
                reporter.progress();
              }

              batch.clear();
            } catch (Exception e) {
              log.error("Failure in RecordWriter.close()", e);
            }
          }

          // commit all un-committed docs now ...
          FusionIndexPipelineStoreFunc.this.commit();

          // reset status counters
          FusionIndexPipelineStoreFunc.this.docCount = 0;
          FusionIndexPipelineStoreFunc.this.batchCount = 0;
        }

        @Override
        public void write(NullWritable k, NullWritable v) {
          // Noop
        }
      };
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public OutputFormat getOutputFormat() throws IOException {
    return new SolrOutputFormat();
  }

  /**
   * Initialize the connection to Solr
   */
  @SuppressWarnings("unchecked")
  @Override
  public void prepareToWrite(RecordWriter writer) throws IOException {
    try {
      pipelineClient = new HttpIndexPipelineClient(endpoints);
    } catch (Exception e) {
      log.error("Unable to connect to: " + endpoints);
      throw new IOException("Unable to connect to: " + endpoints, e);
    }

    if (batch != null) {
      batch.clear();
    } else {
      batch = new JSONArray();
    }
  }

  @Override
  public void setStoreFuncUDFContextSignature(String signature) {
    myUDFContextSignature = signature;
  }

  @Override
  public void checkSchema(ResourceSchema s) throws IOException {
    if (myUDFContextSignature == null) {
      throw new IllegalStateException("No UDFContext Signature provided to this UDF! Cannot store field names!");
    }

    ResourceSchema.ResourceFieldSchema[] fields = s.getFields();
    if (fields == null || fields.length == 0) {
      throw new IOException("Input field names not available from schema during front-end processing! FusionIndexPipelineStoreFunc must have field names!");
    }

    List<String> fieldNames = new ArrayList<String>(fields.length);
    for (int f = 0; f < fields.length; f++) {
      fieldNames.add(fields[f].getName());
    }

    // Save the fieldIndexToType Mapping in the UDFContext, keyed by our
    // UDFContext Signature so we don't step on other FusionIndexPipelineStoreFunc UDFs
    Properties udfProps =
            UDFContext.getUDFContext().getUDFProperties(getClass(), new String[]{myUDFContextSignature});
    udfProps.put(FIELD_NAMES_FROM_SCHEMA_PROPS_KEY, fieldNames);

    log.info(String.format("Saved %s=%s into UDFContext using signature: %s",
            FIELD_NAMES_FROM_SCHEMA_PROPS_KEY, String.valueOf(fieldNames), myUDFContextSignature));
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    // IGNORE since we are writing records to an HTTP endpoint
  }

  // utility method for incrementing a Hadoop counter
  private void incrementCounter(Counters key, long howMany) {
    if (reporter != null) {
      Counter counter = reporter.getCounter(key);
      if (counter != null) {
        try {
          counter.increment(howMany);
        } catch (java.lang.IncompatibleClassChangeError wtf) {
          System.err.println("ERROR: " + wtf);
        }
      }
    }
  }
}