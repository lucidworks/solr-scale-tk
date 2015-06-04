-- REGISTER 's3://solr-scale-tk/pig/hadoop-lws-job-1.2.0-SNAPSHOT-rc2-0.jar';
REGISTER 's3://solr-scale-tk/pig/hadoop-lws-job-2.0.1-0-0-hadoop2.jar';

set solr.zkhost '$zkHost';
set solr.collection '$collection';
set lww.buffer.docs.size $batch;
set lww.commit.on.close true;

SET mapred.map.tasks.speculative.execution false;
SET mapred.reduce.tasks.speculative.execution false;
SET mapred.child.java.opts -Xmx1g;

SET mapred.task.timeout 12000000;
SET mapred.max.tracker.failures 20;
SET mapred.map.max.attempts 20;

-- s3://solr-scale-tk/pig/s3_to_solr.pig
-- s3://solr-scale-tk/pig/output/syn130m/
-- s3://solr-scale-tk/pig/output/syn_sample_10m
-- s3://thelabdude/syn_sample_10m
-- s3://solr-scale-tk/pig/output/foo/
-- -p RED=44 -p collection=perf10x2 -p batch=1500 -p zkHost=ec2-54-147-190-173.compute-1.amazonaws.com:2181,ec2-54-159-24-141.compute-1.amazonaws.com:2181,ec2-54-159-4-235.compute-1.amazonaws.com:2181/rpf

data = load '$INPUT' using PigStorage() as (id: chararray,
  integer1_i: int,
  integer2_i: int,
  long1_l: long,
  long2_l: long,
  float1_f: float,
  float2_f: float,
  double1_d: double,
  double2_d: double,
  timestamp1_tdt: chararray,
  timestamp2_tdt: chararray,
  string1_s: chararray,
  string2_s: chararray,
  string3_s: chararray,
  boolean1_b: chararray,
  boolean2_b: chararray,
  text1_en: chararray,
  text2_en: chararray,
  text3_en: chararray,
  random_bucket: float);

to_sort = foreach data generate id,
  'integer1_i', integer1_i,
  'integer2_i', integer2_i,
  'long1_l', long1_l,
  'long2_l', long2_l,
  'float1_f', float1_f,
  'float2_f', float2_f,
  'double1_d', double1_d,
  'double2_d', double2_d,
  'timestamp1_tdt', timestamp1_tdt,
  'timestamp2_tdt', timestamp2_tdt,
  'string1_s', string1_s,
  'string2_s', string2_s,
  'boolean1_b', boolean1_b,
  'text1_en', text1_en,
  'text3_en', text3_en;
  
to_solr = order to_sort by id ASC parallel $RED;
store to_solr into 's3://solr-scale-tk/pig/output/foo' using com.lucidworks.hadoop.pig.SolrStoreFunc();
