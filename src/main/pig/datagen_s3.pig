-- REGISTER 's3://solr-scale-tk/pig/solr-scale-tk-0.1-exe.jar';
REGISTER '/Users/timpotter/dev/lw/projects/solr-scale-tk/target/solr-scale-tk-0.1-exe.jar'

DEFINE GenKeys com.lucidworks.pig.GenKeys('100','5150','100');
DEFINE SyntheticDoc com.lucidworks.pig.SyntheticDoc('100','5150');

keys_in = load '$INPUT' using PigStorage() as (key_base:chararray);
keys = foreach keys_in generate FLATTEN(GenKeys(key_base)) as (key:chararray, key_bucket:int);
by_base = foreach (group keys by key_bucket parallel 100) generate FLATTEN($1) as (key:chararray, key_bucket:int);
docs = foreach by_base generate FLATTEN(SyntheticDoc(key)), ROUND(RANDOM()*100) as random_bucket;
store docs into '$OUTPUT' using PigStorage();
