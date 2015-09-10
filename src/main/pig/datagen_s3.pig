REGISTER '$SSTK_JAR'

DEFINE GenKeys com.lucidworks.pig.GenKeys('$NUM_KEYS','5150','100');
DEFINE SyntheticDoc com.lucidworks.pig.SyntheticDoc('$NUM_DOCS_PER_KEY','5150');

keys_in = load '$INPUT' using PigStorage() as (key_base:chararray);
keys = foreach keys_in generate FLATTEN(GenKeys(key_base)) as (key:chararray, key_bucket:int);
by_base = foreach (group keys by key_bucket parallel 100) generate FLATTEN($1) as (key:chararray, key_bucket:int);
docs = foreach by_base generate FLATTEN(SyntheticDoc(key)), ROUND(RANDOM()*100) as random_bucket;
store docs into '$OUTPUT' using PigStorage();
