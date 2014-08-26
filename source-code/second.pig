-- The script needs the following jars:
-- jwat-common-1.0.0.jar
-- jwat-gzip-1.0.0.jar
-- jwat-warc-1.0.0.jar
-- warcutils.jar

-- The jwat-*.jar's are in HDFS on a shared locations:

 REGISTER 'hdfs://head02.hathi.surfsara.nl/user/naward02/libs/*.jar'; 
 REGISTER 'hdfs://head02.hathi.surfsara.nl/jars/jwat-*.jar';
SET pig.exec.nocombiner true;

in = LOAD 'temp-in-MAIN' AS (domain:chararray, langs:bag { (lang:chararray) });

next = FOREACH in GENERATE domain, flatten($1) as langs;

grouped = GROUP next by (domain, langs);

temp = FOREACH grouped GENERATE flatten($0), COUNT($1) as count;

grouped2 = GROUP temp by group::domain;

flat = FOREACH grouped2 GENERATE flatten($1);

store flat into 'test-out-29071737';