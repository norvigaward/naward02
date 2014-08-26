-- The script needs the following jars:
-- jwat-common-1.0.0.jar
-- jwat-gzip-1.0.0.jar
-- jwat-warc-1.0.0.jar
-- warcutils.jar

-- The jwat-*.jar's are in HDFS on a shared locations:

 REGISTER 'hdfs://head02.hathi.surfsara.nl/user/naward02/libs/*.jar'; 
 REGISTER 'hdfs://head02.hathi.surfsara.nl/jars/jwat-*.jar';

DEFINE WarcFileLoader nl.surfsara.warcutils.pig.WarcSequenceFileLoader();
SET pig.exec.nocombiner true;

-- Load the data, here we use all the SequenceFiles from the TEST set.
 in = LOAD '/data/public/common-crawl/crawl-data/CC-MAIN-2014-10/*/*/seq/*'
-- in = LOAD '*.seq'
     USING WarcFileLoader AS (domain:chararray, langs:bag { (lang:chararray) });

store in into 'temp-in-MAIN';
