CREATE EXTERNAL TABLE s3Table (word_id string, doc_id string, tfidf double, freq bigint, maxfreq bigint, numdocs bigint)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
LOCATION '${hiveconf:S3URI}'
;

CREATE EXTERNAL TABLE docIndex (word_id string, doc_id string, tfidf double, freq bigint)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES (
"dynamodb.table.name" = "${hiveconf:DYNAMO_TABLE_NAME}",
"dynamodb.column.mapping" = "word_id:word_id,doc_id:doc_id,tfidf:tfidf,freq:freq"
);

SET dynamodb.throughput.write.percent=0.9;
SET mapred.skip.mode.enabled = true;
set hive.merge.mapfiles=false;

INSERT OVERWRITE TABLE docIndex
SELECT word_id, doc_id, tfidf, freq
FROM s3Table
WHERE word_id != '-'
AND freq >= 5
;
