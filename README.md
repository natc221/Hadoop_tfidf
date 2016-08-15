##Hadoop TF-IDF

Computes TF-IDF scores for words associated with a set of documents. The intended execution is on [AWS Elastic MapReduce](https://aws.amazon.com/emr/), but the inner Mapper and Reducer classes can be used for any Hadoop deployment.

#####Input format
```
documentID [tab] space-delimited words
```
#####Output format

```
word [tab] documentID [tab] TF-IDF [tab] word frequency [tab] maximum word frequency [tab] number of documents
```

####Configuration
- Create a bucket in S3, and create an input, output, intermediate and jars folder within that bucket. Place input files in the input folder.
- Edit the bucket name and other S3 URIs in `indexer.IndexerConfig` to reflect the folders created
- Compile an executable JAR of the source files, and upload it to the jars folder on S3
- After compilation, run `java IndexerExecution <numDocs> [EMR cluster ID]`. The cluster ID is optional, and can be included if a cluster has already been created through other means.

####Hadoop Steps Description
- WordCount
	- Count the word frequencies for each word in a document
	- Assumes words are space-delimited. Pre-processing is necessary if more complex tokenization is needed
- DocCount
	- Finds the maximum word frequency in each document
	- Note that the total number of documents (N) is also outputted here. This count is fed in as an argument to this step, and assumes the count can be easily computed elsewhere without MapReduce
- ComputeScore
	- Computes TF-IDF score for each word in each document
	- Also outputs other statistics used in the overall process that may be useful to the user

####Data migration from S3 to Dynamo
Since the output of Elastic MapReduce is stored in S3, the data cannot be queried in a performant manner. A Hive script is included to migrate the TF-IDF data from S3 to [DynamoDB](https://aws.amazon.com/dynamodb/). This is added as a step in the overall indexing process, seen in `indexer.IndexerController`

####Dependencies
- AWS Java SDK
	- aws-java-sdk
- Apache Commons
	- commons-logging
- Apache Hadoop
	- hadoop-common
	- hadoop-mapreduce-client-core
- Apache HTTP
	- httpclient
	- httpcore
- Jackson
	- jackson-annotations
	- jackson-core
	- jackson-databind
	- jackson-dataformat-cbor
- Joda
	- joda-time