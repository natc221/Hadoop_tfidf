package indexer.indexing;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;

public class IndexerConfig {

	public static final String EC2_KEY_NAME = ""; //TODO
	public static final int HADOOP_INSTANCES = 10;
	public static final String MASTER_INSTANCE_TYPE = "m3.xlarge";
	public static final String SLAVE_INSTANCE_TYPE = "m3.xlarge";
	public static final String SERVICE_ROLE = "EMR_DefaultRole";
	public static final String JOB_FLOW_ROLE = "EMR_EC2_DefaultRole";
	

	public static final String BUCKET_NAME = ""; //TODO
	private static final String BUCKET_URI = "s3://" + BUCKET_NAME + "/";

	public static final String INPUT_DATA_KEY_PREFIX = "input/";
	public static final String INPUT_DATA_BUCKET = BUCKET_NAME + "/" + INPUT_DATA_KEY_PREFIX;
	public static final String INPUT_DATA_URI = BUCKET_URI + INPUT_DATA_KEY_PREFIX;

	public static final String OUTPUT_DATA_KEY_PREFIX = "output/";
	public static final String OUTPUT_DATA_BUCKET = BUCKET_NAME + "/" + OUTPUT_DATA_KEY_PREFIX;
	public static final String OUTPUT_DATA_URI = BUCKET_URI + OUTPUT_DATA_KEY_PREFIX;
	
	public static final String INTERMED_KEY_PREFIX = "intermediate/";
	public static final String INTERMED_URI = BUCKET_URI + INTERMED_KEY_PREFIX;
	
	public static final String LOG_PATH = "s3://"; //TODO
	public static final String INDEXER_JAR = "s3://"; //TODO
	
	public static final String HIVE_SCRIPT_URI = "s3://"; //TODO
	
	public static AWSCredentials getAWSCredentials() {
		return (new ProfileCredentialsProvider()).getCredentials();
	}
}
