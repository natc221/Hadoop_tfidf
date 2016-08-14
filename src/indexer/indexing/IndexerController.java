package indexer.indexing;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.ClusterStatus;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

public class IndexerController {
	
	private static final String WORD_COUNT_CLASS = "indexer.indexing.compiler.WordCountDriver";
	private static final String WORD_COUNT_INPUT = IndexerConfig.INPUT_DATA_URI;
	private static final String WORD_COUNT_OUTPUT = IndexerConfig.INTERMED_URI + "word-count";
	
	private static final String DOC_COUNT_CLASS = "indexer.indexing.compiler.DocCountDriver";
	private static final String DOC_COUNT_INPUT = WORD_COUNT_OUTPUT;
	private static final String DOC_COUNT_OUTPUT = IndexerConfig.INTERMED_URI + "doc-count";
	
	private static final String COMPUTE_SCORE_CLASS = "indexer.indexing.compiler.ComputeScoreDriver";
	private static final String COMPUTE_SCORE_INPUT = DOC_COUNT_OUTPUT;
	private static final String COMPUTE_SCORE_OUTPUT = IndexerConfig.OUTPUT_DATA_URI;
	
	private AmazonElasticMapReduceClient emr;
	private String clusterId;
	private int numDocs = 1;
	private StepFactory stepFactory;
	
	public IndexerController(String clusterId) {
		AWSCredentials credentials = IndexerConfig.getAWSCredentials();
		emr = new AmazonElasticMapReduceClient(credentials);
		this.clusterId = clusterId;
		this.stepFactory = new StepFactory();
	}
	
	public void setNumDocs(int numDocs) {
		this.numDocs = numDocs;
		System.out.println("numDocs set as: " + numDocs);
	}
	
	/**
	 * @return
	 * 		whether cluster is ready for steps
	 */
	public boolean createCluster() {
		if (clusterId == null) {
			StepConfig installHiveStep = getInstallStep();
			
			RunJobFlowRequest request = new RunJobFlowRequest()
			.withName("Indexer Cluster")	
			.withAmiVersion("3.11.0")
			.withServiceRole(IndexerConfig.SERVICE_ROLE)
			.withJobFlowRole(IndexerConfig.JOB_FLOW_ROLE)
			.withLogUri(IndexerConfig.LOG_PATH)
			.withSteps(installHiveStep)
			.withInstances(new JobFlowInstancesConfig()
			.withEc2KeyName(IndexerConfig.EC2_KEY_NAME)
			.withInstanceCount(IndexerConfig.HADOOP_INSTANCES)
			.withKeepJobFlowAliveWhenNoSteps(true)
			.withMasterInstanceType(IndexerConfig.MASTER_INSTANCE_TYPE)
			.withSlaveInstanceType(IndexerConfig.SLAVE_INSTANCE_TYPE)
					);
			RunJobFlowResult result = emr.runJobFlow(request);
			clusterId = result.getJobFlowId();
			System.out.println("created cluster with id: " + clusterId);
			return false;
		} else {
			System.out.println("using cluster with id: " + clusterId);
			return true;
		}
	}
	
	public String getClusterStatus() {
		DescribeClusterResult result = 
				emr.describeCluster(new DescribeClusterRequest().withClusterId(clusterId));
		ClusterStatus status = result.getCluster().getStatus();
		String state = status.getState();
		System.out.println("current cluster state: " + state);
		return state;
	}
	
	public void printClusterStatus() {
		System.out.println("current cluster state: " + getClusterStatus());
	}
	
	public void runIndexing() {
		System.out.println("Submitting steps to cluster");
		AddJobFlowStepsRequest req = new AddJobFlowStepsRequest();
		req.withJobFlowId(clusterId);
		
		List<StepConfig> stepConfigs = new ArrayList<>();
		stepConfigs.add(getWordCountStep());
		stepConfigs.add(getDocCountStep());
		stepConfigs.add(getComputeScoreStep());
		stepConfigs.add(getMoveIndexStep());
		req.withSteps(stepConfigs);
		AddJobFlowStepsResult result = emr.addJobFlowSteps(req);
		System.out.println("steps submitted");
	}
	
	private StepConfig getInstallStep() {
		StepConfig step = new StepConfig()
				.withName("Install Hive Step")
				.withHadoopJarStep(stepFactory.newInstallHiveStep())
				.withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW);
		return step;
	}
	
	private StepConfig getWordCountStep() {
		StepConfig step = new StepConfig()
							.withName("Word Count Step")
							.withActionOnFailure(ActionOnFailure.CANCEL_AND_WAIT);
		HadoopJarStepConfig hadoopConfig = new HadoopJarStepConfig(IndexerConfig.INDEXER_JAR);
		hadoopConfig.setMainClass(WORD_COUNT_CLASS);
		List<String> args = new ArrayList<>();
		args.add(WORD_COUNT_INPUT);
		args.add(WORD_COUNT_OUTPUT);
		hadoopConfig.setArgs(args);
		step.withHadoopJarStep(hadoopConfig);
		return step;
	}
	
	public StepConfig getDocCountStep() {
		StepConfig step = new StepConfig()
							.withName("Doc Count Step")
							.withActionOnFailure(ActionOnFailure.CANCEL_AND_WAIT);
		HadoopJarStepConfig hadoopConfig = new HadoopJarStepConfig(IndexerConfig.INDEXER_JAR);
		hadoopConfig.setMainClass(DOC_COUNT_CLASS);
		List<String> args = new ArrayList<>();
		args.add(DOC_COUNT_INPUT);
		args.add(DOC_COUNT_OUTPUT);
		args.add(Integer.toString(numDocs));
		hadoopConfig.setArgs(args);
		step.withHadoopJarStep(hadoopConfig);
		return step;
	}
	
	public StepConfig getComputeScoreStep() {
		StepConfig step = new StepConfig()
							.withName("Compute Score Step")
							.withActionOnFailure(ActionOnFailure.CANCEL_AND_WAIT);
		HadoopJarStepConfig hadoopConfig = new HadoopJarStepConfig(IndexerConfig.INDEXER_JAR);
		hadoopConfig.setMainClass(COMPUTE_SCORE_CLASS);
		List<String> args = new ArrayList<>();
		args.add(COMPUTE_SCORE_INPUT);
		args.add(COMPUTE_SCORE_OUTPUT);
		hadoopConfig.setArgs(args);
		step.withHadoopJarStep(hadoopConfig);
		return step;
	}
	
	public StepConfig getMoveIndexStep() {
		StepFactory stepFactory = new StepFactory();
		String[] args = new String[] {
			"-hiveconf", "S3URI=" + IndexerConfig.OUTPUT_DATA_URI,
			"-hiveconf", "DYNAMO_TABLE_NAME=" + IndexerConfig.OUTPUT_DATA_URI,
		};
		StepConfig step = new StepConfig()
			.withName("Move Index Step")
			.withActionOnFailure(ActionOnFailure.CANCEL_AND_WAIT)
			.withHadoopJarStep(stepFactory.newRunHiveScriptStep(
					IndexerConfig.HIVE_SCRIPT_URI, args));
		return step;
	}
	
	
}