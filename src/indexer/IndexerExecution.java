package indexer;

import java.util.concurrent.TimeUnit;

public class IndexerExecution {

	public static void main(String[] args) {
		if (args.length < 1 || args.length > 2) {
			System.out.println("Usage: java IndexerExecution <numDocs> [EMR cluster ID]");
			System.exit(1);
		}
		try {
			int numDocs = Integer.parseInt(args[0]);
			String clusterID = args.length > 1 ? args[1] : null;

			IndexerController indexer = new IndexerController(clusterID);
			indexer.createCluster();
			indexer.setNumDocs(numDocs);
			submitJobs(indexer);

			System.out.println("Indexing complete!");
		} catch (NumberFormatException e) {
			System.out.println("numDocs must be an integer");
			System.out.println("Usage: java IndexerExecution <numDocs> [EMR cluster ID]");
		}
	}
	
	private static void submitJobs(IndexerController indexer) {
		String currStatus = indexer.getClusterStatus();
		// wait until cluster has started
		while ("STARTING".equals(currStatus)) {
			System.out.println("sleeping...");
			try {
				TimeUnit.SECONDS.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			currStatus = indexer.getClusterStatus();
			System.out.println("EMR cluster starting, status: " + currStatus);
		}
		// submit steps
		indexer.runIndexing();
	}

}
