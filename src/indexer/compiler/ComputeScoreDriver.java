package indexer.compiler;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ComputeScoreDriver {
	
	public static void main(String[] args) throws IOException, 
	ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Compute Score");
		job.setJarByClass(ComputeScoreDriver.class);
		job.setMapperClass(ComputeScoreMapper.class);
		job.setReducerClass(ComputeScoreReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}

}
