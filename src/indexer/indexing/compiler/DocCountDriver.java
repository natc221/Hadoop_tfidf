package indexer.indexing.compiler;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DocCountDriver {
	
	public static void main(String[] args) throws IOException, 
	ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
		String numDocs = args[2];
		config.set("numDocs", numDocs);
		
		Job job = Job.getInstance(config, "Doc info step");
		job.setJarByClass(DocCountDriver.class);
		job.setMapperClass(DocCountMapper.class);
		job.setReducerClass(DocCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}

}
