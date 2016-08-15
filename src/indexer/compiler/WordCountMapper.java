package indexer.compiler;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	public static final String KEY_SPLIT = "\t";
	public static final String WORD_DELIM = " ";
	
	private static final IntWritable ONE = new IntWritable(1);
	private Text emitKey = new Text();
	
	public void map(Object key, Text line, Context context) 
			throws IOException, InterruptedException {
		String[] cols = line.toString().split(KEY_SPLIT, 2);
		if (cols.length == 2) {
			String docID = cols[0];
			String words = cols[1];
			
			String[] split = words.split(WORD_DELIM);
			for (String wordID : split) {
				String compositeKey = wordID + KEY_SPLIT + docID;
				emitKey.set(compositeKey);
				context.write(emitKey, ONE);
			}
		} else {
			System.out.println("line split on tab failed, split length: " + cols.length);
		}
	}
}
