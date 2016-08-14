package indexer.indexing.compiler;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DocCountMapper extends Mapper<Object, Text, Text, Text> {
	
	public static final String NUM_DOCS_STRING = "numDocs";
	public static final String KEY_SPLIT = "\t";

	private String numDocs;
	
	private Text emitKey = new Text();
	private Text emitVal = new Text();
	
	protected void setup(Context context) {
		Configuration config = context.getConfiguration();
		/*
		 *  hacky way of getting the number of documents, since it is an integer
		 *  see DocCountDriver
		 */
		numDocs = config.get(NUM_DOCS_STRING);
	}
	
	public void map(Object key, Text line, Context context) 
			throws IOException, InterruptedException {
		String[] keyVal = line.toString().split(KEY_SPLIT, 3);
		
		if (keyVal.length == 3) {
			String word = keyVal[0];
			String docID = keyVal[1];
			String wordFreq = keyVal[2];
			
			String valString = word + KEY_SPLIT + wordFreq + KEY_SPLIT + numDocs;
			
			emitKey.set(docID);
			emitVal.set(valString);
			context.write(emitKey, emitVal);
		} else {
			System.out.println("line split on tab failed, split length: " + keyVal.length);
		}
	}
}
