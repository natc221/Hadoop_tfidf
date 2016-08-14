package indexer.indexing.compiler;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ComputeScoreMapper extends Mapper<Object, Text, Text, Text> {
	
	public static final String KEY_SPLIT = "\t";

	private Text emitKey = new Text();
	private Text emitVal = new Text();
	
	public void map(Object key, Text line, Context context) 
			throws IOException, InterruptedException {
		
		String[] cols = line.toString().split(KEY_SPLIT, 5);
		if (cols.length == 5) {
			String wordID = cols[0];
			String docID = cols[1];
			String freq = cols[2];
			String maxFreq = cols[3];
			String numDocs = cols[4];
			
			String valString = docID + KEY_SPLIT
								+ freq + KEY_SPLIT
								+ maxFreq + KEY_SPLIT
								+ numDocs;
			emitKey.set(wordID);
			emitVal.set(valString);
			context.write(emitKey, emitVal);
		}
	}
}
