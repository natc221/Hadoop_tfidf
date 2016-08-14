package indexer.indexing.compiler;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DocCountReducer extends Reducer<Text, Text, Text, Text> {
	
	public static final String KEY_SPLIT = DocCountMapper.KEY_SPLIT;
	public static final String FREQ = "freq";
	public static final String MAX_FREQ = "maxFreq";
	public static final String NUM_DOCS = "numDocs";

	private Text emitKey = new Text();
	private Text emitVal = new Text();
	
	public void reduce(Text docIDText, Iterable<Text> values, 
			Context context) throws IOException, InterruptedException {
		String docID = docIDText.toString();
		Map<String, Integer> wordFreqs = new HashMap<>();
		
		int maxFreq = -1;
		String numDocs = "";
		for (Text val : values) {
			String[] wordFreqSplit = val.toString().split(KEY_SPLIT, 3);
			String wordID = wordFreqSplit[0];
			int freq = 0;
			try {
				freq = Integer.parseInt(wordFreqSplit[1]);
			} catch (NumberFormatException e) {}
			numDocs = wordFreqSplit[2];
			
			wordFreqs.put(wordID, freq);
			if (freq > maxFreq) {
				maxFreq = freq;
			}
		}
		
		for (Map.Entry<String, Integer> entry : wordFreqs.entrySet()) {
			String wordID = entry.getKey();
			int freq = entry.getValue();
			emitKey.set(wordID + KEY_SPLIT + docID);
			emitVal.set(freq + KEY_SPLIT + maxFreq + KEY_SPLIT + numDocs);
			context.write(emitKey, emitVal);
		}
	}
}
