package indexer.compiler;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ComputeScoreReducer extends Reducer<Text, Text, Text, Text> {
	
	public static final String KEY_SPLIT = ComputeScoreMapper.KEY_SPLIT;
	public static final String SCORE = "score";
	public static final double ALPHA = 0.5;

	private Text emitKey = new Text();
	private Text emitVal = new Text();
	
	public void reduce(Text wordIDText, Iterable<Text> values, 
			Context context) throws IOException, InterruptedException {
		String wordID = wordIDText.toString();
		int docsWithWord = 0;
		
		List<String> savedVals = new LinkedList<>();
		for (Text val : values) {
			docsWithWord++;
			savedVals.add(val.toString());
		}
		
		for (String val : savedVals) {
			String[] vals = val.toString().split(KEY_SPLIT, 4);
			if (vals.length == 4) {
				String docID = vals[0];
				
				try {
					int freq = Integer.parseInt(vals[1]);
					int maxFreq = Integer.parseInt(vals[2]);
					int numDocs = Integer.parseInt(vals[3]);
					double tfidf = computeTfIdf(freq, maxFreq, docsWithWord, numDocs);
					
					String valString = docID + KEY_SPLIT
										+ tfidf + KEY_SPLIT
										+ freq + KEY_SPLIT
										+ maxFreq + KEY_SPLIT
										+ numDocs + KEY_SPLIT;
					
					emitKey.set(wordID);
					emitVal.set(valString);
					context.write(emitKey, emitVal);
				} catch (NumberFormatException e) {
					// ignore entry
				}
			}
		}
	}
	
	private double computeTfIdf(int freq, int maxFreq, int docsWithWord, int numDocs) {
		double tf = ALPHA;
		tf += ((1 - ALPHA) * freq) / maxFreq;

		double idf = Math.log((double) numDocs / docsWithWord);
		return tf * idf;
	}
}
