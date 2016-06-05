package dbs.bigdata.flink.pprl.functions;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import dbs.bigdata.flink.pprl.data.Person;

/**
 * Class for splitting the records of the data set (i.e. the quasi identifiers (qids)) into n-gram tokens.
 * 
 * @author mfranke
 */
public class NGramTokenizer 
	implements FlatMapFunction<Person, Tuple2<String, String>> {

	private static final long serialVersionUID = -8819111750339989196L;
	private int ngram;
	private static int idCounter;
	
	/**
	 * @param ngram
	 * 		-> the size n of the n-grams
	 * @throws Exception
	 * 		-> an excption is thrown if the value of ngram is smaller then one.
	 */
	public NGramTokenizer(int ngram) throws Exception{
		if (ngram >= 1){
			this.ngram = ngram;
			idCounter = 0;
		}
		else{
			throw new Exception();
		}
	}
	
	@Override
	public void flatMap(Person value, Collector<Tuple2<String, String>> out)
			throws Exception {
		String qids = value.getConcatenatedAttributes();
		
		// normalize
		String normalizedQids = qids.toLowerCase();
		normalizedQids = normalizedQids.replaceAll("\\s+", "");
	
		ArrayList<String> tokens = new ArrayList<String>();
		
		String token = "";
		char[] chars = normalizedQids.toCharArray();
		
		for (int i = 0; i <= chars.length - this.ngram; i++){
			for (int j = i; j < i + this.ngram; j++){
				token = token + chars[j];
			}
			tokens.add(token);
			token = "";
		}

		// emit the pairs
		for (int tokenPointer = 0; tokenPointer < tokens.size(); tokenPointer++){
			out.collect(new Tuple2<String, String>(String.valueOf(idCounter), tokens.get(tokenPointer)));
		}
		idCounter++;
	}
	
	public int getNGramValue(){
		return this.ngram;
	}
	
	public void setNGramValue(int ngram){
		this.ngram = ngram;
	}
	
	public static void resetIdCounter(){
		idCounter = 0;
	}
}