package dbs.bigdata.flink.pprl;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

public class NGramTokenizer implements FlatMapFunction<Tuple5<String, String, String, String, String>, Object> {

	private static final long serialVersionUID = -8819111750339989196L;
	private int ngram;
	
	NGramTokenizer(int ngram) throws Exception{
		if (ngram >= 1){
			this.ngram = ngram;
		}
		else{
			throw new Exception();
		}
	}
	
	@Override
	public void flatMap(Tuple5<String, String, String, String, String> value, Collector<Object> out) throws Exception {
		String name = value.f1 + " " + value.f2;
		
		// normalize
		String normalizedName = name.toLowerCase();
		
		ArrayList<String> tokens = new ArrayList<String>();
		
		String token = "";
		char[] chars = normalizedName.toCharArray();
		
		for (int i = 0; i <= chars.length - this.ngram; i++){
			for (int j = i; j < i + this.ngram; j++){
				token = token + chars[j];
			}
			tokens.add(token);
			token = "";
		}

		// emit the pairs
		for (int tokenPointer = 0; tokenPointer < tokens.size(); tokenPointer++){
			out.collect(new Tuple2<String, String>(value.f0, tokens.get(tokenPointer)));
		}
	}
	
	public int getNGramValue(){
		return this.ngram;
	}
	
	public void setNGramValue(int ngram){
		this.ngram = ngram;
	}
}