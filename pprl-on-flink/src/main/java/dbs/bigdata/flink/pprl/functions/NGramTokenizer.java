package dbs.bigdata.flink.pprl.functions;

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
	
	private static final String PADDING_CHARACTER = "#";
	
	private int ngram;
	private boolean withTokenPadding;
	
	/**
	 * @param ngram
	 * 		-> the size n of the n-grams
	 * 
	 * @param withTokenPadding 
	 * 		-> if true, a padding character is used for the tokens.
	 * 
	 * @throws Exception
	 * 		-> an exception is thrown if the value of ngram is smaller then one.
	 */
	public NGramTokenizer(int ngram, boolean withTokenPadding) throws Exception{
		if (ngram >= 1){
			this.ngram = ngram;
			this.withTokenPadding = withTokenPadding;
		}
		else{
			throw new Exception();
		}
	}
	
	/**
	 * Transform {@link Person} objects into a set of (Id, Token) tuples.
	 */
	@Override
	public void flatMap(Person value, Collector<Tuple2<String, String>> out)
			throws Exception {
		
		String[] qids = value.getAttributeValues();
		
		for (String attribute : qids){
			// normalize
			String normalizedAttribute = attribute.toLowerCase();
			normalizedAttribute = normalizedAttribute.replaceAll("\\s+", "");
			
			if (normalizedAttribute.length() > 0){
				if (this.withTokenPadding){
					for (int i = 1; i < this.ngram; i++){
						normalizedAttribute = PADDING_CHARACTER + normalizedAttribute + PADDING_CHARACTER;
					}
				}
			
				String token = "";
				char[] chars = normalizedAttribute.toCharArray();
				
				for (int i = 0; i <= chars.length - this.ngram; i++){
					for (int j = i; j < i + this.ngram; j++){
						token = token + chars[j];
					}
					out.collect(new Tuple2<String, String>(value.getId(), token));
					token = "";
				}
			}
		}
	}
	
	public int getNGramValue(){
		return this.ngram;
	}
	
	public void setNGramValue(int ngram){
		this.ngram = ngram;
	}

	public boolean isWithTokenPadding() {
		return withTokenPadding;
	}

	public void setWithTokenPadding(boolean withTokenPadding) {
		this.withTokenPadding = withTokenPadding;
	}

}