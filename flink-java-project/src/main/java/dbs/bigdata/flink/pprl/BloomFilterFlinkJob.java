package dbs.bigdata.flink.pprl;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

/**
 * 
 *
 */
public class BloomFilterFlinkJob {

	public static final String PATH_TO_DATA_FILE = "persons";
	public static final String LINE_DELIMITER = "##//##";
	public static final String FIELD_DELIMITER = "#|#";
	
	//	Program
	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		/*
		// get input data
		DataSet<String> text = env.fromElements(
				"To be, or not to be,--that is the question:--",
				"Whether 'tis nobler in the mind to suffer",
				"The slings and arrows of outrageous fortune",
				"Or to take arms against a sea of troubles,"
				);
		
		DataSet<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new LineSplitter())
				// group by the tuple field "0" and sum up tuple field "1"
				.groupBy(0)
				.sum(1);

		// execute and print result
		counts.print();
		*/
		

		// get input data, read all fields
		DataSet<Tuple4<String, String, String, String>> persons =
		  env.readCsvFile(PATH_TO_DATA_FILE)
		    .lineDelimiter(LINE_DELIMITER)
		    .fieldDelimiter(FIELD_DELIMITER)
		    .types(String.class, String.class, String.class, String.class);

		
		ArrayList<String> names = new ArrayList<String>();
		
		List<Tuple4<String, String, String, String>> personList = persons.collect();
		if (!personList.isEmpty()){
			for (int i = 0; i < personList.size(); i++){
				Tuple4<String, String, String, String> tupel = personList.get(i);
				names.add(tupel.f0 + " " + tupel.f1);
			}
		}
		
		ArrayList<String> tokens = getTokensFromNames(names);
		System.out.println(tokens.toString());
		
		//BloomFilter bloomFilter = new BloomFilter(100, 32);
	}

	public static ArrayList<String> getTokensFromNames(ArrayList<String> names){
		ArrayList<String> tokens = new ArrayList<String>();
		
		for (String s : names){
			System.out.println(s);
			String token = "";
			int size = 3;
			char[] chars = s.toCharArray();
			
			for (int i = 0; i <= chars.length - size; i++){
				for (int j = i; j < i + size; j++){
					token = token + chars[j];
				}
				tokens.add(token);
				token = "";
			}
		}
		
		return tokens;
	}
	
	//
	// 	User Functions
	//
	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
}
