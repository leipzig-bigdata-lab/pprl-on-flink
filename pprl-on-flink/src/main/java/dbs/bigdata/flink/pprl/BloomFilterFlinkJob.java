package dbs.bigdata.flink.pprl;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;

import java.util.ArrayList;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

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

		// get input data, read all fields
		DataSet<Tuple5<String, String, String, String, String>> persons =
		  env.readCsvFile(PATH_TO_DATA_FILE)
		    .lineDelimiter(LINE_DELIMITER)
		    .fieldDelimiter(FIELD_DELIMITER)
		    .types(String.class, String.class, String.class, String.class, String.class);

		final int nGramValue = 2;
		FlatMapOperator<Tuple5<String, String, String, String, String>, Object> tokens = 
				persons.flatMap(new NGramTokenizer(nGramValue));
		
		tokens.print();		
		
		/*
		ArrayList<String> names = new ArrayList<String>();
		
		List<Tuple5<String, String, String, String, String>> personList = persons.collect();
		if (!personList.isEmpty()){
			for (int i = 0; i < personList.size(); i++){
				Tuple5<String, String, String, String, String> tupel = personList.get(i);
				names.add(tupel.f1 + " " + tupel.f2);
			}
		}
		
		ArrayList<String> tokens = getTokensFromNames(names);
		System.out.println(tokens.toString());
		
		//BloomFilter bloomFilter = new BloomFilter(100, 32);
		*/
	}

	/*
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
	*/
}
