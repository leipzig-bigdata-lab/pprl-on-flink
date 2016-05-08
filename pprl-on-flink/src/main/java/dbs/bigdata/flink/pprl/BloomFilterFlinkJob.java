package dbs.bigdata.flink.pprl;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.ReduceOperator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;

/**
 * The Flink job for building the bloom filters.
 * 
 * @author thornoff
 * @author mfranke
 */
public class BloomFilterFlinkJob {

	public static final String PATH_TO_DATA_FILE = "persons";
	public static final String LINE_DELIMITER = "##//##";
	public static final String FIELD_DELIMITER = "#|#";
	
	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		/*
		VoterDataLoader voterLoader = new VoterDataLoader(env);
		voterLoader.getAllData().print();
		*/
		
		PersonDataLoader personLoader = new PersonDataLoader(env);
		
		// build the n-grams
		final int nGramValue = 2;
		FlatMapOperator<Tuple5<String, String, String, String, String>, Tuple2<String, String>> tokens = 
				personLoader.getAllData().flatMap(new NGramTokenizer(nGramValue));
		
		// merge the n-grams for the same record
		ReduceOperator<Tuple2<String, String>> reducedTokensById = tokens.groupBy(0).reduce(new NGramReducer());
		reducedTokensById.print();		

		// testing bloom filter implementation
		int expectedElements = 3;
		double falsePositiveRate = 0.1;
		
		FilterBuilder filterBuilder = new FilterBuilder(expectedElements, falsePositiveRate);
		BloomFilter<String> bloomFilter = filterBuilder.buildBloomFilter();
		
		bloomFilter.add("test");
		System.out.println(bloomFilter.asString());
		
		bloomFilter.add("test2");
		System.out.println(bloomFilter.asString());
		
	}
}
