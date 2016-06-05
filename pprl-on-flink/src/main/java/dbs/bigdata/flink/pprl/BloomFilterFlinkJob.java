package dbs.bigdata.flink.pprl;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;

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
		
		// TODO maybe read records with all data fields and then map to a record with only relevant fields
		
		// build the n-grams
		final int nGramValue = 2;
		FlatMapOperator<Tuple5<String, String, String, String, String>, Tuple2<String, String>> tokens = 
				personLoader.getAllData().flatMap(new NGramTokenizer(nGramValue));
		
		// tokens.print();
		
		final int bloomFilterSize = 1000;
		final int bloomFilterHashes = 4;
		
		// group tokens by id and then reduce each group by adding the tokens into a bloom filter
		GroupReduceOperator<Tuple2<String, String>, Tuple2<String, BloomFilter>> bfs = 
				tokens.groupBy(0).reduceGroup(new TokenToBloomFilterGroupReducer(bloomFilterSize, bloomFilterHashes));
		
		bfs.print();
		
		/*
		 * Alternative:
		 * 	- map each token (n-gram) to a BloomFilter
		 *  - then merge the bloom filters for the same id
		 *  - but: probably it is faster to add elements to single bloom filter then
		 *    create x bloom filters and then merge them (set bits vs. search x times the set bit positions)
		 *  
		*//*
		// map n-grams to BloomFilter
		FlatMapOperator<Tuple2<String, String>, Tuple2<String, BloomFilter>> tokensInBloomFilter =
				tokens.flatMap(new TokenToBloomFilterMapper(bloomFilterSize, bloomFilterHashes));
		
		tokensInBloomFilter.print();
		
		// merge the n-grams for the same record
		ReduceOperator<Tuple2<String, BloomFilter>> reducedTokensBfById = 
			tokensInBloomFilter.groupBy(0).reduce(new BloomFilterReducer());
		
		reducedTokensBfById.print();		
		*/
		
		// TODO build blocks (use LSH for this) of bloom filters, where matches are supposed (flat map/reduce)
			
		
		// TODO compare candidate bloom filter pairs of the same block and return similarity values (flat map)
		
		// TODO check similarity values against a threshold and keep only those pairs with a similarity value
		// greater then the threshold (reduce)
		
		// TODO maybe send or save the "matching pairs" to the partiespg or a file 
	}
}	
