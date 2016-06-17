package dbs.bigdata.flink.pprl.job;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.*;
import dbs.bigdata.flink.pprl.data.*;
import dbs.bigdata.flink.pprl.functions.AddIdMapper;
import dbs.bigdata.flink.pprl.functions.BlockReducer;
import dbs.bigdata.flink.pprl.functions.LshBlocker;
import dbs.bigdata.flink.pprl.functions.NGramTokenizer;
import dbs.bigdata.flink.pprl.functions.SimilarityCalculater;
import dbs.bigdata.flink.pprl.functions.TokenToBloomFilterGroupReducer;
import dbs.bigdata.flink.pprl.utils.BloomFilter;

/**
 * The Flink job for building the bloom filters.
 * 
 * @author thornoff
 * @author mfranke
 */
public class PprlFlinkJob {

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		/*
		GeneratedDataLoader genLoader = new GeneratedDataLoader(env);
		DataSet<Person> genOrgData = genLoader.getAllData();
		DataSet<Person> genModData = genLoader.getModifiedData();			
		DataSet<Person> genData = genOrgData.union(genModData);
		//genData.print();
		*/
		
		
		VoterDataLoader voterLoader = new VoterDataLoader(env);
		DataSet<Person> voterDataOrg = voterLoader.getAllData();
		// add ids
		voterDataOrg = voterDataOrg.map(new AddIdMapper("1:"));
		
		DataSet<Person> voterDataDup = voterLoader.getAllDataFromSecondFile();
		// add ids
		voterDataDup = voterDataDup.map(new AddIdMapper("2:"));
		
		
		DataSet<Person> voterData = voterDataOrg.union(voterDataDup);
		voterData.print();
		
		///*
		// build the n-grams
		final int nGramValue = 5;
		FlatMapOperator<Person, Tuple2<String, String>> tokens = 
				voterData.flatMap(new NGramTokenizer(nGramValue));
	
		//tokens.print();
		
		
		final int bloomFilterSize = 10000;
		final int bloomFilterHashes = 4;
		
		// group tokens by id and then reduce each group by adding the tokens into a bloom filter
		GroupReduceOperator<Tuple2<String, String>, Tuple2<String, BloomFilter>> bfTokens = 
				tokens.groupBy(0).reduceGroup(new TokenToBloomFilterGroupReducer(bloomFilterSize, bloomFilterHashes));
		
		//bfTokens.print();
		//System.out.println(bfTokens.count());
		
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
				
				tokensInBloomFilter.print();
				
				// merge the n-grams for the same record
				ReduceOperator<Tuple2<String, BloomFilter>> reducedTokensBfById = 
					tokensInBloomFilter.groupBy(0).reduce(new BloomFilterReducer());
				
				reducedTokensBfById.print();		
				*/
		
		// build blocks (use LSH for this) of bloom filters, where matches are supposed)
		// TODO: rework lsh procedure (use "zehili procedure")
		///*
		final int lshHashFunctions = 4;
		final int blockCount = 10;
		final int partitionSize = 20;
		FlatMapOperator<Tuple2<String, BloomFilter>, Tuple3<String, BloomFilter, Integer>> 
			blockedBfTokens = 
				bfTokens.flatMap(
					new LshBlocker(
						lshHashFunctions, 
						blockCount, 
						partitionSize
							)
					);
		
		//blockedBfTokens.project(0,1,2).print();
		
		// build candidate pairs of bloom filters within the same block
		// MAYBE DO WE CAN DO THIS WITH A SELF JOIN?
		DistinctOperator<Tuple4<String, String, BloomFilter, BloomFilter>> candidateBfPairs = 
				blockedBfTokens.groupBy(2).reduceGroup(new BlockReducer()).distinct(0,1);
		
		//candidateBfPairs.print();
			
		// PROBLEM:
		// each group contains only one element for pair (id1, id2)
		// BUT: another group could contain the same element
		// -> distinct(0,1) remove this if (id1 == id1, id1 == id2)
		// BUT removes not the elements (id2, id1)
		
		// compare candidate bloom filter and return similarity values (flat map)
		double threshold = 0.8;
		FlatMapOperator<Tuple4<String, String, BloomFilter, BloomFilter>, Tuple2<String, String>> 
			matchingPairs = candidateBfPairs.flatMap(new SimilarityCalculater(threshold));
		
		matchingPairs.print();
		
		// TODO maybe send or save the "matching pairs" to the parties or a file 
		//*/
	}
	
}	
