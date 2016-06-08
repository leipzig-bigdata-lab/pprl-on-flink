package dbs.bigdata.flink.pprl.job;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.*;
import dbs.bigdata.flink.pprl.data.Person;
import dbs.bigdata.flink.pprl.data.VoterDataLoader;
import dbs.bigdata.flink.pprl.functions.BlockReducer;
import dbs.bigdata.flink.pprl.functions.LshBlocker;
import dbs.bigdata.flink.pprl.functions.NGramTokenizer;
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
		genData.print();
		*/
		
		// /*
		VoterDataLoader voterLoader = new VoterDataLoader(env);
		DataSet<Person> voterData = voterLoader.getAllData();
		
		//voterData.print();
		// */
		
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
		
		// bfTokens.print();
		
		
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
		// TODO: rework lsh procedure
		final int lshHashFunctions = 4;
		final int blockCount = 100;
		final int partitionSize = 5000;
		FlatMapOperator<Tuple2<String, BloomFilter>, Tuple3<String, BloomFilter, Integer>> 
			blockedBfTokens = 
				bfTokens.flatMap(
					new LshBlocker(
						lshHashFunctions, 
						blockCount, 
						partitionSize
							)
					);
		
		//blockedBfTokens.print();
		
		
		// build candidate pairs of bloom filters within the same block
		// MAYBE DO WE CAN DO THIS WITH A SELF JOIN? MAYBE BETTER BECAUSE GROUPREDUCE IS A BIT HACKY.
		GroupReduceOperator<Tuple3<String, BloomFilter, Integer>, 
			Tuple4<String, String, BloomFilter, BloomFilter>> candidateBfPairs = 
				blockedBfTokens.groupBy(2).reduceGroup(new BlockReducer());
		
		candidateBfPairs.print();
		
		// TODO: remove all duplicates		
		/* not working (?)
		DistinctOperator<Tuple4<String, String, BloomFilter, BloomFilter>> reducedCandidateRecordPairs = 
				candidateBfPairs.distinct(0,1);
		
		candidateBfPairs.print();
		
		DataSet<Tuple4<String, String, BloomFilter, BloomFilter>> projectedPairs = 
				reducedCandidateRecordPairs.project(1,0,2,3);
		
		projectedPairs.print();
		*/
		
		//candidateBfPairs.union(projectedPairs).distinct(0,1).print();
		
		// TODO compare candidate bloom filter and return similarity values (flat map)
		// in flap map: 
		// 		Card(BF_1 AND BF_2) / Card(BF_1 OR BF_2) = similarity value
		// 		out.collect pairs with sim value > threshold
		
		
		// TODO maybe send or save the "matching pairs" to the parties or a file 
	}
}	
