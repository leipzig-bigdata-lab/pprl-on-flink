package dbs.bigdata.flink.pprl.functions;

import java.util.List;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import dbs.bigdata.flink.pprl.utils.BloomFilter;

/**
 * Reduce tokens of the same group by adding to a bloom filter.
 * 
 * @author mfranke
 *
 */
public class TokenToBloomFilterGroupReducer implements 
	GroupReduceFunction<Tuple2<String, List<String>>, Tuple2<String, BloomFilter>> {

	private static final long serialVersionUID = -3771766587993456849L;
	private int size;
	private int hashes;
	
	/**
	 * @param size
	 * 		-> size of the bloom filter
	 * 
	 * @param hashes
	 * 		-> number of hash functions of the bloom filter
	 */
	public TokenToBloomFilterGroupReducer(int size, int hashes){
		this.size = size;
		this.hashes = hashes;
	}
	
	/**
	 * Transformation of all (Id, Token) tuples with the same id by adding all tokens
	 * to a bloom filter and emit the result in the form (Id, Bloom Filter). 
	 */
	@Override
	public void reduce(Iterable<Tuple2<String, List<String>>> values, Collector<Tuple2<String, BloomFilter>> out)
			throws Exception {
		
		for (Tuple2<String, List<String>> value : values){
			BloomFilter bf = new BloomFilter(this.size, this.hashes);
			String id = value.f0;
			
			for (String token : value.f1){
				bf.addElement(token);
			}
			
			out.collect(new Tuple2<String, BloomFilter>(id, bf));
		}
	}
}