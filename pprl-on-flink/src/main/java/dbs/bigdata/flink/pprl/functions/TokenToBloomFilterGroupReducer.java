package dbs.bigdata.flink.pprl.functions;

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
	GroupReduceFunction<Tuple2<String, String>, Tuple2<String, BloomFilter>> {

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
	
	@Override
	public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, BloomFilter>> out)
			throws Exception {
		BloomFilter bf = new BloomFilter(this.size, this.hashes);
		String id = "";
		boolean first = true;
		for (Tuple2<String, String> value : values){
			if (first){
				id = value.f0;
				bf.addElement(value.f1);
				first = false;
			}
			else{
				if (id.equals(value.f0)){
					bf.addElement(value.f1);
				}
			}
		}
		out.collect(new Tuple2<String, BloomFilter>(id, bf));
	}
}