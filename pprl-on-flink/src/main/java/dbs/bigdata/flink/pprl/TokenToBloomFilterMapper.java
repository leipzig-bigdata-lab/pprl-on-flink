package dbs.bigdata.flink.pprl;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Map each token into a bloom filter
 * 
 * @author mfranke
 *
 */
public class TokenToBloomFilterMapper implements FlatMapFunction<Tuple2<String, String>, Tuple2<String, BloomFilter>> {

	private static final long serialVersionUID = -6648014504203710061L;
	private int size;
	private int hashes;

	/**
	 * @param size
	 * 		-> size of the bloom filter
	 * 
	 * @param hashes
	 * 		-> number of hash functions of the bloom filter
	 */
	public TokenToBloomFilterMapper(int size, int hashes){
		this.size = size;
		this.hashes = hashes;
	}

	@Override
	public void flatMap(Tuple2<String, String> input, Collector<Tuple2<String, BloomFilter>> output) throws Exception {
		BloomFilter bf = new BloomFilter(this.size, this.hashes);
		bf.addElement(input.f1);
		output.collect(new Tuple2<String, BloomFilter>(input.f0, bf));
	}
	
}
