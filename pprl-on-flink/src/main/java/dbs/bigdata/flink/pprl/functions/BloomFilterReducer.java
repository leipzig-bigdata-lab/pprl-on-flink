package dbs.bigdata.flink.pprl.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import dbs.bigdata.flink.pprl.utils.BloomFilter;

/**
 * Class for merging bloom filters for the same id.
 * 
 * @author mfranke
 */
public class BloomFilterReducer implements ReduceFunction<Tuple2<String, BloomFilter>> {

	private static final long serialVersionUID = -3771766587993456849L;
	
	@Override
	public Tuple2<String, BloomFilter> reduce(Tuple2<String, BloomFilter> arg0, Tuple2<String, BloomFilter> arg1)
			throws Exception {
		if (arg0.f0.equals(arg1.f0)){
			return new Tuple2<String, BloomFilter>(arg0.f0, arg0.f1.merge(arg1.f1));
		}
		else{
			return null;
		}
	}
}
