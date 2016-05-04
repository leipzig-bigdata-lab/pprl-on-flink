package dbs.bigdata.flink.pprl;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Class for merging tokens of the same record.
 * 
 * @author mfranke
 */
public class NGramReducer implements ReduceFunction<Tuple2<String, String>> {

	private static final long serialVersionUID = -3771766587993456849L;

	@Override
	public Tuple2<String, String> reduce(Tuple2<String, String> arg0, Tuple2<String, String> arg1) throws Exception {
		if (arg0.f0.equals(arg1.f0)){
			return new Tuple2<String, String>(arg0.f0, arg0.f1 + "|" + arg1.f1);
		}
		else{
			return null;
		}
	}
}
