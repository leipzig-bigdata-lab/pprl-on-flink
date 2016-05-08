package dbs.bigdata.flink.pprl;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class VoterDataLoader extends DataLoader<Tuple2<String, String>>{

	public VoterDataLoader(ExecutionEnvironment env) {
		super (env);
		this.dataFilePath = "dmv_voter_id.txt";
		this.lineDelimiter = "\n";
		this.fieldDelimiter = "\t";
	}

	@Override
	public DataSet<Tuple2<String, String>> getAllData(){
		// get input data, read all fields
		DataSet<Tuple2<String, String>> persons =
			  this.env.readCsvFile(this.dataFilePath)
			  .lineDelimiter(this.lineDelimiter)
			  .fieldDelimiter(this.fieldDelimiter)
			  .includeFields("0110000000000000")
			  .types(String.class, String.class);
		
		return persons;
	}
	
}
