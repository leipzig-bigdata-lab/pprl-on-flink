package dbs.bigdata.flink.pprl.data;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;

public class PersonDataLoader extends DataLoader<Tuple5<String, String, String, String, String>>{

	public PersonDataLoader(ExecutionEnvironment env) {
		super (env);
		this.dataFilePath = "persons.txt";
		this.lineDelimiter = "##//##";
		this.fieldDelimiter = "#|#";
	}

	@Override
	public DataSet<Tuple5<String, String, String, String, String>> getAllData(){
		// get input data, read all fields
		DataSet<Tuple5<String, String, String, String, String>> persons =
			  this.env.readCsvFile(this.dataFilePath)
			  .lineDelimiter(this.lineDelimiter)
			  .fieldDelimiter(this.fieldDelimiter)
			  .types(String.class, String.class, String.class, String.class, String.class);
		
		return persons;
	}
	
}
