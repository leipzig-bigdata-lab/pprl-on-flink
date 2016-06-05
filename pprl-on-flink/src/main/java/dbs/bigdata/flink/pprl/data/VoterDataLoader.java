package dbs.bigdata.flink.pprl.data;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Class for loading the voter data from the corresponding file.
 * "Maps" the data to a DataSet with Person objects in it.
 * 
 * @author mfranke
 *
 */
public class VoterDataLoader extends DataLoader<Person>{

	public VoterDataLoader(ExecutionEnvironment env) {
		super (env);
		this.dataFilePath = "test_voter.txt";
		//this.dataFilePath = "dmv_voter_id.txt";
		this.lineDelimiter = "\n";
		this.fieldDelimiter = "\t";
	}

	@Override
	public DataSet<Person> getAllData() {
		DataSet<Person> persons =
				this.env.readCsvFile(this.dataFilePath)
				.lineDelimiter(this.lineDelimiter)
				.fieldDelimiter(this.fieldDelimiter)
				.ignoreFirstLine()
				.includeFields("0110011111000110")
				.pojoType(Person.class, "firstName", "lastName", "addressPartOne", "addressPartTwo", "state",
						"city", "zip", "genderCode", "age");
		
		return persons;
	}	
}