package dbs.bigdata.flink.pprl.data;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Class for loading the generated data (sehili) from the corresponding file.
 * "Maps" the data to a DataSet with Person objects in it.
 * 
 * @author mfranke
 *
 */
public class GeneratedDataLoader extends DataLoader<Person>{
	
	private final String modifiedDataFilePath;
	
	public GeneratedDataLoader(ExecutionEnvironment env) {
		super (env);
		this.dataFilePath = "org_E_100000.csv";
		this.modifiedDataFilePath = "dup_E_100000.csv";
		this.lineDelimiter = "\n";
		this.fieldDelimiter = ",";
	}

	@Override
	public DataSet<Person> getAllData() {
		DataSet<Person> persons =
				this.env.readCsvFile(this.dataFilePath)
				.lineDelimiter(this.lineDelimiter)
				.fieldDelimiter(this.fieldDelimiter)
				.ignoreFirstLine()
				.includeFields("111011111000000")
				.pojoType(Person.class, "id", "firstName", "lastName", "addressPartOne", "addressPartTwo", 
						"city", "zip", "state");
		
		return persons;
	}	
	
	public DataSet<Person> getModifiedData(){
		DataSet<Person> persons =
				this.env.readCsvFile(this.modifiedDataFilePath)
				.lineDelimiter(this.lineDelimiter)
				.fieldDelimiter(this.fieldDelimiter)
				.ignoreFirstLine()
				.includeFields("111011111000000")
				.pojoType(Person.class, "id", "firstName", "lastName", "addressPartOne", "addressPartTwo", 
						"city", "zip", "state");
		
		return persons;
	}
}