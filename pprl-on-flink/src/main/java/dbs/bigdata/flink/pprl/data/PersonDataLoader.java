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
public class PersonDataLoader extends DataLoader<Person>{

	private String includingFields;
	private String[] personFields;
	private boolean ignoreFirstLine;
	
	/**
	 * Creates a new PersonDataLoader object.
	 * 
	 * @param env
	 * 		-> the ExecutionEnvironment.
	 * 
	 * @param dataFilePath
	 * 		-> the file path of the data file.
	 * 
	 * @param lineDelimiter
	 * 		-> the character separator for the lines (i.e. rows) of the file.
	 * 
	 * @param fieldDelimiter
	 * 		-> the character separator for the field (i.e. columns) of the file.
	 * 
	 * @param includingFields
	 * 		-> character sequence containing only 0 and 1 where each 0 (1) indicates that
	 * 		   the field on this position is read (not read). The length of the character
	 *         sequence have to be equal to the number of fields included in the data file.
	 * 
	 * @param personFields
	 * 		-> string array containing the values of a Person object the included fields have to be mapped.
	 *  	   The number of the personFields have to be equal to the number of 1s in the includingFields string.
	 *  
	 * @param ignoreFirstLine
	 * 		-> true (false) to ignore (not ignore) the first line of the data file.
	 */
	public PersonDataLoader(ExecutionEnvironment env, String dataFilePath, String lineDelimiter, 
			String fieldDelimiter, String includingFields, String[] personFields, boolean ignoreFirstLine) {
		
		super (env, dataFilePath, lineDelimiter, fieldDelimiter);
		
		this.includingFields = includingFields;
		this.personFields = personFields;
		this.ignoreFirstLine = ignoreFirstLine;
	}

	@Override
	public DataSet<Person> getData() {
		DataSet<Person> persons;
		
		if (this.ignoreFirstLine){
			persons = this.env
				.readCsvFile(this.dataFilePath)
				.lineDelimiter(this.lineDelimiter)
				.fieldDelimiter(this.fieldDelimiter)
				.ignoreFirstLine()
				.includeFields(this.includingFields)
				.pojoType(Person.class, this.personFields);

		}
		else{
			persons = this.env
				.readCsvFile(this.dataFilePath)
				.lineDelimiter(this.lineDelimiter)
				.fieldDelimiter(this.fieldDelimiter)
				.includeFields(this.includingFields)
				.pojoType(Person.class, this.personFields);
		}
		
		return persons;
	}

	public String getIncludingFields() {
		return includingFields;
	}

	public void setIncludingFields(String includingFields) {
		this.includingFields = includingFields;
	}

	public String[] getPersonFields() {
		return personFields;
	}

	public void setPersonFields(String[] personFields) {
		this.personFields = personFields;
	}

	public boolean isIgnoreFirstLine() {
		return ignoreFirstLine;
	}

	public void setIgnoreFirstLine(boolean ignoreFirstLine) {
		this.ignoreFirstLine = ignoreFirstLine;
	}	
}