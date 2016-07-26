package dbs.bigdata.flink.pprl.data;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Abstract class to load  data from a source file.
 * 
 * @author mfranke
 *
 * @param <T>
 * 		-> type of objects in which each line (row) of the file will be transformed.
 * 
 */
public abstract class DataLoader<T extends Object> {
	
	protected ExecutionEnvironment env;
	protected String dataFilePath;
	protected String lineDelimiter;
	protected String fieldDelimiter;
	
	/**
	 * Creates a new DataLoader object.
	 */
	public DataLoader(){}

	/**
	 * Creates a new DataLoader object.
	 * @param env
	 * 		-> the ExecutionEnvironment of the flink job.
	 */
	public DataLoader(ExecutionEnvironment env){
		this(env, "", "", "");
	}

	/**
	 * Creates a new DataLoader object.
	 * @param env
	 * 		-> the ExecutionEnvironment of the flink job.
	 * 
	 * @param dataFilePath
	 * 		-> path to the data file.
	 * 
	 * @param lineDelimiter
	 * 		-> character separator for the lines (i.e. rows).
	 * 
	 * @param fieldDelimiter
	 * 		-> character separator for the fields (i.e. columns).
	 */
	public DataLoader(ExecutionEnvironment env, String dataFilePath, String lineDelimiter, String fieldDelimiter){
		this.env = env;
		this.dataFilePath = dataFilePath;
		this.lineDelimiter = lineDelimiter;
		this.fieldDelimiter = fieldDelimiter;
	}
	
	/**
	 * Reads the data from the file into a DataSet<T> object.
	 * @return
	 * 		-> the DataSet<T> representation of the specified file.
	 */
	public abstract DataSet<T> getData();
	
	public ExecutionEnvironment getEnv() {
		return env;
	}

	public void setEnv(ExecutionEnvironment env) {
		this.env = env;
	}

	public String getDataFilePath() {
		return dataFilePath;
	}

	public void setDataFilePath(String dataFilePath) {
		this.dataFilePath = dataFilePath;
	}

	public String getLineDelimiter() {
		return lineDelimiter;
	}

	public void setLineDelimiter(String lineDelimiter) {
		this.lineDelimiter = lineDelimiter;
	}

	public String getFieldDelimiter() {
		return fieldDelimiter;
	}

	public void setFieldDelimiter(String fieldDelimiter) {
		this.fieldDelimiter = fieldDelimiter;
	}
}