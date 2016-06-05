package dbs.bigdata.flink.pprl.data;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Abstract class to extend to load various data from 
 * a source file.
 * 
 * @author mfranke
 *
 * @param <T>
 * 		-> Type of Object you want to load from a file.
 * 
 */
public abstract class DataLoader<T extends Object> {
	
	protected ExecutionEnvironment env;
	protected String dataFilePath;
	protected String lineDelimiter;
	protected String fieldDelimiter;
	protected DataSet<T> dataSet;	
	
	public DataLoader(){}

	public DataLoader(ExecutionEnvironment env){
		this.env = env;
	}
	
	public abstract DataSet<T> getAllData();
	
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

	public DataSet<T> getDataSet() {
		return dataSet;
	}

	public void setDataSet(DataSet<T> dataSet) {
		this.dataSet = dataSet;
	}
	
}