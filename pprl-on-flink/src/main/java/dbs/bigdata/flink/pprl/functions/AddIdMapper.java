package dbs.bigdata.flink.pprl.functions;

import org.apache.flink.api.common.functions.MapFunction;

import dbs.bigdata.flink.pprl.data.Person;

/**
 * Adds to each person an id (if it has no) and a data set identifier.
 * 
 * @author mfranke
 */
public class AddIdMapper implements MapFunction<Person, Person> {

	private static final long serialVersionUID = 1874396153396624032L;

	private String dataSetIdentifier;
	
	/**
	 * Returns a new AddIdMapper object.
	 */
	public AddIdMapper(){
		this("");
	}
	
	/**
	 * Returns a new AddIdMapper object.
	 *
	 * @param dataSetIdentifier
	 *		-> defines a identifier for objects of the same data set.
	 */ 
	public AddIdMapper(String dataSetIdentifier){
		this.dataSetIdentifier = dataSetIdentifier;
	}
	
	/**
	 * Calculates an id for each person object, if it has no id already,
	 * Adds the data set identifier to indicate objects of the same data set.
	 */
	@Override
	public Person map(Person value) throws Exception {
		String id = value.getId();
		if (id == null || (id != null && id.equals(""))){
			final String hashId = Long.toString(value.hash());
			value.setId(this.dataSetIdentifier + hashId);
		}
		else{
			value.setId(this.dataSetIdentifier + id);
		}
		return value;
	}

}
