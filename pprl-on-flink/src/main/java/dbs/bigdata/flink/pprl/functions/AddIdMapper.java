package dbs.bigdata.flink.pprl.functions;

import org.apache.flink.api.common.functions.MapFunction;

import dbs.bigdata.flink.pprl.data.Person;

public class AddIdMapper implements MapFunction<Person, Person> {

	private static final long serialVersionUID = 1874396153396624032L;

	private String dataSetIdentifier;
	
	public AddIdMapper(String dataSetIdentifier){
		this.dataSetIdentifier = dataSetIdentifier;
	}
	
	public AddIdMapper(){
		this("");
	}
	
	@Override
	public Person map(Person value) throws Exception {
		String id = value.getId();
		if (id == null || (id != null && id.equals(""))){
			final String hashId = Long.toString(value.hash());
			value.setId(this.dataSetIdentifier + hashId);
		}
		return value;
	}

}
