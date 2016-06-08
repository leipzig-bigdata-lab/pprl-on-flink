package dbs.bigdata.flink.pprl.data;

/**
 * Class for representing a person with various quasi identifier attributes. 
 * 
 * @author mfranke
 */
public class Person {

	private String id;
	private String firstName;
	private String lastName;
	private String addressPartOne;
	private String addressPartTwo;
	private String state;
	private String city;
	private String zip;
	private String genderCode;
	private String age;
	
	/*
	 * if needed add more elements here
	 * private String birthday;
	 * private String street;
	 * ...
	 */
	
	public Person(){}
		
	public Person(String id, String firstName, String lastName, String addressPartOne, String addressPartTwo, String state,
			String city, String zip, String genderCode, String age) {
		this.id = id;
		this.firstName = firstName;
		this.lastName = lastName;
		this.addressPartOne = addressPartOne;
		this.addressPartTwo = addressPartTwo;
		this.state = state;
		this.city = city;
		this.zip = zip;
		this.genderCode = genderCode;
		this.age = age;
	}


	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getFirstName() {
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public String getLastName() {
		return lastName;
	}
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	public String getAddressPartOne() {
		return addressPartOne;
	}
	public void setAddressPartOne(String addressPartOne) {
		this.addressPartOne = addressPartOne;
	}
	public String getAddressPartTwo() {
		return addressPartTwo;
	}
	public void setAddressPartTwo(String addressPartTwo) {
		this.addressPartTwo = addressPartTwo;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getZip() {
		return zip;
	}
	public void setZip(String zip) {
		this.zip = zip;
	}
	public String getGenderCode() {
		return genderCode;
	}
	public void setGenderCode(String genderCode) {
		this.genderCode = genderCode;
	}
	public String getAge() {
		return age;
	}
	public void setAge(String age) {
		this.age = age;
	}
	
	public String getConcatenatedAttributes(){
		StringBuilder builder = new StringBuilder();
		builder.append(firstName);
		builder.append(lastName);
		builder.append(addressPartOne);
		builder.append(addressPartTwo);
		builder.append(state);
		builder.append(city);
		builder.append(zip);
		builder.append(genderCode);
		builder.append(age);
		return builder.toString();	
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Person [id=");
		builder.append(id);
		builder.append(", firstName=");
		builder.append(firstName);
		builder.append(", lastName=");
		builder.append(lastName);
		builder.append(", addressPartOne=");
		builder.append(addressPartOne);
		builder.append(", addressPartTwo=");
		builder.append(addressPartTwo);
		builder.append(", state=");
		builder.append(state);
		builder.append(", city=");
		builder.append(city);
		builder.append(", zip=");
		builder.append(zip);
		builder.append(", genderCode=");
		builder.append(genderCode);
		builder.append(", age=");
		builder.append(age);
		builder.append("]");
		return builder.toString();
	}

}
