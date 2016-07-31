package dbs.bigdata.flink.pprl.data;


import dbs.bigdata.flink.pprl.utils.HashUtils;

/**
 * Class for representing a person with various quasi identifier attributes. 
 * 
 * @author mfranke
 */
public class Person {
	
	public static final String ID_ATTRIBUTE = "id";
	public static final String FIRST_NAME_ATTRIBUTE = "firstName";
	public static final String MIDDLE_NAME_ATTRIBUTE = "middleName";
	public static final String LAST_NAME_ATTRIBUTE = "lastName";
	public static final String ADDRESS_PART_ONE_ATTRIBUTE = "addressPartOne";
	public static final String ADDRESS_PART_TWO_ATTRIBUTE = "addressPartTwo";
	public static final String STATE_ATTRIBUTE = "state";
	public static final String CITY_ATTRIBUTE = "city";
	public static final String ZIP_ATTRIBUTE = "zip";
	public static final String GENDER_CODE_ATTRIBUTE = "genderCode";
	public static final String AGE_ATTRIBUTE = "age";
	public static final String BIRTHDAY_ATTRIBUTE = "birthday";
	public static final String ETHNIC_CODE_ATTRIBUTE = "ethnicCode";
	
	public static final int FIELDS = 13;
	
	private String id;
	private String firstName;
	private String middleName;
	private String lastName;
	private String addressPartOne;
	private String addressPartTwo;
	private String state;
	private String city;
	private String zip;
	private String genderCode;
	private String age;
	private String birthday;
	private String ethnicCode;
		
	/**
	 * Creates a new Person object.
	 */
	public Person(){
		this("", "", "", "", "", "", "", "", "", "", "", "", "");
	}
		
	/**
	 * Creates a new Person object.
	 * 
	 * @param id
	 * 		-> the id of the person.
	 * 
	 * @param firstName
	 * 		-> the first name of the person.
	 * 
	 * @param lastName
	 * 		-> the last name of the person.
	 * 
	 * @param middleName
	 * 		-> the middleName of the person.
	 * 
	 * @param addressPartOne
	 * 		-> the first part of the address of the person.
	 * 
	 * @param addressPartTwo
	 * 		-> the second part of the address of the person.
	 * 
	 * @param state
	 * 		-> the state the person living in.
	 * 
	 * @param city
	 * 		-> the city the person living in.
	 * 
	 * @param zip
	 * 		-> the zip code of the city.
	 * 
	 * @param genderCode
	 * 		-> the genderCode of the person.
	 * 
	 * @param age
	 * 		-> the age of the person.
	 * 
	 * @param birthday
	 * 		-> the birth date of the person.
	 * 
	 * @param ethnicCode
	 * 		-> the ethnic code of the person.
	 */
	public Person(String id, String firstName, String lastName, String middleName, String addressPartOne,
			String addressPartTwo, String state, String city, String zip, String genderCode, String age,
			String birthday, String ethnicCode) {
		this.id = id;
		this.firstName = firstName;
		this.middleName = middleName;
		this.lastName = lastName;
		this.addressPartOne = addressPartOne;
		this.addressPartTwo = addressPartTwo;
		this.state = state;
		this.city = city;
		this.zip = zip;
		this.genderCode = genderCode;
		this.age = age;
		this.birthday = birthday;
		this.ethnicCode = ethnicCode;
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
	
	public String getMiddleName() {
		return middleName;
	}

	public void setMiddleName(String middleName) {
		this.middleName = middleName;
	}

	public String getBirthday() {
		return birthday;
	}

	public void setBirthday(String birthday) {
		this.birthday = birthday;
	}

	public String getEthnicCode() {
		return ethnicCode;
	}

	public void setEthnicCode(String ethnicCode) {
		this.ethnicCode = ethnicCode;
	}

	/**
	 * Concatenates the attributes of the person and return the resulting string.
	 * 
	 * @param separator
	 * 		-> defines a separator to use between the attributes.
	 * 
	 * @return
	 * 		-> string with the concatenated attributes of the person.
	 */
	public String getConcatenatedAttributes(String separator){
		String[] attributes = {
			firstName,
			middleName,
			lastName,
			addressPartOne,
			addressPartTwo,
			state,
			city,
			zip,
			genderCode,
			age,
			birthday,
			ethnicCode
		};
		
		StringBuilder builder = new StringBuilder();
		
		for (String attribute : attributes){
			builder.append(attribute);
			builder.append(separator);
		}
		builder.delete(builder.length() - separator.length(), builder.length());
		
		return builder.toString();	
	}

	/**
	 * @return
	 * 		-> the hash value of the person object.
	 */
	public long hash(){
		return HashUtils.getSHA(this.getConcatenatedAttributes(""));
	}

	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Person [id=");
		builder.append(id);
		builder.append(", firstName=");
		builder.append(firstName);
		builder.append(", middleName=");
		builder.append(middleName);
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
		builder.append(", birthday=");
		builder.append(birthday);
		builder.append(", ethnicCode=");
		builder.append(ethnicCode);
		builder.append("]");
		return builder.toString();
	}
	
	public String[] getAttributeValues(){
		String[] result = new String[FIELDS-1];
		
		result[0] = this.firstName;
		result[1] = this.middleName;
		result[2] = this.lastName;
		result[3] = this.addressPartOne;
		result[4] = this.addressPartTwo;
		result[5] = this.state;
		result[6] = this.city;
		result[7] = this.zip;
		result[8] = this.genderCode;
		result[9] = this.age;
		result[10] = this.birthday;
		result[11] = this.ethnicCode;
		
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		Person other = (Person) obj;
		if (addressPartOne == null) {
			if (other.addressPartOne != null) {
				return false;
			}
		}
		else if (!addressPartOne.equals(other.addressPartOne)) {
			return false;
		}
		if (addressPartTwo == null) {
			if (other.addressPartTwo != null) {
				return false;
			}
		}
		else if (!addressPartTwo.equals(other.addressPartTwo)) {
			return false;
		}
		if (age == null) {
			if (other.age != null) {
				return false;
			}
		}
		else if (!age.equals(other.age)) {
			return false;
		}
		if (birthday == null) {
			if (other.birthday != null) {
				return false;
			}
		} 
		else if (!birthday.equals(other.birthday)) {
			return false;
		}
		if (city == null) {
			if (other.city != null) {
				return false;
			}
		}
		else if (!city.equals(other.city)) {
			return false;
		}
		if (ethnicCode == null) {
			if (other.ethnicCode != null) {
				return false;
			}
		}
		else if (!ethnicCode.equals(other.ethnicCode)) {
			return false;
		}
		if (firstName == null) {
			if (other.firstName != null) {
				return false;
			}
		}
		else if (!firstName.equals(other.firstName)) {
			return false;
		}
		if (genderCode == null) {
			if (other.genderCode != null) {
				return false;
			}
		}
		else if (!genderCode.equals(other.genderCode)) {
			return false;
		}
		if (id == null) {
			if (other.id != null) {
				return false;
			}
		}
		else if (!id.equals(other.id)) {
			return false;
		}
		if (lastName == null) {
			if (other.lastName != null) {
				return false;
			}
		}
		else if (!lastName.equals(other.lastName)) {
			return false;
		}
		if (middleName == null) {
			if (other.middleName != null) {
				return false;
			}
		} 
		else if (!middleName.equals(other.middleName)) {
			return false;
		}
		if (state == null) {
			if (other.state != null) {
				return false;
			}
		} 
		else if (!state.equals(other.state)) {
			return false;
		}
		if (zip == null) {
			if (other.zip != null) {
				return false;
			}
		} 
		else if (!zip.equals(other.zip)) {
			return false;
		}
		return true;
	}
}
