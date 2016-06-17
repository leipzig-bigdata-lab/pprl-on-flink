package dbs.bigdata.flink.pprl.data;

import dbs.bigdata.flink.pprl.utils.HashUtils;

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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((addressPartOne == null) ? 0 : addressPartOne.hashCode());
		result = prime * result + ((addressPartTwo == null) ? 0 : addressPartTwo.hashCode());
		result = prime * result + ((age == null) ? 0 : age.hashCode());
		result = prime * result + ((city == null) ? 0 : city.hashCode());
		result = prime * result + ((firstName == null) ? 0 : firstName.hashCode());
		result = prime * result + ((genderCode == null) ? 0 : genderCode.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((lastName == null) ? 0 : lastName.hashCode());
		result = prime * result + ((state == null) ? 0 : state.hashCode());
		result = prime * result + ((zip == null) ? 0 : zip.hashCode());
		return result;
	}
	
	public long hash(){
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
		return HashUtils.getSHA1(builder.toString());
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
		if (city == null) {
			if (other.city != null) {
				return false;
			}
		} 
		else if (!city.equals(other.city)) {
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
