package dbs.bigdata.flink.pprl;

import static org.junit.Assert.*;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;

import dbs.bigdata.flink.pprl.data.Person;
import dbs.bigdata.flink.pprl.functions.AddIdMapper;

/**
 * Class for testing the id generation for person objects within a flink data set.
 * 
 * @author mfranke
 */
public class AddIdMapperTest {

	private final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

	private DataSet<Person> personData;
	
	@Before
	public void init() throws Exception{
		Person person = new Person();
		person.setFirstName("Hans");
		person.setLastName("Müller");
		person.setAddressPartOne("Sesamestreet123");
		person.setCity("Wonderland");
		person.setAge("23");
		
		Person otherPerson = new Person();
		otherPerson.setId("9980");
		otherPerson.setFirstName("Foo");
		otherPerson.setLastName("Bar");
		otherPerson.setAddressPartOne("Streetway");
		otherPerson.setAddressPartTwo("9");
		otherPerson.setCity("Hugetown");
		otherPerson.setAge("39");
		
		this.personData = env.fromElements(person, otherPerson);	
	}
	
	
	@Test
	public void test() throws Exception {
		assertEquals(2, this.personData.count());
		
		final String dataSetIdentifier = "Test";
		
		DataSet<Person> personWithId = personData.map(new AddIdMapper(dataSetIdentifier));
		
		assertEquals(2, personWithId.count());
		
		List<Person> personList = personWithId.collect();
		
		assertEquals(2, personList.size());
		
		Person p1 = personList.get(0);
		Person p2 = personList.get(1);
		
		assertTrue(p1.getId().startsWith(dataSetIdentifier));
		assertTrue(p2.getId().startsWith(dataSetIdentifier));
		
		assertTrue(p2.getId().endsWith("9980"));
		assertTrue(p1.getId().length() > dataSetIdentifier.length());
	}
}
