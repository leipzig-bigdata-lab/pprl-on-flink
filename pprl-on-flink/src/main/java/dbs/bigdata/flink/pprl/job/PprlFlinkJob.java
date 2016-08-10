package dbs.bigdata.flink.pprl.job;

import java.util.BitSet;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;

import dbs.bigdata.flink.pprl.data.*;
import dbs.bigdata.flink.pprl.functions.AddIdMapper;
import dbs.bigdata.flink.pprl.functions.BlockReducer;
import dbs.bigdata.flink.pprl.functions.DuplicateCandidateRemover;
import dbs.bigdata.flink.pprl.functions.LshBlocker;
import dbs.bigdata.flink.pprl.functions.NGramListTokenizer;
import dbs.bigdata.flink.pprl.functions.NGramTokenizer;
import dbs.bigdata.flink.pprl.functions.SimilarityCalculater;
import dbs.bigdata.flink.pprl.functions.TokenToBloomFilterGroupReducer;
import dbs.bigdata.flink.pprl.functions.TokenToBloomFilterMapper;
import dbs.bigdata.flink.pprl.functions.BloomFilterReducer;
import dbs.bigdata.flink.pprl.utils.BloomFilter;
import dbs.bigdata.flink.pprl.utils.BloomFilterWithLshKeys;
import dbs.bigdata.flink.pprl.utils.CandidateBloomFilterPair;
import dbs.bigdata.flink.pprl.utils.HashFamilyGroup;
import dbs.bigdata.flink.pprl.utils.IndexHash;

/**
 * The Flink job for the pprl.
 * 
 * @author thornoff
 * @author mfranke
 */
@SuppressWarnings("unused")
public class PprlFlinkJob {

	private final ExecutionEnvironment env;
	private String dataFilePath;
	private String dataFilePathDup;
	private String lineDelimiter;
	private String fieldDelimiter;
	private String includingFields;
	private String[] personFields;
	private boolean ignoreFirstLine;
	private boolean withCharacterPadding;
	private boolean bloomFilterBuildVariantOne;
	private int bloomFilterSize;
	private int bloomFilterHashes;
	private int ngramValue;
	private int numberOfHashFamilies;
	private int numberOfHashesPerFamily;
	private double comparisonThreshold;
	
	public PprlFlinkJob(){
		// set up the execution environment
		this.env =  ExecutionEnvironment.getExecutionEnvironment();		
	}
	
	public PprlFlinkJob(String dataFilePath, String dataFilePathDup, String lineDelimiter, String fieldDelimiter,
			String includingFields, String[] personFields, boolean ignoreFirstLine, boolean withCharacterPadding,
			boolean bloomFilterBuildVariantOne, int bloomFilterSize, int bloomFilterHashes, int ngramValue,
			int numberOfHashFamilies, int numberOfHashesPerFamily, double comparisonThreshold) {
		// set up the execution environment
		this.env = ExecutionEnvironment.getExecutionEnvironment();
		this.dataFilePath = dataFilePath;
		this.dataFilePathDup = dataFilePathDup;
		this.lineDelimiter = lineDelimiter;
		this.fieldDelimiter = fieldDelimiter;
		this.includingFields = includingFields;
		this.personFields = personFields;
		this.ignoreFirstLine = ignoreFirstLine;
		this.withCharacterPadding = withCharacterPadding;
		this.bloomFilterBuildVariantOne = bloomFilterBuildVariantOne;
		this.bloomFilterSize = bloomFilterSize;
		this.bloomFilterHashes = bloomFilterHashes;
		this.ngramValue = ngramValue;
		this.numberOfHashFamilies = numberOfHashFamilies;
		this.numberOfHashesPerFamily = numberOfHashesPerFamily;
		this.comparisonThreshold = comparisonThreshold;
	}


	private DataSet<Person> loadDataFromCsvFiles(ExecutionEnvironment env){
		PersonDataLoader voterLoader = new PersonDataLoader(
				env,
				this.dataFilePath,
				this.lineDelimiter,
				this.fieldDelimiter,
				this.includingFields,
				this.personFields,
				this.ignoreFirstLine
		);
		
		DataSet<Person> voterDataOrg = voterLoader.getData();
		
		voterLoader.setDataFilePath(this.dataFilePathDup);
		DataSet<Person> voterDataDup = voterLoader.getData();
		
		// add an id to each person
		voterDataOrg = voterDataOrg.map(new AddIdMapper("1:"));
		voterDataDup = voterDataDup.map(new AddIdMapper("2:"));
		
		// union the original and duplicate data sets
		DataSet<Person> voterData = voterDataOrg.union(voterDataDup);
		
		return voterData;
	}
		
	private DataSet<Tuple2<String, BloomFilter>> buildBloomFilter(ExecutionEnvironment env,
			DataSet<Person> data) throws Exception{
		if (this.bloomFilterBuildVariantOne){
			DataSet<Tuple2<String, List<String>>> tokens = 
					data.flatMap(new NGramListTokenizer(this.ngramValue, this.withCharacterPadding));

			tokens.writeAsText("output_files/step_2_1_tokens",  WriteMode.OVERWRITE);
			//tokens.print();
			
			return tokens.groupBy(0).reduceGroup(new TokenToBloomFilterGroupReducer(this.bloomFilterSize, this.bloomFilterHashes));
		}
		else{
			DataSet<Tuple2<String, String>> tokens =
					data.flatMap(new NGramTokenizer(this.ngramValue, this.withCharacterPadding));
		
			tokens.writeAsText("output_files/step_2_1_tokens",  WriteMode.OVERWRITE);
			//tokens.print();
			
			// map n-grams to bloom filter
			DataSet<Tuple2<String, BloomFilter>> tokensInBloomFilter =
					tokens.flatMap(new TokenToBloomFilterMapper(this.bloomFilterSize, this.bloomFilterHashes));
		
			// merge the n-grams for the same record
			return tokensInBloomFilter.groupBy(0).reduce(new BloomFilterReducer());
		}
	}
	
	private DataSet<Tuple3<Integer, BitSet, BloomFilterWithLshKeys>> calculateLshKeys(ExecutionEnvironment env, 
			DataSet<Tuple2<String, BloomFilter>> data){
		// build blocks (use LSH for this) of bloom filters, where matches are supposed
		final int valueRange = this.bloomFilterSize;
				
		HashFamilyGroup<IndexHash, Boolean> hashFamilyGroup = 
				HashFamilyGroup.generateRandomIndexHashFamilyGroup(
						this.numberOfHashFamilies,
						this.numberOfHashesPerFamily,
						valueRange
				);
				
		return data.flatMap(new LshBlocker(hashFamilyGroup));		
	}
	
	private DataSet<Tuple2<Integer, CandidateBloomFilterPair>> blockWithLshKeys(ExecutionEnvironment env,
			DataSet<Tuple3<Integer, BitSet, BloomFilterWithLshKeys>> data){
		return data.groupBy(0).reduceGroup(new BlockReducer());
	}
	
	private DataSet<Tuple2<Integer, CandidateBloomFilterPair>> removeDuplicateCandidates(ExecutionEnvironment env, 
			DataSet<Tuple2<Integer, CandidateBloomFilterPair>> data){
		return data.filter(new DuplicateCandidateRemover());
	}
	
	private DataSet<Tuple2<String, String>> calculateSimilarity(ExecutionEnvironment env,
			DataSet<Tuple2<Integer,CandidateBloomFilterPair>> data){
		return data.flatMap(new SimilarityCalculater(this.comparisonThreshold));
	}
	
	public void runJob() throws Exception{
		// csv-file --> (Person)
		DataSet<Person> voterData = loadDataFromCsvFiles(env);
		voterData.writeAsText("output_files/step_1_voterData",  WriteMode.OVERWRITE);		
		
		// (Person) --> (id, token) / (id, {token})  --> (id, bloom filter)
		DataSet<Tuple2<String, BloomFilter>> bloomFilter = buildBloomFilter(env, voterData);
		bloomFilter.writeAsText("output_files/step_2_2_bloomFilter",  WriteMode.OVERWRITE);

		// (id, bloom filter) --> (keyId, keyValue, bloom filter with keys)
		DataSet<Tuple3<Integer, BitSet, BloomFilterWithLshKeys>> keyBloomFilterPairs = calculateLshKeys(env, bloomFilter);
		keyBloomFilterPairs.writeAsText("output_files/step_3_keyBloomFilterPairs",  WriteMode.OVERWRITE);
				
		// (keyId, key, BloomFilter) --> (keyId, candidate pair)
		DataSet<Tuple2<Integer, CandidateBloomFilterPair>> keysWithCandidatePair = blockWithLshKeys(env, keyBloomFilterPairs);
		keysWithCandidatePair.writeAsText("output_files/step_4_keysWithCandidatePair", WriteMode.OVERWRITE);
		
		// (keyId, candidate pair) --> (keyId, candidate pair)
		DataSet<Tuple2<Integer, CandidateBloomFilterPair>> distinctCandidatePairs = removeDuplicateCandidates(env, keysWithCandidatePair);
		distinctCandidatePairs.writeAsText("output_files/step_5_distinctCandidatePairs", WriteMode.OVERWRITE);
		
		// (keyId, candidate pair) -> (id, id)
		DataSet<Tuple2<String, String>> matchingPairs = calculateSimilarity(env, distinctCandidatePairs);
		matchingPairs.writeAsText("output_files/step_6_matchingPairs", WriteMode.OVERWRITE);
		//matchingPairs.print();
		System.out.println(matchingPairs.count());;
	}

	public String getDataFilePath() {
		return dataFilePath;
	}

	public void setDataFilePath(String dataFilePath) {
		this.dataFilePath = dataFilePath;
	}

	public String getDataFilePathDup() {
		return dataFilePathDup;
	}

	public void setDataFilePathDup(String dataFilePathDup) {
		this.dataFilePathDup = dataFilePathDup;
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

	public boolean isWithCharacterPadding() {
		return withCharacterPadding;
	}

	public void setWithCharacterPadding(boolean withCharacterPadding) {
		this.withCharacterPadding = withCharacterPadding;
	}

	public boolean isBloomFilterBuildVariantOne() {
		return bloomFilterBuildVariantOne;
	}

	public void setBloomFilterBuildVariantOne(boolean bloomFilterBuildVariantOne) {
		this.bloomFilterBuildVariantOne = bloomFilterBuildVariantOne;
	}

	public int getBloomFilterSize() {
		return bloomFilterSize;
	}

	public void setBloomFilterSize(int bloomFilterSize) {
		this.bloomFilterSize = bloomFilterSize;
	}

	public int getBloomFilterHashes() {
		return bloomFilterHashes;
	}

	public void setBloomFilterHashes(int bloomFilterHashes) {
		this.bloomFilterHashes = bloomFilterHashes;
	}

	public int getNgramValue() {
		return ngramValue;
	}

	public void setNgramValue(int ngramValue) {
		this.ngramValue = ngramValue;
	}

	public int getNumberOfHashFamilies() {
		return numberOfHashFamilies;
	}

	public void setNumberOfHashFamilies(int numberOfHashFamilies) {
		this.numberOfHashFamilies = numberOfHashFamilies;
	}

	public int getNumberOfHashesPerFamily() {
		return numberOfHashesPerFamily;
	}

	public void setNumberOfHashesPerFamily(int numberOfHashesPerFamily) {
		this.numberOfHashesPerFamily = numberOfHashesPerFamily;
	}

	public double getComparisonThreshold() {
		return comparisonThreshold;
	}

	public void setComparisonThreshold(double comparisonThreshold) {
		this.comparisonThreshold = comparisonThreshold;
	}
	

	public static void main(String[] args) throws Exception {
		// use of a property file:
		/*
		final String propertiesFile = "parameter_config.properties";
		ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFile);
		
		String personField = parameter.get("personFields");
		String[] personFields = personField.split("#");
		
		for (int i = 0; i < personFields.length; i++) System.out.println(personFields[i]);
		*/
		
		// change input data files here...
		final String dataFilePath = "dmv_voter_id.txt";
		final String dataFilePathDup = "test_voter.txt";
		
		final String lineDelimiter = "\n";
		final String fieldDelimiter = "\t";//",";//"\t";
		final String includingFields = "0110010000000000";//"0110011111000110";
		final String[] personFields  =  {
				Person.FIRST_NAME_ATTRIBUTE,
				Person.LAST_NAME_ATTRIBUTE,
				Person.ADDRESS_PART_ONE_ATTRIBUTE
				//Person.ADDRESS_PART_TWO_ATTRIBUTE,
				//Person.STATE_ATTRIBUTE,
				//Person.CITY_ATTRIBUTE,
				//Person.ZIP_ATTRIBUTE, 
				//Person.GENDER_CODE_ATTRIBUTE,
				//Person.AGE_ATTRIBUTE
		};
		final boolean ignoreFirstLine = false;
		final boolean withCharacterPadding = true;
		final boolean bloomFilterBuildVariantOne = false;
		
		final int bloomFilterSize = 1000; // 2000
		final int bloomFilterHashes = 15; // 20
		final int ngramValue = 3;		  // 2
		final int numberOfHashFamilies = 5;
		final int numberOfHashesPerFamily = 15;
		final double comparisonThreshold = 0.8;
		
		
		PprlFlinkJob job = new PprlFlinkJob();
		job.setDataFilePath(dataFilePath);
		job.setDataFilePathDup(dataFilePathDup);
		job.setLineDelimiter(lineDelimiter);
		job.setFieldDelimiter(fieldDelimiter);
		job.setIncludingFields(includingFields);
		job.setPersonFields(personFields);
		job.setIgnoreFirstLine(ignoreFirstLine);
		job.setWithCharacterPadding(withCharacterPadding);
		job.setBloomFilterBuildVariantOne(bloomFilterBuildVariantOne);
		job.setBloomFilterSize(bloomFilterSize);
		job.setBloomFilterHashes(bloomFilterHashes);
		job.setNgramValue(ngramValue);
		job.setNumberOfHashFamilies(numberOfHashFamilies);
		job.setNumberOfHashesPerFamily(numberOfHashesPerFamily);
		job.setComparisonThreshold(comparisonThreshold);
		
		long startTime = System.currentTimeMillis();
		job.runJob();
		long endTime = System.currentTimeMillis();
		
		System.out.println("Calculation time: " + (endTime - startTime));
	}
}	
