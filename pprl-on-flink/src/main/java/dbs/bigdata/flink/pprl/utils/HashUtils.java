package dbs.bigdata.flink.pprl.utils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import scala.collection.generic.BitOperations.Int;

/**
 * Use this class for generate the md5 oder sha hashes of
 * strings or bit sets.
 * 
 * @author mfranke
 */
public class HashUtils {
	/**
	 * Calculates the MD5 hash for a string input.
	 * 
	 * @param input
	 * 		-> a {@link String} object.
	 * 
	 * @return
	 * 		-> the {@link Int} representation of the MDH5 hash value.
	 */
	public static int getMD5(String input) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] messageDigest = md.digest(input.getBytes());            
			BigInteger number = new BigInteger(1, messageDigest);
			return number.intValue();
		}
		catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Calculates the SHA hash for a string input.
	 * 
	 * @param input
	 * 		-> a {@link String} object.
	 * 
	 * @return
	 * 		-> the {@link Int} representation of the SHA hash value.
	 */
	public static int getSHA(String input) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA");
			byte[] messageDigest = md.digest(input.getBytes());            
			BigInteger number = new BigInteger(1, messageDigest);
			return number.intValue();
		}
		catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Calculates the SHA hash for a string input.
	 * 
	 * @param input
	 * 		-> a {@link String} object.
	 * 
	 * @return
	 * 		-> the {@link long} representation of the SHA hash value.
	 */	
	public static long getSHALongHash(String input){
		try{
			MessageDigest md = MessageDigest.getInstance("SHA");
			byte[] messageDigest = md.digest(input.getBytes());            
			BigInteger number = new BigInteger(1, messageDigest);
			return number.longValue();
		}
		catch (NoSuchAlgorithmException e){
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Calculates the MD5 hash for a BitSet input.
	 * 
	 * @param input
	 * 		-> a {@link String} object.
	 * 
	 * @return
	 * 		-> the {@link BitSet} representation of the MD5 hash value.
	 */
	public static int getMD5(BitSet input) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] messageDigest = md.digest(input.toByteArray());            
			BigInteger number = new BigInteger(1, messageDigest);
			return number.intValue();
		}
		catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Calculates the SHA hash for a BitSet input.
	 * 
	 * @param input
	 * 		-> a {@link BitSet} object.
	 * 
	 * @return
	 * 		-> the {@link Int} representation of the SHA hash value.
	 */
	public static int getSHA(BitSet input) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA");
			byte[] messageDigest = md.digest(input.toByteArray());
			BigInteger number = new BigInteger(1, messageDigest);
			return number.intValue();
		}
		catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

}