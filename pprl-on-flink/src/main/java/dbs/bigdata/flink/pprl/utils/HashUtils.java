package dbs.bigdata.flink.pprl.utils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;

public class HashUtils {
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