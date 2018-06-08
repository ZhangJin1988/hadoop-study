package top.ganhoo.hbase.study;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

public class RowKeyTest {
	
	public static void main(String[] args) throws Exception {
		String m1 = MD5Hash.getMD5AsHex("1".getBytes());
		String m2 = MD5Hash.getMD5AsHex("01".getBytes());
		
		String m3 = MD5Hash.getMD5AsHex("1".getBytes());
		String m4 = MD5Hash.getMD5AsHex("01".getBytes());
		
		System.out.println(m1);
		System.out.println(m2);
		System.out.println(m3);
		System.out.println(m4);
		
		System.out.println(new Object().hashCode());
		System.out.println(new Object().hashCode());

		MessageDigest md = MessageDigest.getInstance("MD5");
		byte[] lb = Bytes.toBytes(1L);
		byte[] sb = Bytes.toBytes("1");
		System.out.println(lb.length);
		System.out.println(sb.length);
		System.out.println(md.digest(lb).length);
		System.out.println(md.digest(Bytes.toBytes("1")).length);
		
		long l = 1234567890L;
		byte[] lb2 = Bytes.toBytes(l);
		System.out.println("long bytes length: " + lb2.length);   // returns 8

		String s = String.valueOf(l);
		byte[] sb2 = Bytes.toBytes(s);
		System.out.println("long as string length: " + sb2.length);    // returns 10
		
		
		
		System.out.println("1");
		System.out.println(String.valueOf(l));
		
		
	}

}
