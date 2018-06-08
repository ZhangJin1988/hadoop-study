package top.ganhoo.hbase.baseapi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class HbaseClientDemo {
	Connection conn = null;
	@Before
	public void init() throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hdp-01:2181,hdp-02:2181,hdp-03:2181");
		
		conn = ConnectionFactory.createConnection(conf);
	}
	
	
	/**
	 * 建表
	 * @throws Exception 
	 */
	@Test
	public void testCreateTable() throws Exception {

		Admin admin = conn.getAdmin();

		
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("t_stu3"));
		
		HColumnDescriptor base_info = new HColumnDescriptor("b");
		HColumnDescriptor extra_info = new HColumnDescriptor("e");
		
		hTableDescriptor.addFamily(base_info);
		hTableDescriptor.addFamily(extra_info);
		
		//admin.createTable(hTableDescriptor);
		
		// 预分区
		//admin.createTable(hTableDescriptor, "00".getBytes(), "99".getBytes(), 10);
		
		byte[][] splitKeys = new byte[10][];
		splitKeys[0] = "00".getBytes();
		splitKeys[1] = "10".getBytes();
		splitKeys[2] = "20".getBytes();
		splitKeys[3] = "30".getBytes();
		splitKeys[4] = "40".getBytes();
		splitKeys[5] = "50".getBytes();
		splitKeys[6] = "60".getBytes();
		splitKeys[7] = "70".getBytes();
		splitKeys[8] = "80".getBytes();
		splitKeys[9] = "90".getBytes();
		
		
		//admin.createTable(hTableDescriptor, splitKeys);
		admin.createTable(hTableDescriptor, "00000000".getBytes(), "ffffffff".getBytes(), 5);
		admin.close();
		conn.close();
	}
	
	/**
	 * @throws Exception 
	 */
	@Test
	public void testPut() throws Exception {
		
		Table table = conn.getTable(TableName.valueOf("t_stu3"));
		
		Put put = new Put("rk001".getBytes());
		put.addColumn(Bytes.toBytes("b"), Bytes.toBytes("stuno"), Bytes.toBytes(1));
		put.addColumn(Bytes.toBytes("b"), Bytes.toBytes("name"), Bytes.toBytes("zs"));
		put.addColumn(Bytes.toBytes("e"), Bytes.toBytes("addr"), Bytes.toBytes("beijing"));
		
		table.put(put);
		
		
		table.close();
		conn.close();
	}
	
	
	@Test
	public void testPutAndCheck() throws Exception {
		
		Table table = conn.getTable(TableName.valueOf("t_stu3"));
		
		Put put = new Put("rk001".getBytes());
		put.addColumn(Bytes.toBytes("b"), Bytes.toBytes("stuno"), Bytes.toBytes(1));
		put.addColumn(Bytes.toBytes("b"), Bytes.toBytes("name"), Bytes.toBytes("zs"));
		put.addColumn(Bytes.toBytes("e"), Bytes.toBytes("addr"), Bytes.toBytes("shanghai"));
		put.addColumn(Bytes.toBytes("e"), Bytes.toBytes("phone"), Bytes.toBytes("138138"));
		
		boolean checkAndPut = table.checkAndPut("rk001".getBytes(), Bytes.toBytes("b"), Bytes.toBytes("stuno"),  Bytes.toBytes(3), put);
		System.out.println(checkAndPut);
		
		boolean checkAndPut2 = table.checkAndPut("rk001".getBytes(), Bytes.toBytes("b"), Bytes.toBytes("stuno"),  Bytes.toBytes(2), put);
		System.out.println(checkAndPut2);
		
		boolean checkAndPut3 = table.checkAndPut("rk001".getBytes(), Bytes.toBytes("b"), Bytes.toBytes("stuno"), CompareOp.LESS, Bytes.toBytes(5), put);
		System.out.println(checkAndPut3);
		
		
		table.close();
		conn.close();
	}
	
	
	
	/**
	 * @throws Exception 
	 */
	@Test
	public void testDelete() throws Exception {
		
		Table table = conn.getTable(TableName.valueOf("t_stu"));
		
		Delete delete = new Delete(Bytes.toBytes("rk001"));
		//table.delete(delete);
		
		delete.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("stuno"));
		delete.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("addr"));
		table.delete(delete);
		
		table.close();
		conn.close();
		
	}
	
	
	
	/**
	 * ��
	 * @throws Exception 
	 */
	@Test
	public void testUpdate() throws Exception {
		Table table = conn.getTable(TableName.valueOf("t_stu"));
		
		Put put = new Put("rk001".getBytes());
		put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes("hunter.ganhoo"));
		
		table.put(put);
		
		table.close();
		conn.close();
	}
	
	
	/**
	 * ��:���в�ѯ���Ҵӽ����ȡָ����key��value
	 * @throws Exception 
	 */
	
	@Test
	public void testGet1() throws Exception {
		Table table = conn.getTable(TableName.valueOf("t_stu"));
		
		Get get = new Get(Bytes.toBytes("rk001"));
		get.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("name"));
		
		Result result = table.get(get);
		
		byte[] value = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("name.."));
		
		System.out.println(new String(value,"UTF-8"));
		
		table.close();
		conn.close();
		
	}
	
	
	/**
	 * @throws Exception
	 */
	@Test
	public void testGet2() throws Exception {
		Table table = conn.getTable(TableName.valueOf("t_stu3"));
		
		Get get = new Get(Bytes.toBytes("rk001"));
		
		Result result = table.get(get);

		CellScanner cellScanner = result.cellScanner();
		while(cellScanner.advance()) {
			Cell cell = cellScanner.current();
			byte[] f = CellUtil.cloneFamily(cell);
			byte[] r = CellUtil.cloneRow(cell);
			byte[] q = CellUtil.cloneQualifier(cell);
			byte[] v = CellUtil.cloneValue(cell);
			
			System.out.println(new String(r));
			System.out.println(new String(f));
			System.out.println(new String(q));
			
			String qStr = new String(q);
			if(qStr.equals("stuno")) {
				System.out.println(Bytes.toInt(v));
			}else {
				System.out.println(new String(v));
			}
			
			System.out.println();
		}
		
		table.close();
		conn.close();
		
	}
	
	
	/**
	 * @throws IOException 
	 */
	@Test
	public void testScan() throws IOException {
		
		Table table = conn.getTable(TableName.valueOf("t_stu"));
		
		Scan scan = new Scan(Bytes.toBytes("rk001"), Bytes.toBytes("rk002"+"\000")); 
		ResultScanner scanner = table.getScanner(scan);
		
		Iterator<Result> iter = scanner.iterator();
		while(iter.hasNext()) {
			Result result = iter.next();
			byte[] value = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("name"));
			System.out.println(new String(value,"UTF-8"));
			
		}
		
		scanner.close();
		table.close();
		conn.close();
		
	}

}
