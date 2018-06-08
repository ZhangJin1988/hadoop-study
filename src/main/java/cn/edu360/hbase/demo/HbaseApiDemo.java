package cn.edu360.hbase.demo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class HbaseApiDemo {
	
	
	/**
	 * 建表
	 * @throws Exception 
	 */
	@Test
	public void testCreateTable() throws Exception {
		
		Configuration conf = HBaseConfiguration.create(); 
		conf.set("hbase.zookeeper.quorum", "cts02:2181,cts03:2181,cts04:2181");
		
		Connection conn = ConnectionFactory.createConnection(conf);
		
		// DDL操作工具
		Admin admin = conn.getAdmin();
		
		// 创建一个表定义描述对象
		HTableDescriptor tUser = new HTableDescriptor(TableName.valueOf("t_user"));
		
		// 构造一个列族描述对象
		HColumnDescriptor f1 = new HColumnDescriptor("f1");
		HColumnDescriptor f2 = new HColumnDescriptor("f2");
		
		// 在表描述对象中加入列族描述
		tUser.addFamily(f1);
		tUser.addFamily(f2);
		
		
		// 调用admin的建表方法来建表
		admin.createTable(tUser);
		
		// 关闭连接
		admin.close();
		conn.close();
		
		
	}
	
	
	/**
	 * 修改表定义
	 * @throws Exception 
	 */
	@Test
	public void testAlterTable() throws Exception {
		
		Configuration conf = HBaseConfiguration.create(); 
		conf.set("hbase.zookeeper.quorum", "cts02:2181,cts03:2181,cts04:2181");
		
		Connection conn = ConnectionFactory.createConnection(conf);
		
		// DDL操作工具
		Admin admin = conn.getAdmin();
		
		HTableDescriptor user = admin.getTableDescriptor(TableName.valueOf("t_user"));
		
		HColumnDescriptor f3 = new HColumnDescriptor("f3");
		f3.setMaxVersions(3);
		user.addFamily(f3);
		
		admin.modifyTable(TableName.valueOf("t_user"), user);
		
		
		admin.close();
		conn.close();
	}
	
	/**
	 * 删除表
	 */
	@Test
	public void testDropTable() throws Exception {
		
		Configuration conf = HBaseConfiguration.create(); 
		conf.set("hbase.zookeeper.quorum", "cts02:2181,cts03:2181,cts04:2181");
		
		Connection conn = ConnectionFactory.createConnection(conf);
		
		// DDL操作工具
		Admin admin = conn.getAdmin();
		
		// 先禁用
		admin.disableTable(TableName.valueOf("t_user"));
		
		// 再删除
		admin.deleteTable(TableName.valueOf("t_user"));
		
		
		admin.close();
		conn.close();
	}
	

	/**
	 * DML：插入 | 更新
	 * @throws Exception 
	 */
	@Test
	public void testPut() throws Exception {
		
		Configuration conf = HBaseConfiguration.create(); 
		conf.set("hbase.zookeeper.quorum", "cts02:2181,cts03:2181,cts04:2181");
		
		Connection conn = ConnectionFactory.createConnection(conf);
		
		Table table = conn.getTable(TableName.valueOf("t_user"));
		
		Put put1 = new Put("001".getBytes());
		put1.addColumn("f1".getBytes(), "name".getBytes(), "鲁智深".getBytes());
		put1.addColumn("f1".getBytes(), Bytes.toBytes("age"), Bytes.toBytes(28));
		
		
		Put put2 = new Put("002".getBytes());
		put2.addColumn("f1".getBytes(), "name".getBytes(), "镇关西".getBytes());
		put2.addColumn("f1".getBytes(), Bytes.toBytes("age"), Bytes.toBytes(38));
		
		
		ArrayList<Put> puts = new ArrayList<>();
		puts.add(put1);
		puts.add(put2);
		
		table.put(puts);
		
		table.close();
		conn.close();
		
		
	}
	
	
	
	/**
	 * 删
	 */
	@Test
	public void testDelete() throws Exception {
		
		
		Configuration conf = HBaseConfiguration.create(); 
		conf.set("hbase.zookeeper.quorum", "cts02:2181,cts03:2181,cts04:2181");
		
		Connection conn = ConnectionFactory.createConnection(conf);
		
		Table table = conn.getTable(TableName.valueOf("t_user"));
		
		Delete delete = new Delete("0020".getBytes());
		//delete.addColumn("f1".getBytes(), "name".getBytes());
		
		
		table.delete(delete);
		
		table.close();
		conn.close();
	}
	
	
	/**
	 * DML：查  -->  一次查一行
	 * @throws Exception 
	 */
	@Test
	public void testGet() throws Exception {
		
		Configuration conf = HBaseConfiguration.create(); 
		conf.set("hbase.zookeeper.quorum", "cts02:2181,cts03:2181,cts04:2181");
		
		Connection conn = ConnectionFactory.createConnection(conf);
		
		Table table = conn.getTable(TableName.valueOf("t_user"));
		
		Get get = new Get("002".getBytes());
		//get.addColumn("f1".getBytes(), "name".getBytes());
		
		// 调用table的get方法去hbase中查询该行的keyvalue
		Result result = table.get(get);
		
		// 从结果result对象中获取我们指定的key的value
		/*byte[] name = result.getValue("f1".getBytes(), "name".getBytes());
		byte[] age = result.getValue("f1".getBytes(), "age".getBytes());
		
		System.out.println(new String(name));
		System.out.println(Bytes.toInt(age));*/
		
		
		// 如果我们压根不知道表中有哪些key，就只能从result中逐个key-value(cell)进行遍历
		while(result.advance()) {
			Cell cell = result.current();
			/*byte[] rowArray = cell.getRowArray();
			byte[] familyArray = cell.getFamilyArray();
			byte[] qualifierArray = cell.getQualifierArray();
			byte[] valueArray = cell.getValueArray();
			
			System.out.println(new String(rowArray,cell.getRowOffset(),cell.getRowLength()));
			System.out.println(new String(familyArray,cell.getFamilyOffset(),cell.getFamilyLength()));
			System.out.println(new String(qualifierArray,cell.getQualifierOffset(),cell.getQualifierLength()));
			System.out.println(new String(valueArray,cell.getValueOffset(),cell.getValueLength()));*/
			
			byte[] cloneRow = CellUtil.cloneRow(cell);
			byte[] cloneFamily = CellUtil.cloneFamily(cell);
			byte[] cloneQualifier = CellUtil.cloneQualifier(cell);
			byte[] cloneValue = CellUtil.cloneValue(cell);
			
			System.out.println(new String(cloneRow));
			System.out.println(new String(cloneFamily));
			System.out.println(new String(cloneQualifier));
			System.out.println(new String(cloneValue));
			
		}
		table.close();
		conn.close();
	}
	
	/**
	 * DML: scan范围查询
	 * @throws Exception 
	 */
	@Test
	public void testScan() throws Exception {
		Configuration conf = HBaseConfiguration.create(); 
		conf.set("hbase.zookeeper.quorum", "cts02:2181,cts03:2181,cts04:2181");
		
		Connection conn = ConnectionFactory.createConnection(conf);
		
		Table table = conn.getTable(TableName.valueOf("t_user"));
		
		// scan范围指定时，含首不含尾
		Scan scan = new Scan("001".getBytes(), "002\000".getBytes());
		ResultScanner scanner = table.getScanner(scan);
		
		for (Result result : scanner) {
			while(result.advance()) {
				Cell cell = result.current();
				byte[] cloneRow = CellUtil.cloneRow(cell);
				byte[] cloneFamily = CellUtil.cloneFamily(cell);
				byte[] cloneQualifier = CellUtil.cloneQualifier(cell);
				byte[] cloneValue = CellUtil.cloneValue(cell);
				
				System.out.println(new String(cloneRow));
				System.out.println(new String(cloneFamily));
				System.out.println(new String(cloneQualifier));
				System.out.println(new String(cloneValue));
			}
			System.out.println("-----------------------------");
			
		}
		
		
	}
	
	

}
