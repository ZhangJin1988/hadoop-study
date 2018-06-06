package day02.cn.edu360.hdfs.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsClientDemo1 {
	
	// 上传整个的文件
	@Test
	public void testUpload() throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("dfs.blocksize", "128m");
		
		// 1.先获取一个访问HDFS的客户端对象   
		// 参数1:URI-hdfs集群的访问地址   参数2:客户端需要携带的参数对象  参数3:指明客户端的身份
		FileSystem fs = FileSystem.get(new URI("hdfs://cts01:9000/"), conf, "root");
		
		// 调用fs的方法上传文件
		fs.copyFromLocalFile(new Path("d:/myapp.jar"), new Path("/my.jar"));
		
		
		// 关闭客户端对象
		fs.close();
		
	}
	
	// 创建目录 
	@Test
	public void testMkdir() throws Exception {
		
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(new URI("hdfs://cts01:9000/"), conf, "root");
		
		fs.mkdirs(new Path("/aaa/bbb"));
		
		// 关闭客户端对象
		fs.close();
		
	}
	
	// 下载整个的文件
	// 下载这个操作，会涉及到客户端本地系统的访问，hadoop为本地访问专门封装了本地平台库(c语言)
	// 具体做法：将涛哥给的本地库压缩包解压到任意位置，并将这个解压目录配置到HADOOP_HOME环境变量中
	@Test
	public void testDownLoad() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://cts01:9000/"), conf, "root");
		
		fs.copyToLocalFile(new Path("/my.jar"), new Path("e:/"));
	
		fs.close();
	}
	
	
	@Test
	public void testDownLoad2() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://cts01:9000/"), conf, "root");
		// 也可以用java原生工具取访问本地平台，这样就不需要hadoop的本地c语言库
		fs.copyToLocalFile(false,new Path("/my.jar"), new Path("f:/"), true);
		
		fs.close();
	}
	
	// 删除文件或文件夹
	@Test
	public void testRm() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://cts01:9000/"), conf, "root");
		// 参数1：要删除的路径    参数2：是否递归删除
		fs.delete(new Path("/aaa"), true);
		
		fs.close();
	}
	
	// 移动或重命名文件或文件夹
	@Test
	public void testMv() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://cts01:9000/"), conf, "root");
		
		fs.rename(new Path("/my.jar"), new Path("/mymy.jar"));
		
		fs.close();
	}
	
	// 判断文件是否存在，判断一个路径是文件夹还是文件
	@Test
	public void testIfExist() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://cts01:9000/"), conf, "root");
		
		boolean exists = fs.exists(new Path("/aaa"));
		System.out.println(exists);
		
		boolean isFile = fs.isFile(new Path("/mymy.jar"));
		System.out.println(isFile);
		
		fs.close();
	}
	
	// 查看目录信息：仅显示文件信息
	@Test
	public void testLs1() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://cts01:9000/"), conf, "root");
		
		// 思考：为何返回迭代器？
		RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);
		while(iterator.hasNext()) {
			LocatedFileStatus file = iterator.next();
			System.out.println("文件的所属组：" + file.getGroup());
			System.out.println("文件的所有者：" + file.getOwner());
			System.out.println("文件的访问时间：" + file.getAccessTime());
			System.out.println("文件的块大小：" + file.getBlockSize());
			System.out.println("文件的总长度：" + file.getLen());
			System.out.println("文件的修改时间：" + file.getModificationTime());
			System.out.println("文件的副本数：" + file.getReplication());
			System.out.println("文件的路径：" + file.getPath());
			System.out.println("文件的权限：" + file.getPermission());
			BlockLocation[] blockLocations = file.getBlockLocations();
			System.out.println("文件的块位置信息---------------------------");
			for (BlockLocation blk : blockLocations) {
				System.out.println("块长度:" + blk.getLength());
				System.out.println("块在文件中的起始偏移量:" + blk.getOffset());
				System.out.println("块所在的datanode主机:" + Arrays.toString(blk.getHosts()));
			}
			System.out.println("文件的块位置信息---------------------------");
		}
		
		
		fs.close();
		
	}
	
	// 查看目录信息：显示文件以及文件夹信息
	@Test
	public void testLs2() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://cts01:9000/"), conf, "root");
		// 思考：为何返回数组？
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus f : listStatus) {
			System.out.println(f.getPath());
		}
		
		fs.close();
	}

}
