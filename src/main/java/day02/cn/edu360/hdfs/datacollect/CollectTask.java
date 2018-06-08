package day02.cn.edu360.hdfs.datacollect;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimerTask;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 采集任务
 * 
 * @author hunter.d
 * @create_time 2018年4月9日
 * @copyright www.edu360.cn
 */
public class CollectTask extends TimerTask {

	@Override
	public void run() {
		try {
			// 加载配置文件
			Properties props = PropsHolder_hungry.getProps();
			
			
			// 1.检测日志服务器的日志目录中有哪些需要采集的文件
			File logDir = new File(props.getProperty(ParamConstants.LOG_GEN_PATH));
			File[] logFiles = logDir.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.startsWith(props.getProperty(ParamConstants.LOG_NAME_PREFIX));
				}
			});

			// 2.将检测到的文件移动到待上传目录
			// a.判断待上传目录是否存在，如果不存在则创建
			File file = new File("D:/toupload");
			if (!file.exists()) {
				file.mkdirs();
			}
			// b.将日志从日志生成目录移动到待上传目录
			for (File f : logFiles) {
				f.renameTo(new File("D:/toupload/" + f.getName()));
			}

			// 3.遍历待上传目录，将文件逐个上传到HDFS
			// a.获取一个访问HDFS的客户端对象
			FileSystem fs = FileSystem.get(new URI("hdfs://cts01:9000/"), new Configuration(), "root");
			
			// b.判断hdfs中的目标存储目录是否存在
			Date date = new Date();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
			Path path = new Path("/logs/" + sdf.format(date).substring(0, 10));
			if (!fs.exists(path)) {
				fs.mkdirs(path);
			}
			
			// c.准备好本地备份目录
			File backDir = new File("d:/backup/"+sdf.format(date));
			if(!backDir.exists()) {
				backDir.mkdirs();
			}
			

			// d.列出待上传目录的文件
			File toUploadDir = new File("d:/toupload");
			File[] toUpFiles = toUploadDir.listFiles();
			for (File f : toUpFiles) {
				fs.copyFromLocalFile(new Path(f.getPath()), new Path(path.toString()+"/"+f.getName()+"_"+UUID.randomUUID()));
				// e.将上传完的文件移动到日志服务器本地的备份目录
				f.renameTo(new File(backDir.getPath()+"/"+f.getName()));
			}

			fs.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
