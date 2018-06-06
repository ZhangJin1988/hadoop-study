package day02.cn.edu360.hdfs.datacollect;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimerTask;

public class BackCleanTask extends TimerTask {


	@Override
	public void run() {
		try {
			Properties props = PropsHolder_hungry.getProps();
			
			Date dateNow = new Date();
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
			// 检测备份目录中的子文件夹
			File backupDir = new File("d:/backup");
			File[] subDirs = backupDir.listFiles();

			// 判断子文件夹是否已经超过24小时
			for (File dir : subDirs) {
				String name = dir.getName();
				Date dateBack = sdf.parse(name);
				if(dateNow.getTime() - dateBack.getTime() > 24*60*60*1000) {
					FileUtils.deleteDirectory(dir);
				}
				
			}

		} catch (Exception e) {

			e.printStackTrace();
		}

	}
	
	

}
