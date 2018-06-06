package day02.cn.edu360.hdfs.datacollect;

import java.util.Timer;

public class DataCollector {
	
	public static void main(String[] args) {
		Timer timer = new Timer();
		// 定时启动数据采集任务
		//timer.schedule(new CollectTask(), 0, 2*60*1000);
		
		// 定时启动备份清理任务
		timer.schedule(new BackCleanTask(), 5*1000, 5*1000);
	}
	
	
	

}
