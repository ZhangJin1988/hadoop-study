package day02.cn.edu360.hdfs.datacollect;

import java.io.IOException;
import java.util.Properties;

public class PropsHolder_hungry {
	
	private static Properties props =  new Properties();
	static {
		try {
			props.load(PropsHolder_hungry.class.getClassLoader().getResourceAsStream("collect.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public static  Properties getProps() {
		
		
		return props;
	}

}
