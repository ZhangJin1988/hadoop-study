package day02.cn.edu360.hdfs.datacollect;

import java.util.Properties;

/**
 * 单例设计模式：懒汉式，而且考虑了线程安全
 * @author hunter.d
 * @create_time 2018年4月9日
 * @copyright www.edu360.cn
 */
public class PropsHolder_lazy {
	private static Properties props = null;

	public static Properties getProps() throws Exception {

		if (props == null) {
			synchronized ("lock") {
				if (props == null) {
					props = new Properties();
					props.load(PropsHolder_lazy.class.getClassLoader().getResourceAsStream("collect.properties"));
				}
			}
		}

		return props;
	}

}
