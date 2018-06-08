package day05.cn.edu360.mr.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * jdbc数据加载工具，把用户表数据加载到一个hashmap中
 * @author hunter.d
 * @create_time 2018年4月14日
 * @copyright www.edu360.cn
 */
public class UserDataLoader {

	public static void loadUserData(HashMap<String, User> map) {

		Connection conn = null;
		PreparedStatement prepareStatement = null;
		ResultSet rs = null;
		try {
			// 注册 JDBC 驱动
			Class.forName("com.mysql.jdbc.Driver");

			// 打开链接
			conn = DriverManager.getConnection("jdbc:mysql://cts01:3306/user", "root", "root");

			// 执行查询
			prepareStatement = conn.prepareStatement("SELECT id,name, age, addr FROM t_user");
			rs = prepareStatement.executeQuery();

			while (rs.next()) {
				String id = rs.getString("id");
				String name = rs.getString("name");
				int age = rs.getInt("age");
				String addr = rs.getString("addr");
				User user = new User();
				user.set(name, age, addr);

				map.put(id, user);

			}

			rs.close();
			prepareStatement.close();
			conn.close();
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (prepareStatement != null)
					prepareStatement.close();
			} catch (SQLException se2) {
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
	}

}
