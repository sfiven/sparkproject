package com.sfiven.sparkproject.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * JDBC增删改查示范类
 * 
 * JDBC只是java程序操作数据库中最原始和最基础的一种方式
 * 
 * @author Administrator
 *
 */
@SuppressWarnings("unused")
public class JdbcCRUD {

	public static void main(String[] args) {
//		insert();
//		update();
//		delete();
//		select();
		preparedStatement(); 
	}
	
	/**
	 * 测试插入数据
	 */
	private static void insert() {
		// JDBC基本的使用过程
		// 1、加载驱动类：Class.forName()
		// 2、获取数据库连接：DriverManager.getConnection()
		// 3、创建SQL语句执行句柄：Connection.createStatement()
		// 4、执行SQL语句：Statement.executeUpdate()
		// 5、释放数据库连接资源：finally，Connection.close()
		
		// 定义数据库连接对象
		// 引用JDBC相关的所有接口或者是抽象类的时候，必须是引用java.sql包下的
		Connection conn = null;
		
		// 定义SQL语句执行句柄：Statement对象
		// Statement对象，底层会基于Connection数据库连接
		Statement stmt = null;
		
		try {
			// 第一步，加载数据库的驱动，我们都是面向java.sql包下的接口在编程
			Class.forName("com.mysql.jdbc.Driver");  
			
			// 获取数据库的连接
			// 使用DriverManager.getConnection()方法获取针对数据库的连接
			// 需要给方法传入三个参数，包括url、user、password
			// 其中url就是有特定格式的数据库连接串，包括“主协议:子协议://主机名:端口号//数据库”
			conn = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/spark_project", 
					"root", 
					"root");  
			
			// 基于数据库连接Connection对象，创建SQL语句执行句柄，Statement对象
			stmt = conn.createStatement();
			
			// 然后就可以基于Statement对象，来执行insert SQL语句了
			// 插入一条数据
			// Statement.executeUpdate()方法，就可以用来执行insert、update、delete语句
			// 返回类型是个int值，也就是SQL语句影响的行数
			String sql = "insert into test_user(name,age) values('李四',26)";  
			int rtn = stmt.executeUpdate(sql);    
			
			System.out.println("SQL语句影响了【" + rtn + "】行。");  
		} catch (Exception e) {
			e.printStackTrace();  
		} finally {
			// 最后一定要记得在finally代码块中，尽快在执行完SQL语句之后，就释放数据库连接
			try {
				if(stmt != null) {
					stmt.close();
				} 
				if(conn != null) {
					conn.close();  
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
	
	/**
	 * 测试更新数据
	 */
	private static void update() {
		Connection conn = null;
		Statement stmt = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");  
			
			conn = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/spark_project", 
					"root", 
					"root"); 
			stmt = conn.createStatement();
			
			String sql = "update test_user set age=27 where name='李四'";
			int rtn = stmt.executeUpdate(sql);
			
			System.out.println("SQL语句影响了【" + rtn + "】行。");  
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(stmt != null) {
					stmt.close();
				} 
				if(conn != null) {
					conn.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace(); 
			}
		}
	}
	
	/**
	 * 测试删除数据
	 */
	private static void delete() {
		Connection conn = null;
		Statement stmt = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");  
			
			conn = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/spark_project", 
					"root", 
					"root"); 
			stmt = conn.createStatement();
			
			String sql = "delete from test_user where name='李四'";
			int rtn = stmt.executeUpdate(sql);
			
			System.out.println("SQL语句影响了【" + rtn + "】行。");  
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(stmt != null) {
					stmt.close();
				} 
				if(conn != null) {
					conn.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace(); 
			}
		}
	}
	
	/**
	 * 测试查询数据
	 */
	private static void select() {
		Connection conn = null;
		Statement stmt = null;
		// 对于select查询语句，需要定义ResultSet
		// ResultSet就代表了，你的select语句查询出来的数据
		// 需要通过ResutSet对象，来遍历你查询出来的每一行数据，然后对数据进行保存或者处理
		ResultSet rs = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");  
			
			conn = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/spark_project", 
					"root", 
					"root"); 
			stmt = conn.createStatement();
			
			String sql = "select * from test_user";
			rs = stmt.executeQuery(sql);
			
			// 获取到ResultSet以后，就需要对其进行遍历，然后获取查询出来的每一条数据
			while(rs.next()) {
				int id = rs.getInt(1);
				String name = rs.getString(2);
				int age = rs.getInt(3);
				System.out.println("id=" + id + ", name=" + name + ", age=" + age);    
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(stmt != null) {
					stmt.close();
				} 
				if(conn != null) {
					conn.close();  
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
	
	/**
	 * 测试PreparedStatement
	 */
	private static void preparedStatement() {
		Connection conn = null;
		PreparedStatement pstmt = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");  
			
			conn = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/spark_project?characterEncoding=utf8", 
					"root", 
					"root");  
			
			// 第一个，SQL语句中，值所在的地方，都用问好代表
			String sql = "insert into test_user(name,age) values(?,?)";
			
			pstmt = conn.prepareStatement(sql);
			
			// 第二个，必须调用PreparedStatement的setX()系列方法，对指定的占位符设置实际的值
			pstmt.setString(1, "李四");  
			pstmt.setInt(2, 26);  
			
			// 第三个，执行SQL语句时，直接使用executeUpdate()即可，不用传入任何参数
			int rtn = pstmt.executeUpdate();    
			
			System.out.println("SQL语句影响了【" + rtn + "】行。");  
		} catch (Exception e) {
			e.printStackTrace();  
		} finally {
			try {
				if(pstmt != null) {
					pstmt.close();
				} 
				if(conn != null) {
					conn.close();  
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
	
}
