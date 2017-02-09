package com.whp.data;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;


public class Test {
	public static void main(String args[]){
		String driver = "com.mysql.jdbc.Driver";
		String url =  "jdbc:mysql://10.1.3.198:3306/fulldata";
		String user =  "root";
		String password =  "Neus0ft.";
		String tmp = null;
		Map<String, String> m = new HashMap<String, String>();
		try {
			FileOutputStream out = new FileOutputStream(new File("ms.txt"));   
			Class.forName(driver);
			Connection conn = DriverManager.getConnection(url, user, password);
			if (!conn.isClosed()) {
				System.out.println("Succeeded connecting to the Database!");
				Statement statement = conn.createStatement();
				int t = 2970;
				int p = 100;
				System.out.println(System.currentTimeMillis());
				for (int i = 0; i < t / p; ++i){
					String sql = String.format("SELECT category,type,news,_date FROM ms_news_data ORDER BY id ASC LIMIT %d,%d", p * i, p * (i + 1) > t ? t : p * (i + 1));
					ResultSet rs = statement.executeQuery(sql);
					while(rs.next()) {
						tmp = rs.getString("_date") + "," +rs.getString("type") + "," +rs.getString("category") + "," +rs.getString("news") + "\n" ;
						out.write(tmp.getBytes());
					}
				}
				System.out.println(System.currentTimeMillis());
				System.out.println(m.size());
			} else {
				System.out.println("db error");
			}
			out.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	} 
}
