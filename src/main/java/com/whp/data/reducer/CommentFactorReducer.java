package com.whp.data.reducer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CommentFactorReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	private Connection conn = null;
	private Statement statement = null;
	private Map<String, Integer> result_map = null;
	private String id = null;
	private int t = 0;
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		id = key.toString();
		t = 0;
		for (IntWritable val : values) {
			t += val.get();
		}
		result_map.put(id, t);
	}
	
	@Override
	protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		String driver = "com.mysql.jdbc.Driver";
		String url =  "jdbc:mysql://10.1.3.198:3306/fulldata";
		String user =  "root";
		String password =  "Neus0ft.";
		result_map = new HashMap<String, Integer>();
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, user, password);
			statement = conn.createStatement();
			if (!conn.isClosed()) {
				System.out.println("Succeeded connecting to the Database!");
			} else {
				System.out.println("db error");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		try {
			System.out.println(String.format("total records:%d", result_map.size()));
			String full_sql = null;
			String sql_head = "UPDATE ms_news_data SET comment = comment + CASE";
			StringBuilder sb = new StringBuilder(5000);
			int cnt = 0;
			for (Map.Entry<String, Integer> kv : result_map.entrySet()) {
				sb.append(" WHEN ids LIKE '%" + kv.getKey() + "%' THEN " + kv.getValue().toString());
				++cnt;
				if (cnt == 100) {
					System.out.println(System.currentTimeMillis());
					System.out.println("100 Records;");
					full_sql = sql_head + sb.toString() + " END";
					System.out.println(full_sql);
					statement.executeUpdate(full_sql);
					System.out.println("Insert OK!");
					sb.delete(0, sb.length());
					cnt = 0;
				}
			}
			if (cnt > 0) {
				full_sql = sql_head + sb.toString() + " END";
				System.out.println(full_sql);
				statement.executeUpdate(full_sql);
			}
			
			if (!statement.isClosed()) {
				statement.close();
			}
			if (!conn.isClosed()) {
				conn.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
