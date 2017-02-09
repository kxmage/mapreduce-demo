package com.whp.data.reducer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.whp.data.obj.MSNewsObj;

public class MSNewsFactorReducer extends Reducer<Text, Text, Text, Text>{

	private Connection conn = null;
	private Statement statement = null;
	
	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		try {
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

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder(10000);
		String[] ks = key.toString().split(";;");
		if (ks.length != 3) return;
		String _date = ks[0];
		String _type = ks[1];
		String _category = ks[2];
		int t = 0;
		for (Text val : values) {
			MSNewsObj obj = MSNewsObj.fromString(val.toString());
			sb.append(obj.getId0() + "_");
			t += obj.getNum();
		}
//		context.write(key, new Text(String.format("%d\t%s", t, sb)));
		String sql = String.format("INSERT INTO ms_news_data (id, category, type, news, _date, ids) VALUES ('%s', '%s', '%s', %d, '%s', '%s')", 
				key.toString(),
				_category,
				_type,
				t,
				_date,
				sb.toString());
		try {
			statement.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		String driver = "com.mysql.jdbc.Driver";
		String url =  "jdbc:mysql://10.1.3.198:3306/fulldata";
		String user =  "root";
		String password =  "Neus0ft.";
		
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, user, password);
			if (!conn.isClosed()) {
				System.out.println("Succeeded connecting to the Database!");
				statement = conn.createStatement();
			} else {
				System.out.println("db error");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
