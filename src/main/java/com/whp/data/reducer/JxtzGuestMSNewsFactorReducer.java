package com.whp.data.reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JxtzGuestMSNewsFactorReducer extends Reducer<Text, Text, Text, Text>{

	private String[] guests = {"黄渤","孙红雷","黄磊","罗志祥", "王迅", "张艺兴", "陈乔恩", "郭涛","周冬雨","陈柏霖", "徐峥", "岳云鹏", "宋小宝", "薛之谦", "林志玲", "吴秀波", "谢娜", "蒋劲夫", "王大陆", "杨魏玲花", "曾毅", "林俊杰", "蔡依林", "庾澄庆","玲花","凤凰传奇"};
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Map<String, Integer> guest_map = new HashMap<String, Integer>();
		
		for (String s : guests) {
			guest_map.put(s, 0);
		}
		
		for (Text val : values) {
			String[] strs = val.toString().split("\t");
			if (strs.length != 2) continue;
			guest_map.put(strs[0], guest_map.get(strs[0]) + Integer.parseInt(strs[1]));
		}
		
		String r_v = "";
		
		for (Map.Entry<String, Integer> kv : guest_map.entrySet()) {
			r_v += kv.getKey() + "\t" + kv.getValue() + "\t";
		}
		
		context.write(key, new Text(r_v));
	}

}
