package com.whp.data.mapper;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JxtzGuestMSNewsFactorMapper extends TableMapper<Text, Text>{

	private static final String FAMILY_DATA_INFO = "data_info";
	private static final String KEY_TITLE = "Title";
	private static final String KEY_TEXT = "NewsArticleDescription";
	
	private String tmp_full_text = null;
	private String d = null;
	
	private int pos = new String("20xx-mm-dd").length();
	private String[] guests = {"黄渤",
			"孙红雷","黄磊","罗志祥", "王迅", 
			"张艺兴", "陈乔恩", "郭涛","周冬雨",
			"陈柏霖", "徐峥", "岳云鹏", "宋小宝", 
			"薛之谦", "林志玲", "吴秀波", "谢娜", 
			"蒋劲夫", "王大陆", "杨魏玲花", "曾毅", 
			"林俊杰", "蔡依林", "庾澄庆","玲花","凤凰传奇"};
	@Override
	protected void cleanup(Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
					throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		tmp_full_text = getString(value, FAMILY_DATA_INFO, KEY_TITLE) + getString(value, FAMILY_DATA_INFO, KEY_TEXT);
		
		d = Bytes.toString(key.get()).substring(0, pos);
		
		for (String s : guests) {
			if (StringUtils.contains(tmp_full_text, s)) {
				context.write(new Text(d), new Text(s + "\t1"));
			}
		}
	}

	@Override
	protected void setup(Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}
	
	private static String getString(Result result, String family, String column) {
		byte[] value = result.getValue(Bytes.toBytes(family), Bytes.toBytes(column));
		return value == null ? null : Bytes.toString(value);
	}

}
