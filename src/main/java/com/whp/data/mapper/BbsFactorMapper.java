package com.whp.data.mapper;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BbsFactorMapper extends TableMapper<Text, IntWritable>{

	private static final String FAMILY_BBSINFO = "bbsinfo";
	private static final String KEY_TITLE = "title";
	private static final String KEY_TEXT = "text";
	private static final String KEY_PLATFORM = "platform";
	
	private String tmp_full_text = null;
	private String d = null;
	private IntWritable ONE = new IntWritable(1);
	
	private int pos = new String("20xx-mm-dd").length();
	private String platform = null;
	
	@Override
	protected void cleanup(Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
					throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		platform = getString(value, FAMILY_BBSINFO, KEY_PLATFORM);
		if (!StringUtils.equals(platform, "4")) return;
		tmp_full_text = getString(value, FAMILY_BBSINFO, KEY_TITLE) + getString(value, FAMILY_BBSINFO, KEY_TEXT);
		
		d = Bytes.toString(key.get()).substring(0, pos);
		
		if (StringUtils.contains(tmp_full_text, "极限挑战")) {
			context.write(new Text(d), ONE);
		}
		
	}

	@Override
	protected void setup(Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}
	
	private static String getString(Result result, String family, String column) {
		byte[] value = result.getValue(Bytes.toBytes(family), Bytes.toBytes(column));
		return value == null ? null : Bytes.toString(value);
	}

}
