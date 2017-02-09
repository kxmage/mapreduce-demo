package com.whp.data.mapper;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MSNewsFactorMapper extends TableMapper<Text, Text>{

	private static final String FAMILY_DATA_INFO = "data_info";
	private static final String FAMILY_CLUSTER_INFO = "cluster_info";
	private static final String KEY_CLUSTER_0 = "ClusterId0";
	private static final String KEY_TITLE = "Title";
	private static final String KEY_TEXT = "NewsArticleDescription";
	private static final String KEY_CATEGORY = "NewsArticleCategory";
	
	private String tmp_id0 = null;
	private String tmp_full_text = null;
	private String tmp_category = null;
	private String d = null;
	
	private int pos = new String("20xx-mm-dd").length();
	
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
		tmp_id0 = getString(value, FAMILY_CLUSTER_INFO, KEY_CLUSTER_0);
		tmp_full_text = getString(value, FAMILY_DATA_INFO, KEY_TITLE) + getString(value, FAMILY_DATA_INFO, KEY_TEXT);
		tmp_category = getString(value, FAMILY_DATA_INFO, KEY_CATEGORY);
		
		d = Bytes.toString(key.get()).substring(0, pos);
		
		context.write(new Text(d + ";;ALL;;" + tmp_category), new Text(d + "\tALL\t" + tmp_category + "\t" + tmp_id0 + "\t" + "1"));
		
		if (StringUtils.contains(tmp_full_text, "上海")) {
			context.write(new Text(d + ";;上海;;" + tmp_category), new Text(d + "\t上海\t" + tmp_category + "\t" + tmp_id0 + "\t" + "1"));
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
