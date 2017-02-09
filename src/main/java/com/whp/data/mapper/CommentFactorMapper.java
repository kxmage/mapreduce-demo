package com.whp.data.mapper;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CommentFactorMapper extends TableMapper<Text, IntWritable>{

	private static final String FAMILY_DATA_INFO = "data_info";
	private static final String KEY_ID = "ClusterId0";
	
	private IntWritable ONE = new IntWritable(1);
	
	private String id = null;
	
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
		id = getString(value, FAMILY_DATA_INFO, KEY_ID);
		context.write(new Text(id), ONE);
		
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
