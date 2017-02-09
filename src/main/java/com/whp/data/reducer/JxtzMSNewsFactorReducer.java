package com.whp.data.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JxtzMSNewsFactorReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int t = 0;
		for (IntWritable val : values) {
			t += val.get();
		}
		context.write(key, new IntWritable(t));
	}

}
