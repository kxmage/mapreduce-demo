package com.whp.demo.runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;
import com.whp.demo.mapper.DemoHdfsMapper;
import com.whp.demo.reducer.DemoReducer;

public class DemoHdfsRunner extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration config = new Configuration();
		
		Job job = Job.getInstance(config, this.getClass().getName());
		job.setJarByClass(this.getClass());
		
		job.setMapperClass(DemoHdfsMapper.class);
		job.setCombinerClass(DemoReducer.class);
		job.setReducerClass(DemoReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new DemoHdfsRunner(), args));
	}
}
