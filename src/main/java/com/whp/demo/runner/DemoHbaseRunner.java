package com.whp.demo.runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;
import com.whp.demo.mapper.DemoHbaseMapper;
import com.whp.demo.reducer.DemoReducer;

public class DemoHbaseRunner extends Configured implements Tool {

	private static final String HBASE_DATA_SOURCE = "";
	
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Configuration config = HBaseConfiguration.create();
		Job job = Job.getInstance(config, this.getClass().getName());
		job.setJarByClass(this.getClass());
		
		Scan scan = new Scan();
		//设置一次RPC请求的数据条数，
		//map中处理简单的时候，caching可以设置较大的值
		//map中处理复杂时需要适当减小caching，否则产生超时错误；默认每次RPC请求的处理时间为60000ms。
		//可以通过如下操作修改超时时间（不建议），应优先考虑提高map处理效率，其次考虑降低caching数值，
		//config.setLong(HConstants. HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD , 120000);
		scan.setCaching(1000);
		
		//设置处理范围：rowKey，字典序，start<= rowKey <stop
		scan.setStartRow("".getBytes());
		scan.setStopRow("".getBytes());
		
		//不启用缓存
		//多数情况下，mr中hbase的数据是使用一次，关闭缓存降低数据访问速度，反而会降低交换缓存的操作消耗，提高效率
		scan.setCacheBlocks(false);
		
		//初始化hbase
		//params:table, scan, mapper.class, mapper_output_key, mapper_output_value, job
		TableMapReduceUtil.initTableMapperJob(HBASE_DATA_SOURCE, scan, DemoHbaseMapper.class, Text.class, IntWritable.class, job);
		
		//设置reduce输出路径
		FileOutputFormat.setOutputPath(job, new Path("hdfs output path"));
		
		//设置mapper, combiner, reducer
		job.setMapperClass(DemoHbaseMapper.class);
		job.setCombinerClass(DemoReducer.class);
		job.setReducerClass(DemoReducer.class);
		
		//设置reduce output kv
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		
		//同步阻塞执行
		return job.waitForCompletion(true) ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new DemoHbaseRunner(), args));
	}
}
