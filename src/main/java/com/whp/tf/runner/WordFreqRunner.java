package com.whp.tf.runner;

import org.apache.hadoop.util.ToolRunner;

public class WordFreqRunner{
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordFreqMSNewsRunner(), args));
	}
}
