package com.whp.tf.mapper;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//import com.neusoft.nlp.tokenizer.tools.wordsextractor.Word;
//import com.neusoft.nlp.tokenizer.tools.wordsextractor.WordsExtractor;

public class WordFreqMSNewsMapper extends TableMapper<Text, IntWritable>{

//	private WordsExtractor wordsExtractor = new WordsExtractor();
	
//	private static final String DATA_SOURCE = "NewsStream";
	private static final String FAMILY = "data_info";
	private static final String KEY_TEXT = "NewsArticleDescription";
	private static final String word_cls="an;i;n;nr;ns;nt;nz;t;v;vn";
	private static final IntWritable ONE = new IntWritable(1);
	private String text = null;
//	private List<Word> words = null;
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
		text = Bytes.toString(value.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(KEY_TEXT)));
//		words = wordsExtractor.Tokenize(text);
		
//		for (Word w : words) {
//			if (StringUtils.contains(word_cls, w.getPartOfSpeech())) {
//				context.write(new Text(w.getName()), ONE);
//			}
//		}
	}

	@Override
	protected void setup(Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
//		wordsExtractor.Tokenize("");
	}

}
