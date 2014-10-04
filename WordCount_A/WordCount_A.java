/*Author: Piyush Routray p.r@jhu.edu @p_routray
 * Description: This program counts the words starting with A or a. 
 * */
package question2;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount_A {

	public static class PiyushMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String a = "Number of Words with A";
			Text k2 = new Text(a);
			int i = 1;
			IntWritable v2 = new IntWritable(i);
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				String strword = word.toString();
				strword = strword.toUpperCase();// Make all capital
				char firstChar = strword.charAt(0);// 1stchar of the word
				int charCode = (int) firstChar;// Get ASCII
				if (charCode == 65) {
					context.write(k2, v2);
				}
			}
		}
	}

	public static class PiyushReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Insufficient args");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "hdfs://localhost:50001");
		Job job = new Job(conf, "Number of Words with A");

		job.setJarByClass(WordCount_A.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(PiyushMapper.class);
		job.setReducerClass(PiyushReducer.class);
		job.setCombinerClass(PiyushReducer.class);
		job.setNumReduceTasks(1);//To get consolidated output
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}