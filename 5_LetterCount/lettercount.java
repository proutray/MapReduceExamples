/*
 * Author: Piyush Routray p.r@jhu.edu @p_routray
 * Description: Counts the number of letters in each word of a file.
 * Finally displays as (number of words):(number of letters in the word)
 * eg. If there are five 4-lettered words, the output will be 5:4
 */
package question4;

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

public class lettercount {

	public static class PiyushMapper extends
			Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			int i = 1;
			IntWritable v2 = new IntWritable(i);
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				String strword = word.toString();
				IntWritable k2 = new IntWritable(strword.length());
				context.write(k2, v2);
			}
		}
	}

	public static class PiyushReducer extends
			Reducer<IntWritable, IntWritable, Text, String> {

		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(null, new IntWritable(sum)+":"+key);
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Insufficient args");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "hdfs://localhost:50001");
		Job job = new Job(conf, "letter count");

		job.setJarByClass(lettercount.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(String.class);
		job.setMapperClass(PiyushMapper.class);
		job.setReducerClass(PiyushReducer.class);
		job.setCombinerClass(PiyushReducer.class);
		job.setNumReduceTasks(1);// To get consolidated output
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}