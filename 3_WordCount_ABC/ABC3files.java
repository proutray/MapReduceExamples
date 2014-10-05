/* Author: Piyush Routray p.r@jhu.edu @p_routray
 * Description: Map-reduce code to count the number of words starting with A/a or B/b or C/c.
 * The output for each letter has to be stored in separate files.
 */
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

public class ABC3files {

	public static class PiyushMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String a = "Number of Words with A";
			Text k2a = new Text(a);
			String b = "Number of Words with B";
			Text k2b = new Text(b);
			String c = "Number of Words with C";
			Text k2c = new Text(c);
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				String strword = word.toString();
				Text v2 = new Text(strword);
				strword = strword.toUpperCase();// Make all capital
				char firstChar = strword.charAt(0);// 1stchar of the word
				int charCode = (int) firstChar;// Get ASCII
				if (charCode == 65) {
					context.write(k2a, v2);
				}
				else if (charCode == 66) {
					context.write(k2b, v2);
				}
				else if (charCode == 67) {
					context.write(k2c, v2);
				}
			}
		}
	}

	public static class PiyushReducer extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text vl : values) {
				context.write(null, new Text(vl.toString() + "\n"));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Insufficient args");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "hdfs://localhost:50001");
		Job job = new Job(conf, "three files ABC");

		job.setJarByClass(ABC3files.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(PiyushMapper.class);
		job.setReducerClass(PiyushReducer.class);
		job.setCombinerClass(PiyushReducer.class);
		
		job.setNumReduceTasks(3);//Uses hash partition to store in three files
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}