/*Author: Piyush Routray p.r@jhu.edu @p_routray
 *Description: The code is used to count the number of lines in a given file.
 *Like the famous word-count program this code can be used for basic understanding of MapReduce.  */
package question1;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NumberOfLines {
	public static class PiyushMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String a = "NumberOfLines";
			Text k2 = new Text(a);
			int i = 1;
			IntWritable v2 = new IntWritable(i);
			context.write(k2, v2);
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
		Job job = new Job(conf, "Number of Lines");

		job.setJarByClass(NumberOfLines.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(PiyushMapper.class);
		job.setReducerClass(PiyushReducer.class);
		job.setCombinerClass(PiyushReducer.class);
		
		job.setNumReduceTasks(1);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
