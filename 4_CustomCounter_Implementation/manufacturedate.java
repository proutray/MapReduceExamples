/*
 * Author: Piyush Routray. p.r@jhu.edu @p_routray
 * Description: MapReduce program to display the items produced in 2010 and 2012 in separate files.
 * Use the concept of custom counter in your program.
 * Sample Input file:
Product	place	year	sales
Iphone5	California	2010	125484521
Nokia-l	Finland	2010	155442
samsung4	s.korea	2010	122321321
iphone4s	california	2012	21322134 
 */
package question3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class manufacturedate {
	static int a = 0;
	static Text v2 = new Text("test");
	static Text k2 = new Text("test");

	public static enum CUSTOMCOUNTER {
		ERROR_COUNT
	}

	public static class MPhone extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if (a == 1) {// To skip the 1st line of the file.
				StringTokenizer tokenizer = new StringTokenizer(line);
				while (tokenizer.hasMoreTokens()) {
					context.getCounter(CUSTOMCOUNTER.ERROR_COUNT).increment(1);
					word.set(tokenizer.nextToken());

					if (context.getCounter(CUSTOMCOUNTER.ERROR_COUNT)
							.getValue() == 1) {
						String strword = word.toString();
						v2 = new Text(strword);
					}
					if (context.getCounter(CUSTOMCOUNTER.ERROR_COUNT)
							.getValue() == 3) {
						int yr = Integer.parseInt(word.toString());
						if (yr == 2012) {
							String a = "ManufacturedIn2012";
							k2 = new Text(a);
							// context.write(k2, v2);
						} else if (yr == 2010) {
							String a = "ManufacturedIn2010";
							k2 = new Text(a);
							// context.write(k2, v2);
						}
						context.write(k2, v2);
					}
					if (context.getCounter(CUSTOMCOUNTER.ERROR_COUNT)
							.getValue() == 4) {
						context.getCounter(CUSTOMCOUNTER.ERROR_COUNT).setValue(
								0);
					}

				}
			}
			a = 1;// This will change only once even if multiple mappers r there
		}
	}

	public static class RPhone extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text vl : values) {
				context.write(key, vl);
			}
		}
	}

	public static class PhonePartitioner<K, V> extends Partitioner<K, V> {
		public int getPartition(K key, V value, int numReduceTasks) {
			if (key.toString().equalsIgnoreCase("ManufacturedIn2010"))
				return 0;
			else
				return 1;
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Insufficient args");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "hdfs://localhost:50001");
		Job job = new Job(conf, "Salary Comparision");
		job.setJarByClass(manufacturedate.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MPhone.class);
		job.setReducerClass(RPhone.class);
		job.setPartitionerClass(PhonePartitioner.class);
		job.setNumReduceTasks(2);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
