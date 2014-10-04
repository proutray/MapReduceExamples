/*Author: Piyush Routray p.r@jhu.edu @p_routray
 *Description: Implementation of Raw Comparator to arrange values in descending order.
 *This code was written for an Assignment of an online course I took.
 *Q: Display the Name, Job title and salary from the employee.csv file for 
 *people with salary more than 18000. Arrange the list in descending order of salary.
 *"employee.csv" contains: Name, Employee_id, JobTitle, Salary  */


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

public class SalaryOrder {
	static int a = 0;

	public static class MSalary extends
			Mapper<LongWritable, Text, IntWritable, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if (a == 1) {// To skip the 1st line of the csv file.
				String[] elements = line.split(",");
				int sal = Integer.parseInt(elements[3]);
				if (sal > 18000) {
					Text v2 = new Text(elements[0] + " " + elements[2]);
					IntWritable k2 = new IntWritable(sal);
					context.write(k2, v2);
				}
			}
			a = 1;// This will change only once even if multiple mappers arr there
		}
	}

	public static class RSalary extends Reducer<IntWritable, Text, Text, Text> {
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			String ky = String.valueOf(key.get());
			for (Text vl : values) {
				context.write(null, new Text(vl.toString() + " " + ky));
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
		Job job = new Job(conf, "Salary Comparision");
		job.setJarByClass(SalaryOrder.class);
		job.setSortComparatorClass(IndexPairComparator.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MSalary.class);
		job.setReducerClass(RSalary.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
