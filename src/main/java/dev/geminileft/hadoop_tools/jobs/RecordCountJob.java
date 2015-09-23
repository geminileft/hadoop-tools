package dev.geminileft.hadoop_tools.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import dev.geminileft.hadoop_core.CustomTextOutputFormat;
import dev.geminileft.hadoop_tools.mappers.RecordCountMapper;
import dev.geminileft.hadoop_tools.reducers.RecordCountReducer;

public class RecordCountJob extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new RecordCountJob(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "RecordCountJob");
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(CustomTextOutputFormat.class);
		job.setJarByClass(RecordCountJob.class);
		
		
		job.setMapperClass(RecordCountMapper.class);
		//defaults to Text, Text...You should probably always define
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(RecordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
