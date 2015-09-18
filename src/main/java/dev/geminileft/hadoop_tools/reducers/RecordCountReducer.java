package dev.geminileft.hadoop_tools.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RecordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

	private final static IntWritable outputValue = new IntWritable();
	
	   public void reduce(Text key, Iterable<IntWritable> values,
               Context context
               ) throws IOException, InterruptedException {
		   int tally = 0;
		   for (IntWritable val : values) {
			   tally += val.get();
		   }
		   outputValue.set(tally);
		   context.write(key, outputValue);
	   }
}
