package dev.geminileft.hadoop_tools.mappers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RecordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	public final static String RECORD_COUNT_CONFIG_KEY = "RecordCountMapper.key";
	public final static String DEFAULT_KEY = "RECORD";
	private final static Text OUTPUT_KEY = new Text(DEFAULT_KEY);
	private final static IntWritable ONE_VALUE = new IntWritable(1);

	@Override
	protected void setup(Context context) {
		//boiler plate
		Configuration config = context.getConfiguration();
		String temp;
		
		temp = config.get(RECORD_COUNT_CONFIG_KEY);
		if (temp != null) {
			OUTPUT_KEY.set(temp);
			//do something here
		}
		
	}

	public void map(Object key, Text value, Context context) {
		try {
			context.write(OUTPUT_KEY, ONE_VALUE);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
