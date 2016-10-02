package session13.assignment1.q1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PetroleumTask1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int totalVolSold = 0;
		for(IntWritable value: values) {
			totalVolSold = totalVolSold + value.get();
		}
		
		context.write(key, new IntWritable(totalVolSold));
	}

}
