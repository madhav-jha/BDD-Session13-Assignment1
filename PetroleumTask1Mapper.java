package session13.assignment1.q1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PetroleumTask1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] lineArray = value.toString().split(",");

			Text distributor = new Text(lineArray[0]);
			IntWritable volSold = new IntWritable(Integer.parseInt(lineArray[5]));

			context.write(distributor, volSold);

		

	}

}
