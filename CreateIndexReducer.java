package pkg;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CreateIndexReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		StringBuffer ratingInfo = new StringBuffer();
		int iterationCounter = 0;
		for (Text value : values) {
			iterationCounter++;
			String temp[] = value.toString().split(",");

			if (iterationCounter == 1) {
				ratingInfo.append(temp[0]);
			} else {
				ratingInfo.append(",");
				ratingInfo.append(temp[0]);
			}
			ratingInfo.append(":");
			ratingInfo.append(temp[1]);
		}
		context.write(new Text(key), new Text(ratingInfo.toString()));
	}
}