package pkg;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* This mapper will split the input line and outputs user as Key and Movie rating information as value*/
public class MakeUserKeyMapper extends Mapper<Object, Text, Text, Text> {
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		System.out.println("Mapper Start");
		String temp[] = value.toString().split("\\t");
		String user;
		StringBuffer sb = new StringBuffer();
		user = temp[0];
		sb.append(temp[1]);
		sb.append(',');
		sb.append(temp[2]);
		context.write(new Text(user), new Text(sb.toString()));
	}
}