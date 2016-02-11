package pkg;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CoocurredRatingsMapper extends Mapper<Object, Text, Text, Text> {
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		System.out.println("Mapper start");
		// Split the line to separate user name and ratings information
		String temp[] = value.toString().split("\\t");
		String ratings[] = temp[1].split(",");
		for (int i = 0; i < ratings.length; i++) {
			for (int j = i + 1; j < ratings.length; j++) {
				String temp1[] = ratings[i].split(":");
				String temp2[] = ratings[j].split(":");
				StringBuffer movieNames = new StringBuffer();
				StringBuffer movieRatings = new StringBuffer();
				// Compare both the movies and store the names in the
				// alphabetical / ascending order
				if (temp1[0].compareToIgnoreCase(temp2[0]) > 0) {
					movieNames.append(temp2[0]);
					movieNames.append(",");
					movieNames.append(temp1[0]);
					movieRatings.append(temp2[1]);
					movieRatings.append(",");
					movieRatings.append(temp1[1]);

				} else if (temp1[0].compareToIgnoreCase(temp2[0]) < 0) {
					movieNames.append(temp1[0]);
					movieNames.append(",");
					movieNames.append(temp2[0]);
					movieRatings.append(temp1[1]);
					movieRatings.append(",");
					movieRatings.append(temp2[1]);
				}

				context.write(new Text(movieNames.toString()), new Text(
						movieRatings.toString()));
			}
		}
		System.out.println("Mapper End");
	}
}