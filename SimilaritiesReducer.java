package pkg;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilaritiesReducer extends Reducer<Text, Text, Text, Text> {
	DecimalFormat df = new DecimalFormat("#.####");

	// Creating a hashmap to store the list of movie similarities
	static HashMap<String, Double> movieSimilarities = new HashMap<String, Double>();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("Reducer Start");
		ArrayList<Integer> movie1 = new ArrayList<Integer>();
		ArrayList<Integer> movie2 = new ArrayList<Integer>();
		double similarity;

		for (Text value : values) {
			String temp[] = value.toString().split(",");
			movie1.add(Integer.parseInt(temp[0]));
			movie2.add(Integer.parseInt(temp[1]));
		}
		if (context.getConfiguration().get("similarityMethod")
				.equalsIgnoreCase("pearson")) {
			similarity = CalculateSimilarities
					.pearsonSimilarity(movie1, movie2);
		} else {
			similarity = CalculateSimilarities.cosineSimilarity(movie1, movie2);
		}
		movieSimilarities.put(key.toString(),
				Double.parseDouble(df.format(similarity)));
		System.out.println("Reducer End");
	}

	@Override
	public void cleanup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println("In Cleanup");
		/*
		 * We will sort the movie similarity ratings by similarity values and
		 * output only top 100 movies
		 */

		// Creating an Arraylist to store key+value combinations from the
		// HashMap
		ArrayList as = new ArrayList(movieSimilarities.entrySet());
		/*
		 * Creating a comparator object to compare values in the arrayList. This
		 * will return 0 if both arguments are equal, 1 if argument1 is greater
		 * than argument2, -1 if argument1 is less than argument2
		 */
		Comparator comp = new Comparator() {
			@Override
			public int compare(Object o1, Object o2) {
				Map.Entry e1 = (Map.Entry) o1;
				Map.Entry e2 = (Map.Entry) o2;
				Double first = (Double) e1.getValue();
				Double second = (Double) e2.getValue();
				/*
				 * If we return the default return value, the list is sorted in
				 * ascending order. Since we want the list of movies with
				 * highest similarity scores, we might want to sort the list in
				 * descending order. Hence we return the value multiple by -8,
				 * which will result in descending order
				 */
				return (first.compareTo(second) * (-1));
			}
		};

		// Sorting the array list using the above created comparator
		Collections.sort(as, comp);

		// Iterate through the sorted ArrayList and fetch top 100 values
		Iterator it = as.iterator();
		int iterations = 0;
		while (it.hasNext()) {
			// Break the loop after fetching top 100 movie combinations
			if (iterations == 100) {
				break;
			}
			Map.Entry me = (Map.Entry) it.next();
			context.write(new Text(me.getKey().toString()), new Text(me
					.getValue().toString()));
			iterations++;
		}

		FileSystem fs = FileSystem.get(context.getConfiguration());
		fs.delete(new Path("output_intermediate"), true);
		System.out.println("Cleanup End");
	}
}
