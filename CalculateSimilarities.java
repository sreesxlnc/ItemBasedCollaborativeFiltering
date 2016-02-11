package pkg;

import java.util.ArrayList;

public class CalculateSimilarities {
	// This method takes two ArrayLists as input and returns the Pearson
	// similarity between them
	public static double pearsonSimilarity(ArrayList<Integer> movie1,
			ArrayList<Integer> movie2) {
		System.out.println("In pearson similarity calculation");
		double movie1Avg = 0;
		double movie2Avg = 0;
		double numerator = 0;
		double denominator1 = 0;
		double denominator2 = 0;
		double similarity = 0;
		for (int i = 0; i < movie1.size(); i++) {
			System.out.println("Movie1 Rating " + i + ":" + movie1.get(i));
		}
		for (int i = 0; i < movie2.size(); i++) {
			System.out.println("Movie2 Rating " + i + ":" + movie2.get(i));
		}
		// Calculate average of Movie1,Movie2 scores
		for (int i = 0; i < movie1.size(); i++) {
			movie1Avg = movie1Avg + movie1.get(i);
			movie2Avg = movie2Avg + movie2.get(i);
			System.out.println("movie1Avg : " + movie1Avg);
			System.out.println("movie2Avg : " + movie2Avg);
			if (i == (movie1.size() - 1)) {
				movie1Avg = movie1Avg / movie1.size();
				movie2Avg = movie2Avg / movie2.size();
			}
		}

		for (int i = 0; i < movie1.size(); i++) {
			numerator = numerator
					+ ((movie1.get(i) - movie1Avg) * (movie2.get(i) - movie2Avg));
			denominator1 = denominator1
					+ Math.pow((movie1.get(i) - movie1Avg), 2);
			denominator2 = denominator2
					+ Math.pow((movie2.get(i) - movie2Avg), 2);
		}
		// The following IF condition is satisfied when all users give same
		// rating to one of the movie
		if (numerator == 0 || denominator1 == 0 || denominator2 == 0) {
			/*
			 * If all users gave same rating to both movies, then the formula
			 * will dive Divided by zero exception. In that case, if the average
			 * scores of both the movies is same, then we consider movies very
			 * similar and return 1
			 */
			if ((denominator1 == 0 && denominator2 == 0)
					&& (movie1Avg == movie2Avg)) {
				return 1;
			} else {
				return 0;
			}
		} else {
			similarity = (numerator)
					/ (Math.sqrt(denominator1) * Math.sqrt(denominator2));
			return similarity;
		}
	}

	// This method takes two ArrayLists as input and returns the Cosine
	// similarity between them
	public static double cosineSimilarity(ArrayList<Integer> movie1,
			ArrayList<Integer> movie2) {
		System.out.println("In cosine similarity calculation");
		double numerator = 0;
		double denominator1 = 0;
		double denominator2 = 0;
		double similarity = 0;
		for (int i = 0; i < movie1.size(); i++) {
			System.out.println("In second for loop");
			numerator = numerator + ((movie1.get(i)) * (movie2.get(i)));
			denominator1 = denominator1 + Math.pow((movie1.get(i)), 2);
			denominator2 = denominator2 + Math.pow((movie2.get(i)), 2);
		}

		similarity = (numerator)
				/ (Math.sqrt(denominator1) * Math.sqrt(denominator2));
		return similarity;
	}
}