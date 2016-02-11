*************************** Item based collaborative filtering ******************************************
1) The program will take three arguments:
	Argument1 = Input folder
	Argument2 = Output folder
	Argument3 = Similarity measure (Cosine or Pearson)
2) A temporary folder named "output_intermediate" is created to store the output of first MapReduce job. 
   The second MapReduce job takes this folder as input. This temporary folder is deleted from HDFC when 
   second MapReduce job is finished. 
3) Depending on the third Argument, Consine or Pearson similarity measure is used. 
4) List top 100 movie combinations with highest similarity measure is written as final output.
5) Below is the list of Mapper and Reducer classes according to their sequence of execution:
		CollaborativeFilteringDriver -- Driver 
		MakeUserKeyMapper  -- Mapper1
		CreateIndexReducer  -- Reducer1
		CoocurredRatingsMapper  -- Mapper2
		SimilaritiesReducer  -- Reducer2
		CalculateSimilarities -- Class with methods to calculate similarity measure
	