from pyspark import SparkContext

# Create a Spark context (if you don't have one already)
sc = SparkContext("local", "ShakespeareText")  # Or your appropriate configuration

# Create an RDD from a text file (example)
words_rdd = sc.textFile("shakespeare.txt")

# Now you can use words_rdd
shakespeare_text_rdd = words_rdd.flatMap(lambda line: line.split())

# Collect the results
words = shakespeare_text_rdd.collect()

# Print the first 10 words
print(words[:10])

# Print all the words (be careful with large datasets!)
# print(words)

# Print the number of words
print(len(words))
