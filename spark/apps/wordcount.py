from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("wordcount").getOrCreate()

text = spark.sparkContext.parallelize([
    "spark is fast",
    "spark is fun",
    "practice makes perfect",
])

counts = (
    text.flatMap(lambda line: line.split(" "))
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)
    .collect()
)

for word, count in sorted(counts):
    print(f"{word}: {count}")

spark.stop()
