"""Distributed word count reading input from S3 (s3a) and writing the result back to S3.

Usage: spark-submit s3_wordcount.py <input_s3a_path> <output_s3a_path>
"""
import sys

from pyspark.sql import SparkSession


def main() -> None:
    input_path, output_path = sys.argv[1], sys.argv[2]

    spark = SparkSession.builder.appName("s3_wordcount").getOrCreate()

    lines = spark.read.text(input_path)
    counts = (
        lines.rdd.flatMap(lambda row: row[0].split(" "))
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}: {kv[1]}")
    )
    counts.coalesce(1).saveAsTextFile(output_path)

    spark.stop()


if __name__ == "__main__":
    main()
