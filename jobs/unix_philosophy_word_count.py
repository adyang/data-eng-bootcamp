import os, sys, re
from pyspark import SparkContext
from utils import get_absolute_path_of


if __name__ == "__main__":
    sc = SparkContext("local", "Unix Word Count")

    words = sc.textFile('data/word_count/unix-philosophy-basics.txt')
    word_counts = words \
                    .flatMap(lambda str : str.split(" ")) \
                    .map(lambda s: re.sub('[^a-zA-Z]+', '', s)) \
                    .filter(lambda s: s) \
                    .map(lambda w: (w, 1)) \
                    .reduceByKey(lambda a,b: a+b) \
                    .sortBy(lambda x : x[1],ascending=False)

    print(word_counts.take(15))

