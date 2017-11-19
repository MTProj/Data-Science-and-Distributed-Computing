import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from bisect import bisect_left,bisect_right

import re
import itertools
import timeit


# Global Variables
lemmaList = []
lemmaRDD =   ""
lemmaDictionary = {}

WORDCOOCCURVAL = 0
INPUTDIR = ""
OUTPUTDIR = ""

USAGE = "################################################# \n\n"\
        "Incorrect Usage for WordCoOccurence.py \n " \
        "Usage \n " \
        "wordCoOccurence.py InputFile OutputDir LemmatizerFile NumberOfCoOccurences \n" \
        "Ex. \n" \
        "wordCoOccurence.py /home/text.txt /home/results /home/lemma.csv 2 \n" \
        "wordCoOccurence.py /home/text.txt /home/results /home/lemma.csv 3 \n\n" \
        "NOTICE - This program currently only supports 2 or 3 CoOccurences. \n"\
        "You must specify a new output dir each time or delete your old one. \n"\
        "################################################# \n"\

def cleanWord(x):
    s = ""
    s = x.lower()
    s = re.sub('[^a-z]+','',s)
    return s

def normalizeWord(x):
    s = ""
    s = x.replace('i','j')
    s = x.replace('v','u')
    return s

# This function will clean and normalize the text
# while splitting the line by location
def splitLineByLocation(line):
    s = line.split('>')
    loc = s[0] + '>'
    text = s[1]
    text = normalizeWord(text)
    text = text.split(' ')
    for i in range(len(text)):
        cleanWord(text[i])

    return loc,text

def wordPairsTwo(line):
    a = line[0]
    b = line[1]

    pairs = itertools.combinations(b,2)
    words = [(pair,a) for pair in pairs]

    return words

def wordPairsThree(line):
    a = line[0]
    b = line[1]

    pairs = itertools.combinations(b,3)
    words = [(pair, a) for pair in pairs]

    return words

def formatLemmaFile(x):
    s = x.split(',')
    lemmaList = []
    for i in s:
        if i != "":
            lemmaList.append(i)
    return lemmaList

def buildLemmaDictionary(x):
    d = {}

    for i in x:
        values = i
        for v in values:
            d[v] = values
    return d

def checkLemmaForMatch(word):
    key = word
    if key in lemmaDictionary:
        # Return Lemma Values
        return lemmaDictionary[key]
    else:
        return ""



if __name__ == "__main__":
    # Start SparkSession
    spark = SparkContext("local","Word CoOccurence")

    INPUTDIR = sys.argv[1]
    OUTPUTDIR = sys.argv[2]
    LEMMADIR = sys.argv[3]
    WORDCOOCCURVAL = sys.argv[4]


    try:
        # Set Global Vars
        files = spark.wholeTextFiles(INPUTDIR)
        lemmaRDD = spark.textFile(LEMMADIR)
        WORDCOOCCURVAL = int(WORDCOOCCURVAL)

        #Map Lemma and Files
        lines = files.map(lambda x: splitLineByLocation(x[1]))
        lemmaRDD.map(lambda r:r[0])


        # Format Lemma File for Usability
        lemmaRDD = lemmaRDD.map(formatLemmaFile)
        lemmaList = lemmaRDD.collect()
        lemmaDictionary = buildLemmaDictionary(lemmaList)

        # Start time for actualy Map and Reduce Process
        startTimer = timeit.default_timer()

        if WORDCOOCCURVAL == 2:
            ########################
            # Start Map and Reduce #
            ########################
            locLineMapRDD = lines.map(wordPairsTwo) \
                .flatMap(lambda x: x) \
                .reduceByKey(lambda x, y: x + y) \
                .sortByKey()\
                .saveAsTextFile(OUTPUTDIR)
            #######################
            # Stop Map and Reduce #
            #######################

        elif WORDCOOCCURVAL == 3:
            ########################
            # Start Map and Reduce #
            ########################
            locLineMapRDD = lines.map(wordPairsThree) \
                .flatMap(lambda x: x) \
                .reduceByKey(lambda x, y: x + y) \
                .sortByKey() \
                .saveAsTextFile(OUTPUTDIR)
            #######################
            # Stop Map and Reduce #
            #######################
        else:
            print(USAGE)
    except ValueError,FileAlreadyExistsException:
        print(USAGE)

    stopTimer = timeit.default_timer()
    print("######################################")
    print("############## RUN TIME ##############")
    print(stopTimer - startTimer)
    print("######################################")
    spark.stop()

