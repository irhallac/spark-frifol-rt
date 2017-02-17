import os, time
import math
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark import SparkFiles

mainPath=os.path.dirname(__file__)

sc = SparkContext("local", "Simple App")
sqlContext = SQLContext(sc)
start_time = time.time()

# hdfs path to input data
#people = sqlContext.read.json(os.path.join(mainPath,"userJson","userFrinFollow.json"))
people = sqlContext.read.json(os.path.join(mainPath,"bigfile2.json"))
people.registerTempTable("ff")

#spark query
xy1 = sqlContext.sql("SELECT * FROM ff")
outputPath = os.path.join(mainPath, "output_ff.txt")

#calculating similarty matric
with open(outputPath, "w") as testFile:
 res=map(lambda x:
     map(lambda y:
       str( (len(list(set(x[1]).intersection(y[1]))))/(math.sqrt(abs(len(x[1])))*
       math.sqrt(abs(len(y[1])))) + (len(list(set(x[0]).intersection(y[0]))))/
       (math.sqrt(abs(len(x[0])))*math.sqrt(abs(len(y[0])))) )  if x[2]!=y[2] and len(x[1])*len(x[0])*len(y[1])*len(y[0])!=0 else None,
    xy1.collect()),
 xy1.collect())
 print("time taken: " + str(time.time() - start_time) + "\nNo. of users: " + str(len(xy1.collect())) + "\n")
 for sim1 in res:
    for sim2 in sim1:
       if sim2!= None: testFile.write(str(round(float(sim2),4)) + " ")
    testFile.write("\n")
