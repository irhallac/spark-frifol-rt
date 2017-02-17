import os, time
import math
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark import SparkFiles

mainPath=os.path.dirname(__file__)

sc = SparkContext("local", "Simple App")
sqlContext = SQLContext(sc)
start_time = time.time()

people = sqlContext.read.json(os.path.join(mainPath,"rttest2.json"))
people.registerTempTable("rt")

xy1 = sqlContext.sql("SELECT * FROM rt")
path = os.path.join(mainPath, "output_rt.txt")

def calc(x,y):
 c=len(list(set([row[1] for row in x[0]]).intersection([row[1] for row in y[0]])))
 ri=len(x[0])
 rj=len(y[0])
 nij=[row[0] for row in y[0]][[row[1] for row in y[0]].index(int(x[1]))] if int(x[1]) in [row[1] for row in y[0]] else 0
 nji=[row[0] for row in x[0]][[row[1] for row in x[0]].index(int(y[1]))] if int(y[1]) in [row[1] for row in x[0]] else 0
 sim=0
 if ri!=0 and rj!=0:
  sim= round((c/(math.sqrt(abs(ri))*math.sqrt(abs(rj))))+((nij+nji)/(abs(ri)+abs(rj))),4)
 else: 
  if ri!=0 or rj!=0:
   sim= round((nij+nji)/(abs(ri)+abs(rj)),4)
 return sim;

with open(path, "w") as testFile:
 res=map(lambda x:
   map(lambda y: calc(x,y) if x[1]!=y[1] else None, xy1.collect())
 , xy1.collect())

 print("time taken: " + str(time.time() - start_time) + "\nNo. of users: " + str(len(xy1.collect())) + "\n")
 for sim1 in res:
   for sim2 in sim1:
    if sim2 != None: testFile.write(str(sim2) + " ")
   testFile.write("\n")



