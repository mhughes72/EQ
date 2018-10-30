from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
from pyspark.sql import SQLContext
from pyspark.mllib.stat import Statistics
import geopandas as gpd
from quilt.data.ResidentMario import geoplot_data

# import StatCounter
# import org.apache.spark.sql.functions._
import collections
import numpy as np

conf = SparkConf().setMaster("local[4]").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("SparkSQL").getOrCreate()
sqlContext = SQLContext(sc)



def mapPOID01(fields):
    # fields = line.split(',')
    return Row(POIID=fields[0], LatitudePOI=float(fields[1]), LongitudePOI=float(fields[2]))

def mapCities01(fields):
    # fields = line.split(',')
    return Row(ID=fields[0], TimeSt=fields[1], Country=fields[2], Province = fields[3], City = fields[4], LatitudeCity = float(fields[5]), LongitudeCity = float(fields[6]))


POIData3  = sqlContext.read.option("header", "true").format("csv").load("POIList.csv")
print(hasattr(POIData3, "registerTempTable"))
POIData2 = POIData3.rdd.map(mapPOID01).toDF()
POIData = POIData2.registerTempTable('POIData')

# df1 = "SELECT * from POIData"

CityData3  = sqlContext.read.option("header", "true").format("csv").load("DataSample_tiny.csv")
CityData2 = CityData3.rdd.map(mapCities01).toDF()
CityData = CityData2.registerTempTable('CityData')



# print("SQL")
# df1 = "SELECT * from CityData where 'TimeSt' == 'LongitudeCity'"
# df2 = sqlContext.sql(df1)
# print(df2.show(5))



airVelocityKMPH = [12,13,15,12,11,12,11]
parVelocityKMPH = sc.parallelize(airVelocityKMPH)
varianceValue = parVelocityKMPH.stdev()
print(varianceValue)

# def myFunc6(line):
#     return line
#
#
# myFilter = CityData.rdd.filter(myFunc6)
# print(myFilter.take(5))

query3 = "SELECT c.ID, p.POIID, c.City, c.LatitudeCity, c.LongitudeCity, p.LatitudePOI, p.LongitudePOI, \
(6371000 * acos(cos(radians(c.LatitudeCity)) * cos(radians(p.LatitudePOI)) * cos(radians(p.LongitudePOI) - radians(c.LongitudeCity)) + sin(radians(c.LatitudeCity)) * sin(radians(p.LatitudePOI)))) \
AS distance \
FROM POIData p CROSS JOIN CityData c ORDER BY distance"

df3 = sqlContext.sql(query3).orderBy("ID")
# df3.show(100)
print(type(df3))

ratings = df3.rdd.map(lambda l: ((l[0]), ((l[1]), (l[7]))))



def myFunc(accum, n):
    return accum + n

def getMaxDistance(lines):
    allDist = []
    allDist.append(lines[1][1])
    allDist.append(lines[1][3])
    allDist.append(lines[1][5])
    allDist.append(lines[1][7])
    maxDist = min(allDist)
    if maxDist == lines[1][1]:
        return lines[0], lines[1][0], lines[1][1]
    elif (maxDist == lines[1][3]):
        return lines[0], lines[1][2], lines[1][3]
    elif (maxDist == lines[1][5]):
        return lines[0], lines[1][4], lines[1][5]
    elif (maxDist == lines[1][7]):
        return lines[0], lines[1][6], lines[1][7]


def mapPOID02(accum):
    return Row(ID=accum[0], POIID=accum[1], distance=accum[2])


def mapCities02(accum):
    return Row(City=accum[0], Country=accum[1], ID=accum[2], Province = accum[5])


def myFunc5(accum):
    return (accum[0])


rbk = ratings.reduceByKey(myFunc).map(getMaxDistance)
rbk2 = rbk.map(mapPOID02).toDF()
rdd3 = CityData2.rdd.map(mapCities02).toDF()

print(rdd3.show())

finalJoin = rdd3.join(rbk2, rdd3.ID == rbk2.ID)

# results = spark.sql(
#   "SELECT * FROM people")
# names = results.map(lambda p: p.name)
# Apply functions to results of SQL queries.

# print("TYPE")
# print(finalJoin.select('POIID').show())
#
# print("SQL 1")
# print(finalJoin.show(5))
print ('MYMEAN 2')
print(finalJoin.show(5))
MyData = finalJoin.registerTempTable('MyData')

df1 = "SELECT stddev(c.distance) AS standev, mean(c.distance) AS mean FROM MyData c WHERE c.POIID = 'POI1'"
df2 = "SELECT stddev(c.distance) AS standev, mean(c.distance) AS mean FROM MyData c WHERE c.POIID = 'POI2'"
df3 = "SELECT stddev(c.distance) AS standev, mean(c.distance) AS mean FROM MyData c WHERE c.POIID = 'POI3'"
df4 = "SELECT stddev(c.distance) AS standev, mean(c.distance) AS mean FROM MyData c WHERE c.POIID = 'POI4'"
final1 = sqlContext.sql(df1)
final2 = sqlContext.sql(df2)
final3 = sqlContext.sql(df3)
final4 = sqlContext.sql(df4)
print("SQL 1")
final1.show(5)
print("SQL 2")
final2.show(5)
print("SQL 3")
final3.show(5)
print("SQL 4")
final4.show(5)

c = a.join(final1, final2)

print("C")
print(c)

finalJoin.groupBy("POIID").mean("distance").show(5)
# finalJoin.groupBy("POIID").rdd.map(myFunc5).show(5)

# finalJoin.groupBy("POIID").("distance").variance.show(5)

# finalJoin.groupBy("POIID").mean("distance").show(5)
# st.stdev("distance").show(5)
# np.std(finalJoin("distance"), axis=0).show(5)

# f1.stddev("distance").show(5)
# stddev(col)
print(finalJoin.show())
finalJoin.toPandas().to_csv('EQFinalCities.csv')
