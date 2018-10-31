from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
from pyspark.sql import SQLContext
from pyspark.mllib.stat import Statistics

import collections
import numpy as np

def mapPOID01(fields):
    return Row(POIID=fields[0], LatitudePOI=float(fields[1]), LongitudePOI=float(fields[2]))

def mapCities01(fields):
    return Row(ID=fields[0], TimeSt=fields[1], Country=fields[2], Province = fields[3], City = fields[4], LatitudeCity = float(fields[5]), LongitudeCity = float(fields[6]))

def mapPOID02(accum):
    return Row(ID=accum[0], POIID=accum[1], distance=accum[2])

def mapCities02(accum):
    return Row(City=accum[0], Country=accum[1], ID_remove=accum[2], Province = accum[5])

def reduceByPOID(a, b):
    return a + b

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

conf = SparkConf().setMaster("local[4]").setAppName("distanceWithPOIDHistogram")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("SparkSQL").getOrCreate()
sqlContext = SQLContext(sc)

POIData3  = sqlContext.read.option("header", "true").format("csv").load("POIList.csv")
POIData2 = POIData3.rdd.map(mapPOID01).toDF()
POIData = POIData2.registerTempTable('POIData')

CityData3  = sqlContext.read.option("header", "true").format("csv").load("DataSample.csv")
CityData2 = CityData3.rdd.map(mapCities01).toDF()
CityData = CityData2.registerTempTable('CityData')

#Using SQL to calculate the distance between Latitudes and Longitudes: ‘haversine’ formula
sqlCalcDist = "SELECT c.ID, p.POIID, c.City, c.LatitudeCity, c.LongitudeCity, p.LatitudePOI, p.LongitudePOI, \
(6371000 * acos(cos(radians(c.LatitudeCity)) * cos(radians(p.LatitudePOI)) * cos(radians(p.LongitudePOI) - radians(c.LongitudeCity)) + sin(radians(c.LatitudeCity)) * sin(radians(p.LatitudePOI)))) \
AS distance \
FROM POIData p CROSS JOIN CityData c ORDER BY distance"

dfWithDistance = sqlContext.sql(sqlCalcDist).orderBy("ID")
distanceWithPOID = dfWithDistance.rdd.map(lambda l: ((l[0]), ((l[1]), (l[7]))))

onlyMaxDist = distanceWithPOID.reduceByKey(reduceByPOID).map(getMaxDistance)
dfPOID = onlyMaxDist.map(mapPOID02).toDF()
dfCities = CityData2.rdd.map(mapCities02).toDF()

finalJoin = dfCities.join(dfPOID, dfCities.ID_remove == dfPOID.ID).drop('ID_remove')

MyData = finalJoin.registerTempTable('MyData')

sqlMeanStd = "SELECT stddev(c.distance) AS standev, mean(c.distance) AS mean FROM MyData c WHERE c.POIID = 'POI1' \
        UNION ALL \
        SELECT stddev(c.distance) AS standev, mean(c.distance) AS mean FROM MyData c WHERE c.POIID = 'POI2' \
        UNION ALL \
        SELECT stddev(c.distance) AS standev, mean(c.distance) AS mean FROM MyData c WHERE c.POIID = 'POI3' \
        UNION ALL \
        SELECT stddev(c.distance) AS standev, mean(c.distance) AS mean FROM MyData c WHERE c.POIID = 'POI4'"

final1 = sqlContext.sql(sqlMeanStd)
finalJoin.groupBy("POIID").mean("distance").show(5)

print(finalJoin.show())
final1.toPandas().to_csv('EQFinalPOID.csv')
finalJoin.toPandas().to_csv('EQFinalCities.csv')
