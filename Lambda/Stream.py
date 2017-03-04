# -*- coding: utf-8 -*-
import sys

import sqlite3
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row


import datetime

def saveStream(rdd):
    rdd.saveAsTextFile("HDFS/new/"+datetime.datetime.now().strftime("%H%m%s"))

def saveToDB(rdd):
    dbconn = sqlite3.connect(database="DB/Pruebas.db")
    dbcursor = dbconn.cursor()

    dbcursor.execute("CREATE TABLE IF NOT EXISTS wordcount_rt1(word CHAR(200) PRIMARY KEY, cuenta INT)")

    delta=rdd.collect() #  .map(lambda (V,K)=> Row("Key"=V,"Numero"=V))
    for row in delta:
        old_cuenta=0

        for innerrow in dbcursor.execute("SELECT cuenta  FROM wordcount_rt1 WHERE word='"+row[0]+"'"):
            old_cuenta=innerrow[0]
            work_row=Row(row[0],row[1] + old_cuenta)
            dbcursor.execute("update wordcount_rt1 set cuenta="+str(work_row[1])+" where word='"+work_row[0]+"'")
        if (old_cuenta==0):
            dbcursor.execute("INSERT INTO wordcount_rt1 VALUES (?,?)" ,row)
        dbconn.commit()
#    print "\n\n******************************************\n\n"
#    for row in dbcursor.execute("SELECT * FROM wordcount_rt1"):
#        print row
    dbconn.close()

if __name__ == "__main__":

    #dbconn = sqlite3.connect(database="Pruebas")
    #dbcursor = dbconn.cursor()

    sc = SparkContext(appName="PyStreamNWC", master="local[2]")
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("checkpoint")

    #Conexion a kafka para crear el DSTREAM
    brokers = 'localhost:9092'
    topics = ["pythontest"]

    kvs = KafkaUtils.createDirectStream(ssc,topics,{"metadata.broker.list" : brokers})
    #kafka emite tuplas, el partido ocupa la segunda parte de la tupla
    # Hay que pasar la linea que llega de kafka a JSON
    line = kvs.map(lambda z: z[1]).cache()

    toHDFS = line
    #

    #guardar HDFS
    toHDFS.foreachRDD(saveStream)


    #procesar
    words = line.flatMap(lambda x: x.split(" ")) \
    .map(lambda w: (w,1)) \
    .reduceByKey(lambda a,b: a+b)


    words.pprint()
    words.foreachRDD(saveToDB)


    ssc.start()
    ssc.awaitTermination()
