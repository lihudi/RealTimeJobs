
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row
import json
import sqlite3
from DatabaseManager import DatabaseManager

#JSON de entrada
#[ {"timestamp":"2017-03-03T16:59:01.771Z",
# "product_id":2,
# "prescription":
#   {"sph":-14,
#   "cyl":9,
#   "axis":88,
#   "Add":1},
# "design":[
#   {"name":"FreeStyle","type":"D"}
#   ],
# "lab":[
#   {"name":"LOA",
#   "country":"Spain"}
# ]
# }
# ]


#Pasa los disegnos de nombre-tipo a una cadena 'nombre_tipo' para poder operar con ellos
def designsAsStrings(dict):
    return dict['name'] + "_" + dict['type']

#Acumulaciones
def updateFunc (new_values, last_sum):
    return sum(new_values) + (last_sum or 0)

#Obtiene el porcentaje de cada producto sobre el total de la tupla
#def porcentajePorProducto(rdd):
#    if rdd.count():
#        total = float(rdd.map(lambda x: x[1]).reduce(lambda a, b: a+ b))
#        return rdd.map(lambda x: (x[0],x[1]/total))
#    return rdd.map(lambda x: (x[0],0))


def saveStream(rdd):
    rdd.saveAsTextFile("HDFS/new/"+datetime.datetime.now().strftime("%H%m%s"))

#######################################
##########Guarda tablas en la BD
#######################################

def createItemCountTable(tableName):
    dbconn = sqlite3.connect(database="DB/LentesDB.db")
    dbcursor = dbconn.cursor()
    dbcursor.execute("CREATE TABLE IF NOT EXISTS " + tableName + " (item CHAR(200) PRIMARY KEY, cuenta INT)")
    dbconn.commit()
    dbconn.close()


def updateTableWithRegister(table, item, cuenta):
    dbconn = sqlite3.connect(database="DB/LentesDB.db")
    dbcursor = dbconn.cursor()
    #Comprueba si el producto existe: si no existe lo crea
    for count in dbcursor.execute("select COUNT(*) from "+table+" where item='"+item+"'"):
        if float(count[0] != 0):
            dbcursor.execute("update  "+table+"  set cuenta="+str(cuenta)+" where item='"+item+"'")
        else:
            query = "INSERT INTO  "+table+"  values('"+item+"',"+str(cuenta)+")"
            #print query
            dbcursor.execute(query)
    dbconn.commit()
    dbconn.close()

#llega un array de tuplas
def saveHistoricoProductosToDB(rdd):
    if rdd.count():
        rdd.foreach(lambda x: updateTableWithRegister("historico_productos_rt1", str(x[0]), str(x[1])))



if __name__ == "__main__":
    windowLength = 10

    sc = SparkContext(appName="PyStreamNWC", master="local[2]")
    ssc = StreamingContext(sc, windowLength)
    ssc.checkpoint("checkpoint")

    #Conexion DB: creacion de tablas
    createItemCountTable("historico_productos_rt1")

    #Conexion a kafka para crear el DSTREAM
    brokers = '192.168.1.111:9092'
    topics = ["LensJobs"]
    kvs = KafkaUtils.createDirectStream(ssc,topics,{"metadata.broker.list" : brokers})

    #kafka emite tuplas, el partido ocupa la segunda parte de la tupla
    # Hay que pasar la linea que llega de kafka a JSON
    json_objects = kvs.map(lambda z: json.loads(z[1])).cache()
    #json_objects.pprint()

    ###############################
    ######### Total de lentes por cada windowLength (10)
    #################################
    #Total de disegnos
    designs = json_objects.map(lambda x: x['design'][0]).cache()
    #designs.pprint()
    #designCount = designs.map(lambda x: (designsAsStrings(x), 1))\
    #    .reduceByKey(lambda a, b: a + b)\
    #    .transform(lambda x: x.sortBy(lambda (k, v): -v))
    #designCount.pprint()
    # Datos historicos de productos por familia: producto strong
    #historicoDisegnos = designs.map(lambda x: (designsAsStrings(x), 1))\
    #    .updateStateByKey(updateFunc) \
    #    .transform(lambda x: x.sortBy(lambda (k, v): -v)).cache()
    #historicoDisegnos.pprint()

    #Total de productos por familia
    products = designs.map(lambda x: (x['name'], 1)).cache()
    #products.pprint()
    #productCount = products.reduceByKey(lambda a, b: a + b)\
    #    .transform(lambda x: x.sortBy(lambda (k, v): -v)).cache()
    #productCount.pprint()
    #Datos historicos de productos por familia: producto strong
    historicoProductos = products.updateStateByKey(updateFunc)\
        .transform(lambda x: x.sortBy(lambda (k, v): -v)).cache()
    historicoProductos.pprint()
    historicoProductos.foreachRDD(saveHistoricoProductosToDB)


    ssc.start()
    ssc.awaitTermination()