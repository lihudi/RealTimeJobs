
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row
import json

def updateFunc (new_values, last_sum):
    return sum(new_values) + (last_sum or 0)

def porcentajePorProducto(rdd):
    if rdd.count():
        total = float(rdd.map(lambda x: x[1]).reduce(lambda a, b: a+ b))
        return rdd.map(lambda x: (x[0],x[1]/total))
    return rdd.map(lambda x: (x[0],0))


def productosPorZonas(producto):
    lat = producto['location']['lat']
    if (lat < 38):
        return ("Zona-1", 1)
    elif lat < 40:
        return ("Zona-2", 1)
    elif lat < 42:
        return ("Zona-3", 1)
    else:
        return ("Zona-4", 1)


if __name__ == "__main__":
    sc = SparkContext(appName="PyStreamNWC", master="local[2]")
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("checkpoint")

    #Conexion a kafka para crear el DSTREAM
    brokers = '192.168.1.111:9092'
    topics = ["ZaramazonProduct"]

    kvs = KafkaUtils.createDirectStream(ssc,topics,{"metadata.broker.list" : brokers})
    #kafka emite tuplas, el partido ocupa la segunda parte de la tupla
    # Hay que pasar la linea que llega de kafka a JSON
    json_objects = kvs.map(lambda z: json.loads(z[1])).cache()
    product_tuples = json_objects.map(lambda x: (x['item_type'], 1)).cache()

    #Datos historicos: producto strong
    historico = product_tuples.updateStateByKey(updateFunc)\
        .transform(lambda x: x.sortBy(lambda (k, v): -v)).cache()
    #historico.pprint()

    # Datos de ventana: producto hot (10 seg)
    hot = product_tuples.reduceByKey(lambda x, y: x + y) \
        .transform(lambda x: x.sortBy(lambda (k, v): -v))
    #hot.pprint()

    #Datos historicos: proporcion de ventas
    porcentaje_historico = product_tuples.updateStateByKey(updateFunc).transform(porcentajePorProducto)
    #porcentaje_historico.pprint()

    # Datos de ventana: proporcion de ventas en la ultima hora
    porcentaje_hora = product_tuples.reduceByKeyAndWindow(lambda x, y: x + y, 10, 3600) \
        .transform(porcentajePorProducto)
    #porcentaje_hora.pprint()

    #Datos geograficos
    productos_zona = json_objects.map(productosPorZonas).reduceByKeyAndWindow(lambda x, y: x + y, 10, 300) \
        .transform(lambda x: x.sortBy(lambda (k, v): -v))
    productos_zona.pprint()

    ssc.start()
    ssc.awaitTermination()