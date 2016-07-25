spark-parquet-example
=====================

Compares performance of parquet libraries (read/write speed and disk usage).


Original readme
===============

This is a simple fork from https://github.com/adobe-research/spark-parquet-thrift-example as described
on http://zenfractal.com/2013/08/21/a-powerful-big-data-trio/

It contains the following updates:

* Updated to Spark 1.3.1, Avro 1.7.7
* Registers Avro serialization inside Spark's Kryo serialization as provided by Twitter's chill library

How to run
==========

Example project to show how to use Spark to read and write Avro/Parquet files

To run this example, you will need to have Maven installed. Once installed,
you can launch the example by cloning this repo and running,

    $ mvn scala:run -DmainClass=com.zenfractal.SparkParquetExample
