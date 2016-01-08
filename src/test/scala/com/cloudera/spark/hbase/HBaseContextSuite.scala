package com.cloudera.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseTestingUtility, TableName}
import org.apache.spark._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}


class HBaseContextSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll { // with LocalSparkContext {

  var htu: HBaseTestingUtility = null

  val tableName = "t1"
  val columnFamily = "c"

  var sc:SparkContext = null

  override def beforeAll() {
    htu = HBaseTestingUtility.createLocalHTU()
    
    htu.cleanupTestDir()
    println("starting minicluster")
    htu.startMiniZKCluster()
    htu.startMiniHBaseCluster(1, 1)
    println(" - minicluster started")
    try {
      htu.deleteTable(Bytes.toBytes(tableName))
    } catch {
      case e: Exception =>
        println(" - no table " + tableName + " found")
    }
    println(" - creating table " + tableName)
    htu.createTable(Bytes.toBytes(tableName), Bytes.toBytes(columnFamily))
    println(" - created table")

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.broadcast.compress", "false")
    sc = new SparkContext("local", "test", sparkConfig)
  }

  override def afterAll() {
    htu.deleteTable(Bytes.toBytes(tableName))
    println("shuting down minicluster")
    htu.shutdownMiniHBaseCluster()
    htu.shutdownMiniZKCluster()
    println(" - minicluster shut down")
    htu.cleanupTestDir()

    sc.stop()
  }

  test("bulkput to test HBase client") {
    val config = htu.getConfiguration

    val rdd = sc.parallelize(Array(
      (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo1")))),
      (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("foo2")))),
      (Bytes.toBytes("3"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("c"), Bytes.toBytes("foo3")))),
      (Bytes.toBytes("4"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("d"), Bytes.toBytes("foo")))),
      (Bytes.toBytes("5"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("e"), Bytes.toBytes("bar"))))))

    val hbaseContext = new HBaseContext(sc, config)

    hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](rdd,
      tableName,
      (putRecord) => {

        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2, putValue._3))
        put

      },
      autoFlush = true)

    val connection = ConnectionFactory.createConnection(config)
    val table = connection.getTable(TableName.valueOf("t1"))

    assert(Bytes.toString(
      CellUtil.cloneValue(
        table.get(
          new Get(Bytes.toBytes("1")))
          .getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("a"))))
      .equals("foo1"))

    assert(Bytes.toString(
      CellUtil.cloneValue(
        table.get(
          new Get(Bytes.toBytes("2")))
          .getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("b"))))
      .equals("foo2"))

    assert(Bytes.toString(
      CellUtil.cloneValue(
        table.get(
          new Get(Bytes.toBytes("3")))
          .getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("c"))))
      .equals("foo3"))

    assert(Bytes.toString(
      CellUtil.cloneValue(
        table.get(
          new Get(Bytes.toBytes("4")))
          .getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("d"))))
      .equals("foo"))

    assert(Bytes.toString(
      CellUtil.cloneValue(
        table.get(
          new Get(Bytes.toBytes("5")))
          .getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("e"))))
      .equals("bar"))

  }

  test("bulkput to test HBase client fs storage of Config") {
    val config = htu.getConfiguration

    val rdd = sc.parallelize(Array(
      (Bytes.toBytes("1x"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo1")))),
      (Bytes.toBytes("2x"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("foo2")))),
      (Bytes.toBytes("3x"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("c"), Bytes.toBytes("foo3")))),
      (Bytes.toBytes("4x"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("d"), Bytes.toBytes("foo")))),
      (Bytes.toBytes("5x"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("e"), Bytes.toBytes("bar"))))))

    val tmpPath = "src/test/resources/HBaseConfig"

    val fs = FileSystem.newInstance(new Configuration())
    if (fs.exists(new Path(tmpPath))) {
      fs.delete(new Path(tmpPath), false)
    }

    val hbaseContext = new HBaseContext(sc, config, tmpPath)

    hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](rdd,
      tableName,
      (putRecord) => {

        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2, putValue._3))
        put

      },
      autoFlush = true)

    val connection = ConnectionFactory.createConnection(config)
    val table = connection.getTable(TableName.valueOf("t1"))

    assert(Bytes.toString(
      CellUtil.cloneValue(
        table.get(
          new Get(Bytes.toBytes("1x")))
          .getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("a"))))
      .equals("foo1"))

    assert(Bytes.toString(
      CellUtil.cloneValue(
        table.get(
          new Get(Bytes.toBytes("2x")))
          .getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("b"))))
      .equals("foo2"))

    assert(Bytes.toString(
      CellUtil.cloneValue(
        table.get(
          new Get(Bytes.toBytes("3x")))
          .getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("c"))))
      .equals("foo3"))

    assert(Bytes.toString(
            CellUtil.cloneValue(
              table.get(
                new Get(Bytes.toBytes("4x")))
                .getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("d"))))
          .equals("foo"))

    assert(Bytes.toString(
      CellUtil.cloneValue(
        table.get(
          new Get(Bytes.toBytes("5x")))
          .getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("e"))))
      .equals("bar"))
  }

  test("bulkIncrement to test HBase client") {
    val config = htu.getConfiguration

    val rdd = sc.parallelize(Array(
      (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("counter"), 1L))),
      (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("counter"), 2L))),
      (Bytes.toBytes("3"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("counter"), 3L))),
      (Bytes.toBytes("4"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("counter"), 4L))),
      (Bytes.toBytes("5"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("counter"), 5L)))))

    val hbaseContext = new HBaseContext(sc, config)

    hbaseContext.bulkIncrement[(Array[Byte], Array[(Array[Byte], Array[Byte], Long)])](rdd,
      tableName,
      (incrementRecord) => {
        val increment = new Increment(incrementRecord._1)
        incrementRecord._2.foreach((incrementValue) =>
          increment.addColumn(incrementValue._1, incrementValue._2, incrementValue._3))
        increment
      },
      4)

    hbaseContext.bulkIncrement[(Array[Byte], Array[(Array[Byte], Array[Byte], Long)])](rdd,
      tableName,
      (incrementRecord) => {
        val increment = new Increment(incrementRecord._1)
        incrementRecord._2.foreach((incrementValue) =>
          increment.addColumn(incrementValue._1, incrementValue._2, incrementValue._3))
        increment
      },
      4)

    val connection = ConnectionFactory.createConnection(config)
    val table = connection.getTable(TableName.valueOf("t1"))

    assert(Bytes.toLong(
      CellUtil.cloneValue(
        table.get(
          new Get(Bytes.toBytes("1")))
          .getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("counter"))))
      .equals(2L))

    assert(Bytes.toLong(
      CellUtil.cloneValue(
        table.get(
          new Get(Bytes.toBytes("2")))
          .getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("counter"))))
      .equals(4L))

    assert(Bytes.toLong(
      CellUtil.cloneValue(
        table.get(
          new Get(Bytes.toBytes("3")))
          .getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("counter"))))
      .equals(6L))

    assert(Bytes.toLong(
      CellUtil.cloneValue(
        table.get(
          new Get(Bytes.toBytes("4")))
          .getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("counter"))))
      .equals(8L))

    assert(Bytes.toLong(
      CellUtil.cloneValue(
        table.get(
          new Get(Bytes.toBytes("5")))
          .getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("counter"))))
      .equals(10L))

  }

  test("bulkDelete to test HBase client") {
    val config = htu.getConfiguration

    val connection = ConnectionFactory.createConnection(config)
    val table = connection.getTable(TableName.valueOf("t1"))

    var put = new Put(Bytes.toBytes("delete1"))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo1"))
    table.put(put)
    put = new Put(Bytes.toBytes("delete2"))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo2"))
    table.put(put)
    put = new Put(Bytes.toBytes("delete3"))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo3"))
    table.put(put)

    val rdd = sc.parallelize(Array(
      Bytes.toBytes("delete1"),
      Bytes.toBytes("delete3")))

    val hbaseContext = new HBaseContext(sc, config)

    hbaseContext.bulkDelete[Array[Byte]](rdd,
      tableName,
      putRecord => new Delete(putRecord),
      4)

    assert(table.get(new Get(Bytes.toBytes("delete1"))).
      getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("a")) == null)
    assert(table.get(new Get(Bytes.toBytes("delete3"))).
      getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("a")) == null)

    assert(Bytes.toString(
      CellUtil.cloneValue(
        table.get(
          new Get(Bytes.toBytes("delete2")))
          .getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("a"))))
      .equals("foo2"))
  }

  test("bulkGet to test HBase client") {
    val config = htu.getConfiguration

    config.set("spark.broadcast.compress", "false")

    val connection = ConnectionFactory.createConnection(config)
    val table = connection.getTable(TableName.valueOf("t1"))

    var put = new Put(Bytes.toBytes("get1"))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo1"))
    table.put(put)
    put = new Put(Bytes.toBytes("get2"))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo2"))
    table.put(put)
    put = new Put(Bytes.toBytes("get3"))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo3"))
    table.put(put)

    val rdd = sc.parallelize(Array(
      Bytes.toBytes("get1"),
      Bytes.toBytes("get2"),
      Bytes.toBytes("get3"),
      Bytes.toBytes("get4")))

    val hbaseContext = new HBaseContext(sc, config)

    val getRdd = hbaseContext.bulkGet[Array[Byte], Object](
      tableName,
      2,
      rdd,
      record => {
        new Get(record)
      },
      (result: Result) => {
        if (result.listCells() != null) {
          val it = result.listCells().iterator()
          val B = new StringBuilder

          B.append(Bytes.toString(result.getRow) + ":")

          while (it.hasNext) {
            val kv = it.next()
            val q = Bytes.toString(CellUtil.cloneQualifier(kv))
            if (q.equals("counter")) {
              B.append("(")
                .append(Bytes.toString(CellUtil.cloneQualifier(kv)))
                .append(",")
                .append(Bytes.toLong(CellUtil.cloneValue(kv)))
                .append(")")
            } else {
              B.append("(")
              .append(Bytes.toString(CellUtil.cloneQualifier(kv)))
              .append(",")
              .append(Bytes.toString(CellUtil.cloneValue(kv)))
              .append(")")
            }
          }
          "" + B.toString
        } else {
          ""
        }
      })


    val getArray = getRdd.collect()

    getArray.foreach(f => println(f))

    assert(getArray.length == 4)
    assert(getArray.contains("get1:(a,foo1)"))
    assert(getArray.contains("get2:(a,foo2)"))
    assert(getArray.contains("get3:(a,foo3)"))

  }

  test("distributedScan to test HBase client") {
    val config = htu.getConfiguration

    config.set("spark.broadcast.compress", "false")

    val connection = ConnectionFactory.createConnection(config)
    val table = connection.getTable(TableName.valueOf("t1"))

    var put = new Put(Bytes.toBytes("scan1"))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo1"))
    table.put(put)
    put = new Put(Bytes.toBytes("scan2"))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo2"))
    table.put(put)
    put = new Put(Bytes.toBytes("scan3"))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo3"))
    table.put(put)
    put = new Put(Bytes.toBytes("scan4"))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo3"))
    table.put(put)
    put = new Put(Bytes.toBytes("scan5"))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo3"))
    table.put(put)

    val scan = new Scan()
    scan.setCaching(100)
    scan.setStartRow(Bytes.toBytes("scan2"))
    scan.setStopRow(Bytes.toBytes("scan4_"))

    val hbaseContext = new HBaseContext(sc, config)

    val scanRdd = hbaseContext.hbaseRDD(tableName, scan)
    
    val scanList = scanRdd.collect()
    
    //assert(scanList.length == 3)
    
  }

}