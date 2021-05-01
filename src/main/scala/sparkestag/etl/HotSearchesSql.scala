package sparkestag.etl

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object HotSearchesSql {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[1]")
      .setAppName("hot searches scala sql")
    val sc = new SparkContext(config)
    val lines = sc.textFile("hdfs://namenode:8020/searchlog.sample.txt")
    val row = lines.map(line => {
      var arr = line.split("\t")
      val str = arr(3) // at index 4 in log.sample.txt: 
                    // [Rank of this URL in search results] and [Click order by user]
      val rank_click = str.split(" ") // above 2 terms are separated by a single space
      Row(rank_click(0).toInt, rank_click(1).toInt) // convert to Int
    })

    // StructType: a collection of StructField's
    // StructField: a collection of column names
    val structType = StructType( 
      StructField("rank", IntegerType, false) ::
        StructField("click", IntegerType, false) :: Nil
    )

    val ss = SparkSession.builder().getOrCreate()
    val df = ss.createDataFrame(row,structType)
    df.createOrReplaceTempView("tb")
    val re = df.sqlContext.sql("SELECT COUNT(if(t.rank=t.click,1,null)) as hit" +
      ", COUNT(1) as total FROM tb as t ")
    re.show()
    val next = re.toLocalIterator().next()
    val hit = next.getAs[Long]("hit")
    val total = next.getAs[Long]("total")
    println("hit rate:" + (hit.toFloat/total.toFloat))

  }

}
