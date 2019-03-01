from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("spark sql").getOrCreate()

    df = spark.sql("select id, value from cupid_wordcount")
    df.printSchema()
    df.show(10, 200)

    df_2 = spark.sql("SELECT product,category,revenue FROM (SELECT product,category,revenue, dense_rank() OVER (PARTITION BY category ORDER BY revenue DESC) as rank  FROM productRevenue) tmp WHERE  rank <= 2");
    df_2.printSchema()
    df_2.show(10, 200)

    df_3 = spark.sql("select id, value from cupid_partition_table1 where pt1 = 'part1'").show(10, 200)

    #Create Drop Table
    spark.sql("create table TestCtas as select * from cupid_wordcount").show
    spark.sql("drop table TestCtas").show