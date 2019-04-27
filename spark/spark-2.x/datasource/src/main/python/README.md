# SPARK on cupid PySpark 支持

## 运行命令

> pyspark示例代码如下：

```
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("spark sql").getOrCreate()

    spark.sql("DROP TABLE IF EXISTS spark_sql_test_table")
    spark.sql("CREATE TABLE spark_sql_test_table(name STRING, num BIGINT)")
    spark.sql("INSERT INTO spark_sql_test_table SELECT 'abc', 100000")
    spark.sql("SELECT * FROM spark_sql_test_table").show()
    spark.sql("SELECT COUNT(*) FROM spark_sql_test_table").show()
```


### Local模式运行

> 运行命令如下:

```

spark-submit --master local[4] --driver-class-path cupid/odps-spark-datasourcexxx.jar example.py

PS. local模式运行一定要用driver-class-path，而不能用--jars

```

### Cluster模式运行

```

spark-submit --master yarn-cluster --jars cupid/odps-spark-datasourcexxx.jar example.py


```
