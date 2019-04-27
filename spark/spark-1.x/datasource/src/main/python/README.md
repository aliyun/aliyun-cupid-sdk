# SPARK on cupid PySpark 支持

## 运行命令

> pyspark示例如下：

```
from pyspark import SparkContext, SparkConf
from pyspark.sql import OdpsContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("odps_pyspark")
    sc = SparkContext(conf=conf)
    sql_context = OdpsContext(sc)
    sql_context.sql("DROP TABLE IF EXISTS spark_sql_test_table")
    sql_context.sql("CREATE TABLE spark_sql_test_table(name STRING, num BIGINT)")
    sql_context.sql("INSERT INTO TABLE spark_sql_test_table SELECT 'abc', 100000")
    sql_context.sql("SELECT * FROM spark_sql_test_table").show()
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
