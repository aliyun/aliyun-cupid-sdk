## Sample

```scala
val ssc = new StreamingContext(conf, Durations.seconds(logSetting.getDuration))

val lines = LoghubUtils.createStream(ssc, buildParam(logSetting), logSetting.getNumReceivers)

val messages = lines.map(line => {
  val traceLog = DomainBuilder.buildNlsTraceLogJsonString(line)
  println(traceLog)
  traceLog
})
```

#### Loghub是日志服务的一个组件，日志服务详见[官方文档](https://www.aliyun.com/product/sls)