package com.westar

import org.apache.flink.streaming.api.scala._

object WordCountScala {
  def main(args: Array[String]): Unit = {
    //1、初始化流的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)

    //2、data source
    val dataStreamSource = env.socketTextStream("localhost",5001)

    //3、data Process
    val wordCounts = dataStreamSource
      .flatMap(line => line.split(" "))
      .map( (_,1))
      .keyBy(0)
      .sum(1)

    //4、data Sink
    wordCounts.print()

    env.execute("Streaming WordCount Scala")
  }

}
