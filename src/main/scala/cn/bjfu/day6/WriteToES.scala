package cn.bjfu.day6

import java.util

import cn.bjfu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object WriteToES {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //有关es配置的样板代码
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("Worker1",9200))
    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
        httpHosts,new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
              val hashMap = new util.HashMap[String,String]()
              hashMap.put("data",t.toString)
              val indexRequest = Requests.indexRequest()
                .index("sensor")
                .`type`("readingData")
                .source(hashMap)
             requestIndexer.add(indexRequest)
        }
      }
    )
    esSinkBuilder.setBulkFlushMaxActions(2)
    val stream = env.addSource(new SensorSource)
    stream.addSink(esSinkBuilder.build())
    env.execute()
  }
}
