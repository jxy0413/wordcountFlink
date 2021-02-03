package cn.bjfu.proj

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object MyKafkaProducerUtil {
  def main(args: Array[String]): Unit = {
       writeToKafka("testApache")
  }

  def writeToKafka(topic: String): Unit ={
    val props = new Properties()
    props.put("bootstrap.servers", "Master:9092")
    props.put(
      "key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    props.put(
      "value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    val producer = new KafkaProducer[String,String](props)
    val bufferedSource = io.Source.fromFile("C:\\Users\\Administrator\\IdeaProjects\\wordcountFlink\\src\\main\\resources\\apache.log")
    for(line<-bufferedSource.getLines()){
      val record = new ProducerRecord[String,String](topic,line)
      producer.send(record)
    }
    println("插入完成")
    producer.close()
  }
}
