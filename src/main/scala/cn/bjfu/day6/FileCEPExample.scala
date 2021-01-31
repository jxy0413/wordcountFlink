package cn.bjfu.day6

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time



object FileCEPExample {
  case class LoginEvent(userId:String,eventType:String,ip:String,envenTime:Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.fromElements(
        LoginEvent("user_1","fail","0.0.0.1",1000L),
        LoginEvent("user_1","fail","0.0.0.2",2000L),
//        LoginEvent("user_1","fail","0.0.0.3",3000L),
//        LoginEvent("user_1","fail","0.0.0.1",1000L),
//        LoginEvent("user_1","fail","0.0.0.2",2000L),
//        LoginEvent("user_1","fail","0.0.0.3",3000L),
//        LoginEvent("user_1","fail","0.0.0.1",1000L),
//        LoginEvent("user_1","fail","0.0.0.2",2000L),
//        LoginEvent("user_1","fail","0.0.0.3",3000L),
        LoginEvent("user_2","success","0.0.0.1",3000L)
    )
      .assignAscendingTimestamps(_.envenTime)
      .keyBy(_.userId)

    //声明一个规则 10秒中之内连续登录失败
    val loginFailPattern = Pattern
      .begin[LoginEvent]("first") //第一个事件命名为first
      .where(_.eventType.equals("fail"))  //第一个事件需要满足的条件
      .next("second")
      .where(_.eventType.endsWith("fail"))
      .next("third")
      .where(_.eventType.equals("fail"))
      .within(Time.seconds(10)) //从begin开始的10s

    //第一个参数是带匹配的流，第二个参数是匹配规则
    val patternStream = CEP.pattern(stream,loginFailPattern)

    //使用select方法 将匹配到的流
    val loginFailDataStream = patternStream
      .select((pattern:scala.collection.Map[String, Iterable[LoginEvent]]) => {
        val first = pattern.getOrElse("first", null).iterator.next()
        val second = pattern.getOrElse("second", null).iterator.next()
        val third = pattern.getOrElse("third", null).iterator.next()
        "用户分别在"+first.ip+","+second.ip+","+third.ip+"匹配失败"
      }).print()

     env.execute()

  }
}
