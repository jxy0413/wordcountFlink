package cn.bjfu.day1

//导入隐式类型转换,导入一些implicit
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object WordCount {
  def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      //设置分区的数量为1
      env.setParallelism(1)
      //获取建立数据源
      val stream = env.socketTextStream("Master",9999)
      //写对流的转换处理逻辑
      import org.apache.flink.api.scala._
       val transformed: DataStream[WordWithCount] = stream.
         flatMap(line => line.split(" ")).
         map(w => WordWithCount(w, 1))
         .keyBy(0)
         .timeWindow(Time.seconds(5))
         .sum(1)
         //讲计算的结果输出到标准输出
         transformed.print()
         //执行计算逻辑
         env.execute()
  }
}
case class WordWithCount(word:String,count:Int)