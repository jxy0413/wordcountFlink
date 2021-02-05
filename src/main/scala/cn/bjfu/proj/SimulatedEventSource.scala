package cn.bjfu.proj

import java.util.{Calendar, UUID}
import org.apache.flink.streaming.api.functions.source._
import scala.util.Random

class SimulatedEventSource extends RichParallelSourceFunction[MarketingUserBehaviour] {
  var running = true

  val channelSet = Seq("Appstore", "xiaomiStore", "HuaweiStore")
  val behaviourTypes = Seq("BROWSE", "CLICK", "INSTALL")

  val rand = new Random()

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehaviour]): Unit = {
    while (running) {
      val userId = UUID.randomUUID().toString
      val beehaviourType = behaviourTypes(rand.nextInt(behaviourTypes.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = Calendar.getInstance().getTimeInMillis
      ctx.collect(MarketingUserBehaviour(userId, beehaviourType, channel, ts))
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = running = false
}
