package wesleymiao.tech.kafka.offset.monitor.riemann

import com.quantifind.kafka.OffsetGetter.OffsetInfo

class OffsetRiemannReporter (pluginsArgs: String) extends com.quantifind.kafka.offsetapp.OffsetInfoReporter {

  RiemannReporterArguments.parseArguments(pluginsArgs)


  override def report(info: scala.IndexedSeq[OffsetInfo]) =  {
    info.foreach(i => {
//      val values: GaugesValues = gauges.get(getMetricName(i))
//      values.logSize = i.logSize
//      values.offset = i.offset
//      values.lag = i.lag
    })
  }

  def getMetricName(offsetInfo: OffsetInfo): String = {
    offsetInfo.topic.replace(".", "_") + "." + offsetInfo.group.replace(".", "_") + "." + offsetInfo.partition
  }
}
