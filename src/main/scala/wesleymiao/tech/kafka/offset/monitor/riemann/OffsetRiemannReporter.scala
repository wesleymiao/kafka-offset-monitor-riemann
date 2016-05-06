package wesleymiao.tech.kafka.offset.monitor.riemann

import com.quantifind.kafka.OffsetGetter.OffsetInfo

class OffsetRiemannReporter (pluginsArgs: String) extends com.quantifind.kafka.offsetapp.OffsetInfoReporter {

  RiemannReporterArguments.parseArguments(pluginsArgs)


  override def report(info: scala.IndexedSeq[OffsetInfo]) =  {
    // translate to Riemann Event format
    type RIEMANNEVENT = (String /*host*/, String /*service*/, Option[Long] /*metric*/, Seq[String] /*tags*/, Option[String] /*state*/, Option[String] /*description*/)
    val events = info.flatMap { i => Seq[RIEMANNEVENT](
      (getMetricName(i), "logSize", Some(i.logSize), Seq(), Some("ok"), None),
      (getMetricName(i), "offset", Some(i.offset), Seq(), Some("ok"), None),
      (getMetricName(i), "lag", Some(i.lag), Seq(), if (i.lag > 0) Some("critical") else Some("ok"), None)
    )}

    RiemannConnection.send(RiemannReporterArguments.riemannHost, RiemannReporterArguments.riemannPort, events.toIterator)
  }

  def getMetricName(offsetInfo: OffsetInfo): String = {
    offsetInfo.topic.replace(".", "_") + "." + offsetInfo.group.replace(".", "_") + "." + offsetInfo.partition
  }
}
