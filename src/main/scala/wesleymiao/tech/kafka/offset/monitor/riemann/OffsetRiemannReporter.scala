package wesleymiao.tech.kafka.offset.monitor.riemann

import com.quantifind.kafka.OffsetGetter.OffsetInfo

class OffsetRiemannReporter (pluginsArgs: String) extends com.quantifind.kafka.offsetapp.OffsetInfoReporter {

  RiemannReporterArguments.parseArguments(pluginsArgs)


  override def report(info: scala.IndexedSeq[OffsetInfo]) =  {
    // translate to Riemann Event format
    type RIEMANNEVENT = (String /*host*/, String /*service*/, Option[Long] /*metric*/, Seq[String] /*tags*/, Option[String] /*state*/, Option[String] /*description*/)
    val grouped = info.groupBy(i => (i.topic, i.group))
    val groupedReduced = grouped.mapValues (events => events.map(i => (i.logSize, i.offset, i.lag)).reduce( (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3)))
    val events = groupedReduced.flatMap {
      case ((topic, group), (logSize, offset, lag)) => Seq[RIEMANNEVENT](
      (topic+"_"+group, "logSize", Some(logSize), Seq(), Some("ok"), None),
      (topic+"_"+group, "offset", Some(offset), Seq(), Some("ok"), None),
      (topic+"_"+group, "lag", Some(lag), Seq(), if (lag > 10) Some("critical") else Some("ok"), None)
    )}

    RiemannConnection.send(RiemannReporterArguments.riemannHost, RiemannReporterArguments.riemannPort, events.toIterator)
  }

  def getMetricName(offsetInfo: OffsetInfo): String = {
    offsetInfo.topic.replace(".", "_") + "_" + offsetInfo.group.replace(".", "_") + "_" + offsetInfo.partition
  }
}
