package wesleymiao.tech.kafka.offset.monitor.riemann

import com.quantifind.kafka.OffsetGetter.OffsetInfo
import com.twitter.util.{Duration, Time}
import com.twitter.conversions.time._

class OffsetRiemannReporter (pluginsArgs: String) extends com.quantifind.kafka.offsetapp.OffsetInfoReporter {

  RiemannReporterArguments.parseArguments(pluginsArgs)


  override def report(info: scala.IndexedSeq[OffsetInfo]) =  {
    // translate to Riemann Event format
    try {

      type RIEMANNEVENT = (String /*host*/, String /*service*/, Option[Long] /*metric*/, Seq[String] /*tags*/, Option[String] /*state*/, Option[String] /*description*/)
      val grouped = info.groupBy(i => (i.topic, i.group))
      val groupedReduced = grouped.mapValues (events => events.map(i => (i.logSize, i.offset, i.lag, i.modified)).reduce( (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, if (a._4 >= b._4) a._4 else b._4)))
      val env = RiemannReporterArguments.env
      val events = groupedReduced.flatMap {
        case ((topic, group), (logSize, offset, lag, mtime)) =>
          val isActive = if ((Time.now - mtime).inNanoseconds > 1.day.inNanoseconds) "inactive" else "active"
          val inactiveFor = (Time.now - mtime).toString()
          val tags = Seq(env, isActive, inactiveFor)
          Seq[RIEMANNEVENT]((topic+"_"+group, "logSize", Some(logSize), tags, Some("ok"), None),
            (topic+"_"+group, "offset", Some(offset), tags, Some("ok"), None),
            (topic+"_"+group, "lag", Some(lag), tags, if (lag > 100) Some("critical") else Some("ok"), Some(isActive + inactiveFor))
          )}

      RiemannConnection.send(RiemannReporterArguments.riemannHost, RiemannReporterArguments.riemannPort, events.toIterator)

    } catch {
      case e: Throwable => println(e.printStackTrace())
    }

  }

  def getMetricName(offsetInfo: OffsetInfo): String = {
    offsetInfo.topic.replace(".", "_") + "_" + offsetInfo.group.replace(".", "_") + "_" + offsetInfo.partition
  }
}
