package wesleymiao.tech.kafka.offset.monitor.riemann


import com.aphyr.riemann.client.RiemannClient
import collection.JavaConversions._


class Logging {
  def logError(e: String) = println(e)
}


/**
 * Created by wemia on 10/15/2015.
 */
object RiemannConnection extends Logging {

  type RIEMANNEVENT = (String /*host*/, String /*service*/, Option[Long] /*metric*/, Seq[String] /*tags*/, Option[String] /*state*/, Option[String] /*description*/)

  def send(riemannHost: String, riemannPort: Int, records: Iterator[RIEMANNEVENT]) = {

    //println("\n\n\n Raw riemannEvents: " + records)

    var conn : RiemannClient = null
    try {

      conn = RiemannClient.tcp(riemannHost, riemannPort)
      conn.connect()

      val events = records.map {
        case event@(h, srv, m, t, s, d) =>
          val e = conn.event().
            host(h).
            service(srv).
            tags(t: _*)

          val e1 = if (s.isDefined) e.state(s.get) else e
          val e2 = if (m.isDefined) e1.metric(m.get) else e1

          if (d.isDefined) e2.description(d.get).build() else e2.build()
      }

      conn.sendEvents(events.toList).deref(1, java.util.concurrent.TimeUnit.SECONDS)

      //println("\n\n\n Sent to Riemann: " + records)

    } catch {
      case e: Throwable =>
        logError(s"Failed to send Riemann events due to ${e.toString}")
        logError(e.getStackTraceString)
        var c = e.getCause
        var level = 1
        while (c != null) {
          logError(s"Caused by (l$level): ${c.toString}")
          logError(c.getStackTraceString)
          level = level + 1
          c = c.getCause
        }
    } finally {
      if (conn != null) conn.close()
    }
  }

}
