package wesleymiao.tech.kafka.offset.monitor.riemann

import java.lang.Integer.parseInt

object RiemannReporterArguments {

  var riemannHost : String = "riemann-hackday.cloudapp.net"

  var riemannPort : Int = 5555

  def parseArguments(args: String) = {
    val argsMap: Map[String, String] = args.split(",").map(_.split("=", 2)).filter(_.length > 1).map(arg => { arg(0) -> arg(1) }).toMap
    argsMap.get("riemannHost").foreach(riemannHost = _)
    argsMap.get("riemannPort").foreach(str => {riemannPort = parseInt(str)})
  }
}
