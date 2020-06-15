import scala.io.Source

object PT extends App {
  val topo = Topo.fromJson(Source.fromResource("topo.json").mkString)
  var macTable: Map[Int, Port] =  Map(1 -> Topo.getPortById("s1:1").get, 2 -> Topo.getPortById("s2:1").get, 3 -> Topo.getPortById("s3:1").get)

//  @Mthread
  def l2_custom(pkt: Packet, ingestion: Port): Path = {
    pkt.l2.dst = 3

    if (macTable.contains(pkt.l2.dst)) {
      return topo.shortestPath(ingestion, macTable(pkt.l2.dst))
    } else {
      return topo.stp(ingestion)
    }
  }
}