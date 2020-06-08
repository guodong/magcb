import scala.io.Source

object L2 extends App {
  val topo = Topo.fromJson(Source.fromResource("topo.json").mkString)
  var macTable: Map[Int, Port] = Map.empty

//  @Mthread
  def l2_custom(pkt: Packet, ingestion: Port): Path = {
    if (!macTable.contains(pkt.l2.src)) {
      macTable += (pkt.l2.src -> ingestion)
    }
    if (macTable.contains(pkt.l2.dst)) {
      return topo.shortestPath(ingestion, macTable(pkt.l2.dst))
    } else {
      return topo.stp(ingestion)
    }
  }
}