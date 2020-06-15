import scala.io.Source

object L2 extends App {
  val topo = Topo.fromJson(Source.fromResource("topo.json").mkString)
  var macTable: collection.mutable.Map[Int, Port] = collection.mutable.Map.empty

  //@MThread
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

