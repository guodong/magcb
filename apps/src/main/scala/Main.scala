import scala.io.Source

object Main extends App {
  Topo.fromJson(Source.fromResource("topo.json").mkString)
  val eiPorts: Set[Port] = Set(Topo.getPortById("s1:1").orNull, Topo.getPortById("s2:1").orNull, Topo.getPortById("s3:1").orNull)
  var macTable: Map[Int, Port] = Map(100 -> Topo.getPortById("s1:1").orNull, 200 -> Topo.getPortById("s2:1").orNull, 300 -> Topo.getPortById("s3:1").orNull)

  @Mthread
  def l2_custom(pkt: Packet, ingestion: Port): Path = {
    //    if (!macTable.contains(pkt.l2.src)) {
    //      macTable += (pkt.l2.src -> ingestion)
    //    }
    if (macTable.contains(pkt.l2.dst)) {
      return Topo.shortestPath(ingestion, macTable(pkt.l2.dst))
    } else {
      return Topo.stp(ingestion)
    }
  }


  gen()
  IR.dump()
  IR.tabulation()
  IR.dumpTables()
  IR.genDFGInst()
  println(IR.dfg)
  IR.explore()
  for (i <- 1 to 5) {
    IR.localize(s"s$i")
    IR.genP4(s"s$i", IR.getFinalTable(s"s$i"))
  }
  //  IR.dumpFinalTable("s4")

}