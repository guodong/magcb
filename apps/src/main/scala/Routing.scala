object Routing {
  //  def shortestPath(src: Port, dst: Port): Path = {
  //    new Path
  //  }
  def shortestPath(topo: Topo, src: Any, dst: Any): Path = {
    val p = new Path
    p.nm = "sssp"
    p
  }

  def stp(topo: Topo, root: Any): Path = {
    val p = new Path
    p.nm = "stp"
    p
  }
}
