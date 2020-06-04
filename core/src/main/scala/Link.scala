class Link(val p0: Port, val p1: Port, val bw: String) {
  def getPortByNode(node: Node): Option[Port] = {
    if (p0.node == node) {
      return Some(p0)
    } else if (p1.node == node) {
      return Some(p1)
    }
    None
  }
}