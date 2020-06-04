class PathElement(val ingress: Port, var egresses: Set[Port], val fwdIndicator: String = "all")

class Path {
  var nm: String = "path"
  var elements: Set[PathElement] = Set.empty

  def isTraverse(sw: String): Boolean = {
    for (e <- elements) {
      if (e.ingress.node.id == sw) {
        return true
      }
      for (egress <- e.egresses) {
        if (egress.node.id == sw) {
          return true
        }
      }
    }
    false
  }

  def ingressOf(sw: String): Option[Port] = {
    for (e <- elements) {
      if (e.ingress.node.id == sw) {
        return Some(e.ingress)
      }
    }
    None
  }

  def egressOf(sw: String): Option[Set[Port]] = {
    for (e <- elements) {
      if (e.ingress.node.id == sw) {
        return Some(e.egresses)
      }
    }
    None
  }

  override def toString: String = {
    elements.map(e => s"${e.ingress} -> (${e.egresses.mkString(", ")})").mkString(", ")
  }
}