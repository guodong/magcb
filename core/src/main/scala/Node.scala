trait Node {
  val id: String
  var ports: Set[Port]
}

case class Host(val id: String) extends Node {
  override var ports: Set[Port] = Set.empty
}

case class Switch(val id: String) extends Node {
  override var ports: Set[Port] = Set.empty
}