class Port(val id: String, val node: Node, var peer: Port = null){
  override def toString: String = id
}