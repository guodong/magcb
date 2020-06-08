class Port(val id: String, val node: Node = null, var peer: Port = null){
  override def toString: String = id
}