class Wildcard extends Port("*", null) {
  override def toString: String = "*"
}

class Nul extends Port("nul", null) {
  override def toString: String = id
}