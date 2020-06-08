object Util {
  def toDpValue(v: Any): String = {
    if (v.isInstanceOf[Boolean]) {
      return if (v.asInstanceOf[Boolean]) "1" else "0"
    } else if (v.isInstanceOf[Path]) {
      (v.hashCode() & Int.MaxValue).toString
    } else if (v.isInstanceOf[Wildcard]) {
      "_"
    } else if (v.isInstanceOf[Port]) {
      (v.toString.hashCode() & Int.MaxValue).toString
//      v.toString
    } else {
      v.toString
    }
  }
}
