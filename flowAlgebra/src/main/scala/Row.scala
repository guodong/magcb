import scala.util.control.Breaks._

class Row(val priority: Int, val data: Map[String, Any]) {


  /**
   * Merge two rows together.
   *
   * @param another another row to be merged.
   * @return a new row.
   */
  def merge(another: Row): Option[Row] = {
    var conflict = false
    val comm_keys = data.keySet.intersect(another.data.keySet)
    breakable {
      for (ck <- comm_keys) {
        if (data(ck).toString != another.data(ck).toString && data(ck).toString != "*" && another.data(ck).toString != "*") {
          conflict = true
//          println(data(ck))
//          println(another.data(ck))
          break
        }
      }
    }
    if (!conflict) {
      val attributes = data.keySet ++ another.data.keySet
      var result: Map[String, Any] = Map.empty
      for (a <- attributes) {
        if (data.contains(a) && !another.data.contains(a)) {
          result += (a -> data(a))
        } else if (!data.contains(a) && another.data.contains(a)) {
          result += (a -> another.data(a))
        } else {
          if (data(a).toString == "*") {
            result += (a -> another.data(a))
          } else {
            result += (a -> data(a))
          }
        }
      }
      val e = new Row(another.priority + priority, result)
      return Some(e)
    }
    None
  }

  def project(cols: Iterable[String]): Row = {
    new Row(priority, data.view.filterKeys(e => cols.toSet.contains(e)).toMap)
  }

  override def toString: String = {
    s"$priority | ${data.toSeq.sortBy(_._1).map(_._2).mkString(" | ")}"
  }
}
