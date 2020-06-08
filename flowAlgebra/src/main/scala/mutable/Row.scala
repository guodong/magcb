package mutable

import scala.collection.mutable

class Row(var priority: Int, val data: mutable.Map[String, Any]) {
  /**
   * Merge two rows together.
   *
   * @param another another row to be merged.
   * @return a new row.
   */
  def merge(another: Row): Option[Row] = {
    val comm_keys = data.keySet.intersect(another.data.keySet)
    for (ck <- comm_keys) {
      if (data(ck).toString != another.data(ck).toString && data(ck).toString != "*" && another.data(ck).toString != "*") {
        return None
      }
    }
    priority += another.priority
    for ((k, v) <- another.data) {
      if (!data.contains(k)) {
        data += (k -> v)
      } else {
        if (data(k).toString == "*") {
          data.update(k, v)
        }
      }
    }
    Some(this)
  }
}
