package mutable

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class Table(val columns: mutable.Set[String], val rows: mutable.Set[Row] = mutable.Set.empty) {
  def insert(priority: Int, data: mutable.Map[String, Any]): Try[Table] = {
    if (data.keySet.subsetOf(columns.toSet)) {
      val row = new Row(priority, data)
      rows += row
    } else {
      val diffs = data.keySet -- columns
      return Failure(new Exception(s"column ${diffs.mkString(", ")} not found"))
    }
    Success(this)
  }

  /**
   * Join two tables together, and returns a new table.
   *
   * @param another another table to be joined.
   * @return joined result.
   */
  def join(another: Table): Table = {
    columns ++= another.columns
    val rRows: mutable.Set[Row] = mutable.Set.empty
    for ((x, y) <- rows.flatMap(x => another.rows.map(y => (x, y)))) {
      val rRow = x.merge(y)
      if (rRow.isDefined) {
        rRows += rRow.get
      }
    }
    this
  }
}
