import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
 * Table class. Handles the immutable storage and access of data in a Row / Column format.
 *
 * @param name    the name of table.
 * @param columns the columns of table.
 */
class Table(name: String, val columns: Iterable[String], val rows: Iterable[Row] = Iterable.empty) {
  val keys: Iterable[String] = Iterable.empty

  def length(): Int = {
    rows.size
  }

  /**
   * Insert row to table, and returns a new table
   * @param priority
   * @param data
   * @return
   */
  def insert(priority: Int, data: Map[String, Any]): Try[Table] = {
    var ndata: Map[String, Any] = Map.empty
    for (e <- data) {
      columns.find(_ == e._1) match {
        case Some(c) => ndata += (c -> e._2)
        case _ => return Failure(new Exception(s"column ${e._1} not found"))
      }
    }
    val row = new Row(priority, ndata)
    Success(insert(row))
  }

  def insert(row: Row): Table = {
    insert(List(row))
  }

  def insert(row: Iterable[Row]): Table = {
    val newRows = rows.toSet ++ row.toSet
    buildTable(columns, newRows)
  }

  private def buildTable(columns: Iterable[String], rows: Iterable[Row]): Table = {
    new Table(name, columns, rows)
  }

  /**
   * Join two tables together, and returns a new table.
   *
   * @param another another table to be joined.
   * @return a new table of joined result.
   */
  def join(another: Table): Table = {
    val rColumns = columns.toSet ++ another.columns.toSet
    val rRows: mutable.Set[Row] = mutable.Set.empty
    for ((x, y) <- rows.flatMap(x => another.rows.map(y => (x, y)))) {
      val rRow = x.merge(y)
      if (rRow.isDefined) {
        rRows += rRow.get
      }
    }
    buildTable(rColumns, rRows)
  }

  /**
   * Filter rows of a table.
   *
   * @param f a lambda function for filtering $row => bool
   * @return a new filtered table.
   */
  def filter(f: Row => Boolean): Table = {
    val rRows: mutable.Set[Row] = mutable.Set.empty
    for (r <- rows) {
      if (f(r)) {
        rRows += r
      }
    }
    buildTable(columns, rRows)
  }

  /**
   * Project columns to a subset
   *
   * @param cols subset of table columns
   * @return a new projected table
   */
  def project(cols: Iterable[String]): Table = {
    val rCols = cols.toSet.intersect(columns.toSet)
    val rRows: mutable.Set[Row] = mutable.Set.empty
    for (r <- rows) {
      val row = r.project(cols)
      rRows += row
    }
    buildTable(rCols, rRows)
  }

  /**
   * Update table rows
   *
   * @param f the lambda function takes an original row and returns a new row
   * @return new table
   */
  def update(f: Row => Row): Table = {
    var rRows: Set[Row] = Set.empty
    rows.foreach(r => rRows += f(r))
    buildTable(columns, rRows)
  }

  /**
   * Update table column name, rows with old column name will be updated also
   *
   * @param old
   * @param name
   * @return new table
   */
  def updateColumn(old: String, name: String): Table = {
    val rCols = columns.toSet.filter(_ != old) + name
    var rRows: Set[Row] = Set.empty
    rows.foreach(r => rRows += new Row(r.priority, r.data.removed(old) + (name -> r.data(old))))
    buildTable(rCols, rRows)
  }

  def distinct(): Table = ???

  override def toString: String = {
    s"""
       |pri | ${columns.toSeq.sorted.mkString(" | ")}
       |${rows.mkString("\n")}
       |""".stripMargin
  }
}
