import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
 * Table class. Handles the immutable storage and access of data in a Row / Column format.
 *
 * @param name    the name of table.
 * @param columns the columns of table.
 */
class Table(name: String, val columns: Iterable[String], val rows: Iterable[Row] = Iterable.empty) {

//  def getColumn(name: String): ColumnLike = {
//    columns.find(_.name == name).orNull
//  }

  def insert(priority: Int, data: Map[String, Any]): Try[Table] = {
    var ndata: Map[String, Any] = Map.empty
    for (e <- data) {
      columns.find(_.name == e._1) match {
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

  private def buildTable(columns: Iterable[ColumnLike], rows: Iterable[Row]): Table = {
    new Table(name, columns, rows)
  }

  /**
   * Join two tables together, and returns a new table.
   *
   * @param another another table to be joined.
   * @return a new table of joined result.
   */
  def join(another: Table): Table = {
    val tmp = columns.toSet ++ another.columns.toSet
    var rColumns: Set[ColumnLike] = Set.empty
    for (c <- tmp) {
      if (!rColumns.exists(_.name == c.name)) {
        rColumns += c
      }
    }
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
  def project(cols: Iterable[ColumnLike]): Table = {
    val rCols = cols.toSet.intersect(columns.toSet)
    val rRows: mutable.Set[Row] = mutable.Set.empty
    for (r <- rows) {
      val row = r.project(cols)
      rRows += row
    }
    buildTable(rCols, rRows)
  }

  def update(f: Row => Row): Table = {
    var rows: Set[Row] = Set.empty
    rows.foreach(r => rows += f(r))
    buildTable(columns, rows)
  }

  override def toString: String = {
    s"""
       |pri | ${columns.toSeq.sortBy(_.name).mkString(" | ")}
       |${rows.mkString("\n")}
       |""".stripMargin
  }
}
