import immutable.{Row, Table}
import operations.{Distinct, Filter, Insert, Join, Operation, Project, SetKeys, Update, UpdateColumn}

import scala.collection.mutable.ArrayBuffer

/**
 * Transaction().join($table, $another).project($col).commit()
 */
class Transaction(val table: Table) {
  val operations: ArrayBuffer[Operation] = ArrayBuffer.empty

  def insert(priority: Int, data: Map[String, Any]): Transaction = {
    val op = new Insert(priority, data)
    operations += op
    this
  }

  def join(t: Table): Transaction = {
    val op = new Join(t)
    operations += op
    this
  }

  def filter(f: Row => Boolean): Transaction = {
    val op = new Filter(f)
    operations += op
    this
  }

  def project(cols: Iterable[String]): Transaction = {
    val op = new Project(cols)
    operations += op
    this
  }

  def update(f: Row => Row): Transaction = {
    val op = Update(f)
    operations += op
    this
  }

  def distinct(): Transaction = {
    val op = Distinct()
    operations += op
    this
  }

  def updateColumn(old: String, name: String): Transaction = {
    val op = UpdateColumn(old, name)
    operations += op
    this
  }

  def setKeys(keys: Iterable[String]): Transaction = {
    val op = SetKeys(keys)
    operations += op
    this
  }

  def commit(): Table = {
    var r: Table = table
    for (o <- operations) {
      o match {
        case x: Insert => r = r.insert(x.priority, x.data).get
        case x: Join => r = r.join(x.t)
        case x: Project => r = r.project(x.cols)
        case x: Filter => r = r.filter(x.f)
        case x: Update => r = r.update(x.f)
        case x: Distinct => r = r.distinct()
        case x: UpdateColumn => r = r.updateColumn(x.old, x.name)
        case x: SetKeys => r = r.setKeys(x.keys)
        case _ => None
      }
    }
    r
  }
}
