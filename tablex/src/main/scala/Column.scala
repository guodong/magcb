import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

trait ColumnLike {
  val name: String
}

/**
 * A column with data type T
 *
 * @param name    the name of table.
 * @param default the default value of rows of the column.
 * @param extra   the extra data specified by user.
 * @tparam T the type of values of the column.
 */
class Column1[T: TypeTag](val name: String, default: T = null, extra: Any = null) extends ColumnLike {
  val columnType: universe.Type = typeOf[T]

  override def toString: String = name
}

class Column(val name: String, default: Any = null, extra: Any = null) extends ColumnLike {

  override def toString: String = name
}