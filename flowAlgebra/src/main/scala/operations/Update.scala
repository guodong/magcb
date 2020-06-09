package operations

import immutable.Row

case class Update(val f: Row => Row) extends Operation {

}
