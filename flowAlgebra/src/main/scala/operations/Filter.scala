package operations

import immutable.Row

case class Filter(val f: Row => Boolean) extends Operation {

}
