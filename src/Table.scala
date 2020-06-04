import scala.collection.mutable.ArrayBuffer

class Entry(val priority: Int, var data: Map[Value, Any]) {

  def merge(entry: Entry): Option[Entry] = {
    var conflict = false
    val comm_keys = data.keySet.intersect(entry.data.keySet)
    for (ck <- comm_keys) {
      if (data(ck).toString != entry.data(ck).toString && data(ck).toString != "*" && entry.data(ck).toString != "*") {
        conflict = true
        // TODO: break here
      }
    }
    if (!conflict) {
      val attributes = data.keySet ++ entry.data.keySet
      var result: Map[Value, Any] = Map.empty
      for (a <- attributes) {
        if (data.contains(a) && !entry.data.contains(a)) {
          result += (a -> data(a))
        } else if (!data.contains(a) && entry.data.contains(a)) {
          result += (a -> entry.data(a))
        } else {
          if (data(a).toString == "*") {
            result += (a -> entry.data(a))
          } else {
            result += (a -> data(a))
          }
        }
      }
      val e = new Entry(entry.priority + priority, result)
      return Some(e)
    }
    None
  }
}

class Attribute(val id: Any, val isKey: Boolean = false) {

}

class Table() {
  var attributes: Set[Value] = Set.empty
  var keys: Set[Value] = Set.empty
  val entries: ArrayBuffer[Entry]= ArrayBuffer.empty

  def join(tbl: Table): Table = {
    val result = new Table
    result.attributes = attributes ++ tbl.attributes
    result.keys = keys ++ tbl.keys
    for ((x, y) <- entries.flatMap(x => tbl.entries.map(y => (x, y)))) {
      val e = x.merge(y)
      if (e.isDefined) {
        result.entries ++= e
      }
    }
    result
  }

  // filter entries with a condition function and returns a new table
  def filter(f: Entry => Boolean): Table = {
    val table = new Table
    table.attributes = attributes
    table.keys = keys
    for (e <- entries) {
      if (f(e)) {
        table.entries += e
      }
    }
    table
  }

  def project(cols: Set[Value]): Table = {
    val table = new Table
    table.attributes = cols.intersect(attributes)
    table.keys = cols.intersect(keys)
    for (e <- entries) {
      val newe = new Entry(e.priority, e.data.view.filterKeys(p => cols.contains(p)).toMap)
      table.entries += newe
    }
    table
  }

  def updateAttribute(attr: Value, newAttr: Value): Unit = {
    var newAttrs: Set[Value] = Set.empty
    var newKeys: Set[Value] = Set.empty
    for (a <- attributes) {
      if (a == attr) {
        newAttrs += newAttr
      } else {
        newAttrs += a
      }
    }
    attributes = newAttrs
    for (a <- keys) {
      if (a == attr) {
        newKeys += newAttr
      } else {
        newKeys += a
      }
    }
    keys = newKeys
    for (e <- entries) {
      e.data = e.data.map(i => if (i._1 == attr) (newAttr -> i._2) else i)
    }
  }

  def update(key: Value, f: Entry => Any): Unit = {
    for (e <- entries) {
      e.data = e.data.updated(key, f(e))
    }
  }

  def distinct(): Unit = {
    var result: Set[Entry] = Set.empty
    for (e <- entries) {

    }
  }

  def dump(): Unit = {
    println(s"pri | ${attributes.mkString(" | ")}")
    for (entry <- entries.sortBy(e => e.priority).reverse) {
      print(s"${entry.priority}")
      attributes.foreach(a => print(s" | ${entry.data(a)}"))
      println()
      // Note: do not use map function on attributes as bellow, since it's retures a Set that removes duplicated values
      // println(s"${entry.priority} | ${attributes.map(a => entry.data(a)).mkString(" | ")}")
    }
    println()
  }

}