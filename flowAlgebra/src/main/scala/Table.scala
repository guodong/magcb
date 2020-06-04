import scala.collection.mutable.ArrayBuffer

class Entry(val priority: Int, var data: Map[Any, Any]) {

  def merge(entry: Entry): Option[Entry] = {
    var conflict = false
    val comm_keys = data.keySet.intersect(entry.data.keySet)
    for (ck <- comm_keys) {
      if (data(ck) != entry.data(ck) && data(ck) != "*" && entry.data(ck) != "*") {
        conflict = true
        // TODO: break
      }
    }
    if (!conflict) {
      val attributes = data.keySet ++ entry.data.keySet
      var result: Map[Any, Any] = Map.empty
      for (a <- attributes) {
        if (data.contains(a) && !entry.data.contains(a)) {
          result += (a -> data(a))
        } else if (!data.contains(a) && entry.data.contains(a)) {
          result += (a -> entry.data(a))
        } else {
          if (data(a) == "*") {
            result += (a -> entry.data(a))
          } else {
            result += (a -> data(a))
          }
        }
      }
      val e = new Entry(entry.priority + priority, result)
      return Some(e)
    } else {
      //      println("confilict")
      //      println(data)
      //      println(entry.data)
    }
    None
  }
}

class Table {
  var attributes: Set[Any] = Set.empty
  val entries: ArrayBuffer[Entry] = ArrayBuffer.empty

  def join(tbl: Table): Table = {
    val result = new Table
    result.attributes = attributes ++ tbl.attributes
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
    for (e <- entries) {
      if (f(e)) {
        table.entries += e
      }
    }
    table
  }

  def project(cols: Set[Any]): Table = {
    val table = new Table
    table.attributes = cols.intersect(attributes)
    for (e <- entries) {
      val newe = new Entry(e.priority, e.data.view.filterKeys(p => cols.contains(p)).toMap)
      table.entries += newe
    }
    table
  }

  def rewrite(): Unit = {

  }

  def distinct(): Unit = {
    var result: Set[Entry] = Set.empty
    for (e <- entries) {

    }
  }

  def dump(): Unit = {
    println(s"pri | ${attributes.mkString(" | ")}")
    for (entry <- entries) {
      print(s"${entry.priority}")
      attributes.foreach(a => print(s" | ${entry.data(a)}"))
      println()
      // Note: do not use map function on attributes as bellow, since it's retures a Set that removes duplicated values
      // println(s"${entry.priority} | ${attributes.map(a => entry.data(a)).mkString(" | ")}")
    }
    println()
  }
}
