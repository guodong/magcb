abstract class Instruction {
  val table = new Table
  val inputs: List[Value]
  val output: Value
  val gv: Value
  def tabulation(): Unit
  var table_sigma: Table = null
  var table_psi: Map[String, Table] = Map.empty
  val isUdf: Boolean = true
}

class SysInstruction(val gv: Value, gs: Boolean, val ctx: Any, val op: String, val inputs: List[Value], val output: Value) extends Instruction {
  override val isUdf: Boolean = false
  override def toString: String = {
    if (gv == null) {
      s"${output} = $op(${inputs.mkString(", ")})"
    } else {
      if (gs) {
        s"if $gv: ${output} = $op(${inputs.mkString(", ")})"
      } else {
        s"if !$gv: ${output} = $op(${inputs.mkString(", ")})"
      }
    }
  }

  def tabulation(): Unit = {
    if (gv != null) {
      table.attributes += gv
      table.keys += gv
    }
    table.attributes += output
    table.attributes ++= inputs
    table.keys ++= inputs
    if (op == "in") {
      ctx match {
        case x: Map[Int, Port] => {
          for ((k, v) <- x) {
            var entry: Entry = null
            if (gv != null) {
              val gvv = if (gs) "1" else "0"
              entry = new Entry(2, Map(gv -> gvv, inputs(0) -> k, output -> "1"))
            } else {
              entry = new Entry(2, Map(inputs(0) -> k, output -> "1"))
            }
            table.entries += entry
          }
        }
        case x: Set[Port] => {
          for (k <- x) {
            var entry: Entry = null
            if (gv != null) {
              val gvv = if (gs) "1" else "0"
              entry = new Entry(2, Map(gv -> gvv, inputs(0) -> k, output -> "1"))
            } else {
              entry = new Entry(2, Map(inputs(0) -> k, output -> "1"))
            }
            table.entries += entry
          }
        }
      }
      if (gv != null) {
        val gvv = if (gs) "1" else "0"
        val default_entry = new Entry(1, Map(gv -> gvv, inputs(0) -> new Wildcard, output -> "0"))
        table.entries += default_entry
      } else {
        val default_entry = new Entry(1, Map(inputs(0) -> new Wildcard, output -> "0"))
        table.entries += default_entry
      }

      if (gv != null) {
        // inverse gvv
        val igvv = if (gs) "0" else "1"
        var data: Map[Value, Any] = Map(gv -> igvv, output -> new Wildcard)
        data += inputs(0) -> new Wildcard
        data = data.filterKeys(table.attributes).toMap
        val default_entry = new Entry(0, data)
        table.entries += default_entry
      }

    } else if (op == "get") {
      for ((k, v) <- ctx.asInstanceOf[Map[Int, Port]]) {
        if (gv != null) {
          val gvv = if (gs) "1" else "0"
          val entry = new Entry(1, Map(gv -> gvv, inputs(0) -> k, output -> v))
          table.entries += entry
        } else {
          val entry = new Entry(1, Map(inputs(0) -> k, output -> v))
          table.entries += entry
        }
      }
      if (gv != null) {
        // inverse gvv
        val igvv = if (gs) "0" else "1"
        var data: Map[Value, Any] = Map(gv -> igvv, output -> new Wildcard)
        data += inputs(0) -> new Wildcard
        data = data.filterKeys(table.attributes).toMap
        val default_entry = new Entry(0, data)
        table.entries += default_entry
      }
    } else if (op == "phi") {
      if (gv != null) {
        val gvv = if (gs) "1" else "0"
        val entry = new Entry(1, Map(gv -> gvv, inputs(0) -> new Wildcard, output -> new Wildcard))
        table.entries += entry
      } else {
        val entry = new Entry(1, Map(inputs(0) -> new Wildcard, output -> new Wildcard))
        table.entries += entry
      }
      if (gv != null) {
        // inverse gvv
        val igvv = if (gs) "0" else "1"
        var data: Map[Value, Any] = Map(gv -> igvv, output -> new Wildcard)
        data += inputs(0) -> new Wildcard
        data = data.filterKeys(table.attributes).toMap
        val default_entry = new Entry(0, data)
        table.entries += default_entry
      }
    }
  }
}

class UdfInstruction1[iT, oT](val gv: Value, gs: Boolean,
                              val f: iT => oT,
                              val inputs: List[Value],
                              val output: Value) extends Instruction {
  override def toString: String = {
    if (gv == null) {
      s"${output} = $f(${inputs.mkString(", ")})"
    } else {
      if (gs) {
        s"if $gv: ${output} = $f(${inputs.mkString(", ")})"
      } else {
        s"if !$gv: ${output} = $f(${inputs.mkString(", ")})"
      }
    }
  }

  def tabulation(): Unit = {
    if (gv != null) {
      table.attributes += gv
      table.keys += gv
    }
    table.attributes += output
    table.attributes ++= inputs
    table.keys ++= inputs
    var data: Map[Value, Any] = Map.empty
    if (gv != null) {
      val gvv = if (gs) "1" else "0"
      data += (gv -> gvv)
    }
    for (input <- inputs) {
      data += (input -> new Wildcard)
    }
    data += (output -> new Wildcard)
    val entry = new Entry(1, data)
    table.entries += entry

    if (gv != null) {
      // inverse gvv
      val igvv = if (gs) "0" else "1"
      var data: Map[Value, Any] = Map(gv -> igvv, output -> new Wildcard)
      inputs.foreach(i => data += i -> new Wildcard)
      data = data.filterKeys(table.attributes).toMap
      val default_entry = new Entry(0, data)
      table.entries += default_entry
    }
  }
}

class UdfInstruction2[iT1, iT2, oT](val gv: Value, gs: Boolean,
                                    val f: (iT1, iT2) => oT,
                                    val inputs: List[Value],
                                    val output: Value) extends Instruction {
  override def toString: String = {
    if (gv == null) {
      s"${output} = $f(${inputs.mkString(", ")})"
    } else {
      if (gs) {
        s"if $gv: ${output} = $f(${inputs.mkString(", ")})"
      } else {
        s"if !$gv: ${output} = $f(${inputs.mkString(", ")})"
      }
    }
  }

  def tabulation(): Unit = {
    if (gv != null) {
      table.attributes += gv
      table.keys += gv
    }
    table.attributes += output
    table.attributes ++= inputs
    table.keys ++= inputs
    var data: Map[Value, Any] = Map.empty
    if (gv != null) {
      val gvv = if (gs) "1" else "0"
      data += (gv -> gvv)
    }
    for (input <- inputs) {
      data += (input -> new Wildcard)
    }
    data += (output -> new Wildcard)
    val entry = new Entry(1, data)
    table.entries += entry

    if (gv != null) {
      // inverse gvv
      val igvv = if (gs) "0" else "1"
      var data: Map[Value, Any] = Map(gv -> igvv, output -> new Wildcard)
      inputs.foreach(i => data += i -> new Wildcard)
      data = data.filterKeys(table.attributes).toMap
      val default_entry = new Entry(0, data)
      table.entries += default_entry
    }
  }
}

class UdfInstruction3[iT1, iT2, iT3, oT](val gv: Value, gs: Boolean,
                                         val f: (iT1, iT2, iT3) => oT,
                                         val inputs: List[Value],
                                         val output: Value) extends Instruction {
  override def toString: String = {
    if (gv == null) {
      s"${output} = $f(${inputs.mkString(", ")})"
    } else {
      if (gs) {
        s"if $gv: ${output} = $f(${inputs.mkString(", ")})"
      } else {
        s"if !$gv: ${output} = $f(${inputs.mkString(", ")})"
      }
    }
  }

  def tabulation(): Unit = {
    if (gv != null) {
      table.attributes += gv
      table.keys += gv
    }
    table.attributes += output
    table.attributes ++= inputs
    table.keys ++= inputs
    var data: Map[Value, Any] = Map.empty
    if (gv != null) {
      val gvv = if (gs) "1" else "0"
      data += (gv -> gvv)
    }
    for (input <- inputs) {
      data += (input -> new Wildcard)
    }
    data += (output -> new Wildcard)
    val entry = new Entry(1, data)
    table.entries += entry

    if (gv != null) {
      // inverse gvv
      val igvv = if (gs) "0" else "1"
      var data: Map[Value, Any] = Map(gv -> igvv, output -> new Wildcard)
      inputs.foreach(i => data += i -> new Wildcard)
      data = data.filterKeys(table.attributes).toMap
      val default_entry = new Entry(0, data)
      table.entries += default_entry
    }
  }
}


