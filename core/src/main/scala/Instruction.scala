import immutable.{Row, Table}

sealed abstract class Instruction {
  var table: Table = null
  val inputs: List[Value]
  val output: Value
  val gv: Value
  def tabulation(): Unit
  var table_sigma: Table = null
  var table_psi: Map[String, Table] = Map.empty
  var table_rho: Map[String, Table] = Map.empty
  val isUdf: Boolean = true
}

sealed abstract class UdfInstruction extends Instruction {
  val gs: Boolean
  val f: AnyRef
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
    var cols: Set[String] = Set.empty
    if (gv != null) {
      cols += gv.name
    }
    cols += output.name
    inputs.foreach(i => cols += i.name)
    var tb = new Table("", cols, keys=if (gv!=null)inputs.map(_.name) ++ Seq(gv.name) else inputs.map(_.name))
    if (op == "in") {
      ctx match {
        case x: Map[Int, Port] => {
          for ((k, v) <- x) {
            if (gv != null) {
              val gvv = if (gs) true else false
              tb = new Transaction(tb).insert(2, Map(gv.name -> gvv, inputs(0).name -> k, output.name -> true)).commit()
//              tb = tb.insert(2, Map(gv.name -> gvv, inputs(0).name -> k, output.name -> true)).get
            } else {
              tb = new Transaction(tb).insert(2, Map(inputs(0).name -> k, output.name -> true)).commit()
//              tb = tb.insert(2, Map(inputs(0).name -> k, output.name -> true)).get
            }
          }
        }
        case x: Set[Port] => {
          for (k <- x) {
            var row: Row = null
            if (gv != null) {
              val gvv = if (gs) true else false
              tb = tb.insert(2, Map(gv.name -> gvv, inputs(0).name -> k, output.name -> true)).get
            } else {
              tb = tb.insert(2, Map(inputs(0).name -> k, output.name -> true)).get
            }
          }
        }
      }
      if (gv != null) {
        val gvv = if (gs) true else false
        tb = tb.insert(1, Map(gv.name -> gvv, inputs(0).name -> new Wildcard, output.name -> false)).get
      } else {
        tb = tb.insert(1, Map(inputs(0).name -> new Wildcard, output.name -> false)).get
      }

      if (gv != null) {
        // inverse gvv
        val igvv = if (gs) false else true
        var data: Map[String, Any] = Map(gv.name -> igvv, output.name -> new Nul)
        data += inputs(0).name -> new Wildcard
        tb = tb.insert(0, data).get
      }

    } else if (op == "get") {
      for ((k, v) <- ctx.asInstanceOf[Map[Int, Port]]) {
        if (gv != null) {
          val gvv = if (gs) true else false
          tb = tb.insert(1, Map(gv.name -> gvv, inputs(0).name -> k, output.name -> v)).get
        } else {
          tb = tb.insert(1, Map(inputs(0).name -> k, output.name -> v)).get
        }
      }
      if (gv != null) {
        // inverse gvv
        val igvv = if (gs) false else true
        var data: Map[String, Any] = Map(gv.name -> igvv, output.name -> new Nul)
        data += inputs(0).name -> new Wildcard
        tb = tb.insert(0, data).get
      }
    } else if (op == "phi") {
      if (gv != null) {
        val gvv = if (gs) true else false
        tb = tb.insert(1, Map(gv.name -> gvv, inputs(0).name -> new Wildcard, output.name -> new Wildcard)).get
      } else {
        tb = tb.insert(1, Map(inputs(0).name -> new Wildcard, output.name -> new Wildcard)).get
      }
      if (gv != null) {
        // inverse gvv
        val igvv = if (gs) false else true
        var data: Map[String, Any] = Map(gv.name -> igvv, output.name -> new Nul)
        data += inputs(0).name -> new Wildcard
//        data = data.filterKeys(tb.columns.toSet).toMap
        tb = tb.insert(0, data).get
      }
    }
    table = tb
  }
}

class UdfInstruction1[iT, oT](val gv: Value, val gs: Boolean,
                              val f: iT => oT,
                              val inputs: List[Value],
                              val output: Value) extends UdfInstruction {


  def tabulation(): Unit = {
    var cols: Set[String] = Set.empty
    if (gv != null) {
      cols += gv.name
    }
    cols += output.name
    inputs.foreach(i => cols += i.name)
    var tb = new Table("", cols, keys=if (gv!=null)inputs.map(_.name) ++ Seq(gv.name) else inputs.map(_.name))
    var data: Map[String, Any] = Map.empty
    if (gv != null) {
      val gvv = if (gs) true else false
      data += (gv.name -> gvv)
    }
    for (input <- inputs) {
      data += (input.name -> new Wildcard)
    }
    data += (output.name -> new Wildcard)
    tb = tb.insert(1, data).get

    if (gv != null) {
      // inverse gvv
      val igvv = if (gs) false else true
      var data: Map[String, Any] = Map(gv.name -> igvv, output.name -> new Nul)
      inputs.foreach(i => data += i.name -> new Wildcard)
      tb = tb.insert(0, data).get
    }
    table = tb
  }
}

class UdfInstruction2[iT1, iT2, oT](val gv: Value, val gs: Boolean,
                                    val f: (iT1, iT2) => oT,
                                    val inputs: List[Value],
                                    val output: Value) extends UdfInstruction {

  def tabulation(): Unit = {
    var cols: Set[String] = Set.empty
    if (gv != null) {
      cols += gv.name
    }
    cols += output.name
    inputs.foreach(i => cols += i.name)
    var tb = new Table("", cols, keys=if (gv!=null)inputs.map(_.name) ++ Seq(gv.name) else inputs.map(_.name))
    var data: Map[String, Any] = Map.empty
    if (gv != null) {
      val gvv = if (gs) true else false
      data += (gv.name -> gvv)
    }
    for (input <- inputs) {
      data += (input.name -> new Wildcard)
    }
    data += (output.name -> new Wildcard)
    tb = tb.insert(1, data).get

    if (gv != null) {
      // inverse gvv
      val igvv = if (gs) false else true
      var data: Map[String, Any] = Map(gv.name -> igvv, output.name -> new Nul)
      inputs.foreach(i => data += i.name -> new Wildcard)
//      data = data.filterKeys(table.columns.toSet).toMap
      tb = tb.insert(0, data).get
    }
    table = tb
  }
}

class UdfInstruction3[iT1, iT2, iT3, oT](val gv: Value, val gs: Boolean,
                                         val f: (iT1, iT2, iT3) => oT,
                                         val inputs: List[Value],
                                         val output: Value) extends UdfInstruction {

  def tabulation(): Unit = {
    var cols: Set[String] = Set.empty
    if (gv != null) {
      cols += gv.name
    }
    cols += output.name
    inputs.foreach(i => cols += i.name)
    var tb = new Table("", cols, keys=if (gv!=null)inputs.map(_.name) ++ Seq(gv.name) else inputs.map(_.name))
    var data: Map[String, Any] = Map.empty
    if (gv != null) {
      val gvv = if (gs) true else false
      data += (gv.name -> gvv)
    }
    for (input <- inputs) {
      data += (input.name -> new Wildcard)
    }
    data += (output.name -> new Wildcard)
    tb = tb.insert(1, data).get

    if (gv != null) {
      // inverse gvv
      val igvv = if (gs) false else true
      var data: Map[String, Any] = Map(gv.name -> igvv, output.name -> new Nul)
      inputs.foreach(i => data += i.name -> new Wildcard)
//      data = data.filterKeys(table.columns.toSet).toMap
      tb = tb.insert(1, data).get
    }
    table = tb
  }
}


