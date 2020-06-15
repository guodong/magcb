import java.io.{File, PrintWriter}

import immutable.{Row, Table}
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._
import scalax.collection.mutable.Graph

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object IR {
  val values: ArrayBuffer[Value] = ArrayBuffer.empty
  val instructions: ArrayBuffer[Instruction] = ArrayBuffer.empty

  val guardStack: mutable.Stack[Value] = mutable.Stack(null)
  var idx = 0
  var gs = true
  var dfg: Graph[Instruction, DiEdge] = Graph()

  def newValue(name: String = null): Value = {
    if (name == null) {
      val v = new Value("v" + idx.toString)
      values += v
      idx += 1
      v
    } else {
      val v = new Value(name)
      values += v
      v
    }
  }

  def getValueByName(name: String): Option[Value] = {
    values.find(v => v.name == name)
  }

  def newIfInst(cond: Value, thenp: () => (), elsep: () => ()): Unit = {
    guardStack.push(cond)
    gs = true
    thenp()
    gs = false
    elsep()
    guardStack.pop()
    gs = true
    // TODO: generate phi automatically
    if (guardStack.length == 2) {
      newUdfInst3(phi, List(getValueByName("v1").orNull, getValueByName("v3").orNull, getValueByName("v4").orNull))
      //      newSysInst(Tuple2(getValueByName("v3").orNull, getValueByName("v4").orNull), "phi", List(getValueByName("v1").orNull))
    }
  }

  def phi(cond: Any, x: Any, y: Any): Any = {
    if (cond.toString == "true" || cond == "*")
      x
    else
      y
  }

  def newSysInst(ctx: Any, op: String, inputs: List[Value]): Value = {
    val output = newValue()
    val inst = new SysInstruction(guardStack.top, gs, ctx, op, inputs, output)
    output.instruction = inst
    instructions += inst
    output
  }

  def newUdfInst1[iT, oT](
                           f: iT => oT,
                           inputs: List[Value]): Value = {
    val output = newValue()
    val inst = new UdfInstruction1(guardStack.top, gs, f, inputs, output)
    output.instruction = inst
    instructions += inst
    output
  }

  def newUdfInst2[iT1, iT2, oT](
                                 f: (iT1, iT2) => oT,
                                 inputs: List[Value]): Value = {
    val output = newValue()
    val inst = new UdfInstruction2(guardStack.top, gs, f, inputs, output)
    output.instruction = inst
    instructions += inst
    output
  }

  def newUdfInst3[iT1, iT2, iT3, oT](
                                      f: (iT1, iT2, iT3) => oT,
                                      inputs: List[Value]): Value = {
    val output = newValue()
    val inst = new UdfInstruction3(guardStack.top, gs, f, inputs, output)
    output.instruction = inst
    instructions += inst
    output
  }

  def genDFGInst(): Unit = {
    def getNode(v: Value): Instruction = {
      val n = dfg.nodes.find(n => n.toOuter.output == v)
      n match {
        case Some(x) => x.toOuter
        case None => null
      }
    }

    instructions.foreach(i => dfg.add(i))

    for (inst <- instructions) {
      if (inst.gv != null) {
        val src = getNode(inst.gv)
        if (src != null)
          dfg.add(src ~> inst)
      }

      for (input <- inst.inputs) {
        val src = getNode(input)
        if (src != null)
          dfg.add(src ~> inst)
      }
    }
  }

  def explore(): Unit = {
    dfg.topologicalSort() match {
      case Right(topOrder) =>
        for (node <- topOrder) {
          val inst = node.toOuter
          if (node.diPredecessors.isEmpty) {
            inst.table_sigma = inst.table
          } else {
            var tbs = new ArrayBuffer[Table]()
            for (p <- node.diPredecessors) {
              tbs += p.toOuter.table_sigma
            }

            tbs += node.toOuter.table
            val sigma_t = tbs.reduce((t1, t2) => t1.join(t2))
            inst.table_sigma = sigma_t
            if (inst.isUdf) {
              inst match {
                case x: UdfInstruction1[Any, _] =>
                  val trans: Transaction = new Transaction(inst.table_sigma).update(r => new Row(r.priority, r.data.updated(x.output.name, if (r.data(x.output.name).toString != "nul") {
                    x.f(r.data(x.inputs(0).name))
                  } else new Nul)))
                  inst.table_sigma = trans.commit()
                case x: UdfInstruction2[Any, Any, _] =>
                  val trans: Transaction = new Transaction(inst.table_sigma).update(r => new Row(r.priority, r.data.updated(x.output.name, if (r.data(x.output.name).toString != "nul") x.f(r.data(x.inputs(0).name), r.data(x.inputs(1).name)) else new Nul)))
                  inst.table_sigma = trans.commit()
                case x: UdfInstruction3[Any, Any, Any, _] =>
                  val trans: Transaction = new Transaction(inst.table_sigma).update(r => new Row(r.priority, r.data.updated(x.output.name, if (r.data(x.output.name).toString != "nul") x.f(r.data(x.inputs(0).name), r.data(x.inputs(1).name), r.data(x.inputs(2).name)) else new Nul)))
                  inst.table_sigma = trans.commit()
              }
            }
            inst match {
              case x: SysInstruction if x.op == "phi" =>
                inst.table_sigma = inst.table_sigma.update(r => if (r.data(inst.inputs(0).name) == true) {
                  new Row(r.priority, r.data.updated(x.output.name, r.data(x.inputs(1).name)))
                } else {
                  new Row(r.priority, r.data.updated(x.output.name, r.data(x.inputs(2).name)))
                })
              case _ => Nil
            }
          }
          val cols = if (inst.gv != null) Seq(inst.gv) ++ inst.inputs ++ Seq(inst.output) else inst.inputs ++ Seq(inst.output)
          inst.table_sigma = inst.table_sigma
          println(inst.table_sigma.project(cols.map(_.name)).distinct())
        }
      case Left(cycleNode) => throw new Error(s"Graph contains a cycle at node: ${cycleNode}.")
    }
  }

  /**
   * project global pipeline to switch $sw
   *
   * @param sw target switch to project global pipeline
   */
  def project(sw: String): Unit = {
    val sink_inst = instructions.last
    dfg.topologicalSort() match {

      case Right(topOrder) => {
        for (node <- topOrder.toList.reverse) {
          if (node.toOuter == sink_inst) {
            val filtered_table = sink_inst.table_sigma.filter(e => e.data(sink_inst.output.name) match {
              case p: Path => p.isTraverse(sw)
              case _ => false
            })
            node.toOuter.table_psi += (sw -> filtered_table)
          } else {
            var tbs = new ArrayBuffer[Table]()
            for (p <- node.diSuccessors) {
              tbs += p.toOuter.table_psi(sw)
            }
            tbs += node.toOuter.table
            val psi_t = tbs.reduce((t1, t2) => t1.join(t2))
            //            val result = rewriteKeys(sw)
            node.toOuter.table_psi += (sw -> psi_t)
          }
        }
      }
      case Left(cycleNode) => throw new Error(s"Graph contains a cycle at node: ${cycleNode}.")
    }
  }

  /**
   * rewrite global context variables to local context variables
   *
   * @param sw
   */
  def localize(sw: String): Unit = {
    for (i <- instructions) {
      val trans = new Transaction(i.table_psi(sw)).project(i.table.columns).distinct().setKeys(if (i.gv != null) i.inputs.map(_.name) ++ Seq(i.gv.name) else i.inputs.map(_.name))
      i.table_rho += sw -> trans.commit()
    }

    val last_output_name = instructions.last.output.name
    var r = instructions.last.table_rho(sw).update(r => new Row(r.priority, r.data.updated(last_output_name, r.data(last_output_name).asInstanceOf[Path].egressOf(sw) match {
      case Some(ps) => ps.toList.head.toString.split(":")(1)
      case _ => "drop"
    })))

    r = r.updateColumn(last_output_name, "egress_spec")
    instructions.last.table_rho = instructions.last.table_rho.updated(sw, r)
  }

  def dumpRhoTables(sw: String): Unit = {
    println("++++ localized tables +++++")
    instructions.foreach(inst => println(inst.table_rho(sw)))
  }

  def dump(): Unit = {
    instructions.foreach(println(_))
  }

  def tabulation(): Unit = {
    instructions.foreach(i => i.tabulation())
  }

  def dumpTables(): Unit = {
    instructions.foreach(i => println(i.table))
  }

  def genP4(name: String): String = {
    def genPreTable(): Table = {
      var t = new Table("pre", Set("ingress_port", "ingestion"), keys = Set("ingress_port"))
      if (name == "s1") {
        t = t.insert(0, Map("ingress_port" -> 1, "ingestion" -> new Port("s1:1"))).get
        t = t.insert(0, Map("ingress_port" -> 2, "ingestion" -> new Port("s2:1"))).get
        t = t.insert(0, Map("ingress_port" -> 3, "ingestion" -> new Port("s2:1"))).get
      } else if (name == "s2") {
        t = t.insert(0, Map("ingress_port" -> 1, "ingestion" -> new Port("s2:1"))).get
        t = t.insert(0, Map("ingress_port" -> 2, "ingestion" -> new Port("s1:1"))).get
        t = t.insert(0, Map("ingress_port" -> 3, "ingestion" -> new Port("s1:1"))).get
      } else if (name == "s3") {
        t = t.insert(0, Map("ingress_port" -> 1, "ingestion" -> new Port("s3:1"))).get
        t = t.insert(0, Map("ingress_port" -> 2, "ingestion" -> new Port("s1:1"))).get
        t = t.insert(0, Map("ingress_port" -> 3, "ingestion" -> new Port("s1:1"))).get
      } else if (name == "s4") {
        t = t.insert(0, Map("ingress_port" -> 1, "ingestion" -> new Port("s1:1"))).get
        t = t.insert(0, Map("ingress_port" -> 2, "ingestion" -> new Port("s2:1"))).get
        t = t.insert(0, Map("ingress_port" -> 3, "ingestion" -> new Port("s3:1"))).get
      } else if (name == "s5") {
        t = t.insert(0, Map("ingress_port" -> 1, "ingestion" -> new Port("s1:1"))).get
        t = t.insert(0, Map("ingress_port" -> 2, "ingestion" -> new Port("s2:1"))).get
        t = t.insert(0, Map("ingress_port" -> 3, "ingestion" -> new Port("s3:1"))).get
      }
      t
    }

    def genP4Table(table: Table, inst: Instruction = null): String = {
      val outputs = table.columns.toSet.diff(table.keys.toSet).toSeq.sorted
      s"""
         |    // ${if (inst != null) inst.toString else ""}
         |    action t${table.hashCode() & Int.MaxValue}_action(${outputs.map(o => if (o == "egress_spec") s"bit<9> $o" else s"bit<32> $o").mkString(", ")}) {
         |        ${outputs.map(o => if (o == "egress_spec") s"standard_metadata.egress_spec = $o;" else s"meta.$o = $o;").mkString("\n")}
         |    }
         |    table t${table.hashCode() & Int.MaxValue} {
         |        key = {
         |            ${table.keys.toSeq.sorted.map(a => if (a == "ingress_port") "standard_metadata.ingress_port: exact;" else if (a.contains(".")) "hdr.ethernet.dstAddr: ternary;" else s"meta.${a}: ternary;").mkString("\n")}
         |        }
         |        actions = {
         |            t${table.hashCode() & Int.MaxValue}_action;
         |            drop;
         |            NoAction;
         |        }
         |        size = 1024;
         |        default_action = drop();
         |        const entries = {
         |        ${table.rows.toSeq.sortBy(_.priority)(Ordering[Int].reverse).map(e => s"(${table.keys.toSeq.sorted.map(k => Util.toDpValue(e.data(k))).mkString(", ")}): t${table.hashCode() & Int.MaxValue}_action(${table.columns.toSet.diff(table.keys.toSet).toSeq.sorted.map(o => Util.toDpValue(e.data(o))).mkString(", ")});").mkString("\n")}
         |        }
         |    }""".stripMargin
    }

    def genTables(): String = {
      instructions.map(i => genP4Table(i.table_rho(name), i)).mkString("\n")
    }

    def genApply(): String = {
      instructions.map(i => s"t${i.table_rho(name).hashCode() & Int.MaxValue}.apply();").mkString("\n")
    }

    val pretable = genPreTable()
    val result =
      s"""
         |#include <core.p4>
         |#include <v1model.p4>
         |
         |const bit<16> TYPE_IPV4 = 0x800;
         |
         |/*************************************************************************
         |*********************** H E A D E R S  ***********************************
         |*************************************************************************/
         |
         |typedef bit<9>  egressSpec_t;
         |typedef bit<48> macAddr_t;
         |typedef bit<32> ip4Addr_t;
         |
         |header ethernet_t {
         |    macAddr_t dstAddr;
         |    macAddr_t srcAddr;
         |    bit<16>   etherType;
         |}
         |
         |struct metadata {
         |    ${values.filter(!_.name.contains(".")).map(v => s"bit<32> ${v.name};").mkString("\n")}
         |}
         |
         |struct headers {
         |    ethernet_t   ethernet;
         |}
         |
         |/*************************************************************************
         |*********************** P A R S E R  ***********************************
         |*************************************************************************/
         |
         |parser MyParser(packet_in packet,
         |                out headers hdr,
         |                inout metadata meta,
         |                inout standard_metadata_t standard_metadata) {
         |
         |    state start {
         |        transition parse_ethernet;
         |    }
         |
         |    state parse_ethernet {
         |        packet.extract(hdr.ethernet);
         |        transition accept;
         |    }
         |
         |}
         |
         |/*************************************************************************
         |************   C H E C K S U M    V E R I F I C A T I O N   *************
         |*************************************************************************/
         |
         |control MyVerifyChecksum(inout headers hdr, inout metadata meta) {
         |    apply {
         |
         |    }
         |}
         |
         |
         |/*************************************************************************
         |**************  I N G R E S S   P R O C E S S I N G   *******************
         |*************************************************************************/
         |
         |control MyIngress(inout headers hdr,
         |                  inout metadata meta,
         |                  inout standard_metadata_t standard_metadata) {
         |    action drop() {
         |        mark_to_drop(standard_metadata);
         |    }
         |    ${genP4Table(pretable)}
         |    ${genTables()}
         |
         |    apply {
         |    t${pretable.hashCode() & Int.MaxValue}.apply();
         |    ${genApply()}
         |    }
         |}
         |
         |/*************************************************************************
         |****************  E G R E S S   P R O C E S S I N G   *******************
         |*************************************************************************/
         |
         |control MyEgress(inout headers hdr,
         |                 inout metadata meta,
         |                 inout standard_metadata_t standard_metadata) {
         |    apply {
         |           /* TBD */
         |          }
         |}
         |
         |/*************************************************************************
         |*************   C H E C K S U M    C O M P U T A T I O N   **************
         |*************************************************************************/
         |
         |control MyComputeChecksum(inout headers  hdr, inout metadata meta) {
         |     apply {
         |
         |    }
         |}
         |
         |/*************************************************************************
         |***********************  D E P A R S E R  *******************************
         |*************************************************************************/
         |
         |control MyDeparser(packet_out packet, in headers hdr) {
         |    apply {
         |        packet.emit(hdr.ethernet);
         |    }
         |}
         |
         |/*************************************************************************
         |***********************  S W I T C H  *******************************
         |*************************************************************************/
         |
         |V1Switch(
         |MyParser(),
         |MyVerifyChecksum(),
         |MyIngress(),
         |MyEgress(),
         |MyComputeChecksum(),
         |MyDeparser()
         |) main;
         |""".stripMargin
    //    val tpl = Source.fromResource("p4.tpl").mkString
    //    val template = new Template(tpl)
    //    val ctx = Context().withValues("tbl" -> table)
    //    val result = template.render(ctx)
    val pw = new PrintWriter(new File(s"out/$name.p4"))
    pw.write(result)
    pw.close()
    result
  }

}
