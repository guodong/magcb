import java.io.{File, PrintWriter}

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

  def phi(cond: String, x: Any, y: Any): Any = {
    if (cond == "1" || cond == "*")
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
      case Right(topOrder) => {
        for (node <- topOrder) {
          val inst = node.toOuter
          if (node.diPredecessors.size == 0) {
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
              for (e <- inst.table_sigma.entries) {
                // set output attribute to execution result
                inst match {
                  case x: UdfInstruction1[Any, _] =>
                    e.data = e.data.updated(x.output, x.f(e.data(x.inputs(0))))
                  case x: UdfInstruction2[Any, Any, _] =>
                    e.data = e.data.updated(x.output, x.f(e.data(x.inputs(0)), e.data(x.inputs(1))))
                  case x: UdfInstruction3[Any, Any, Any, _] =>
                    e.data = e.data.updated(x.output, x.f(e.data(x.inputs(0)), e.data(x.inputs(1)), e.data(x.inputs(2))))
                }
              }
            }
            inst match {
              case x: SysInstruction if x.op == "phi" =>
                for (e <- inst.table_sigma.entries) {
                  if (e.data(inst.inputs(0)) == "1") {
                    e.data = e.data.updated(inst.output, e.data(inst.inputs(1)))
                  } else {
                    e.data = e.data.updated(inst.output, e.data(inst.inputs(2)))
                  }
                }
              case _ => Nil
            }
          }
          inst.table_sigma.dump()
        }
      }
      case Left(cycleNode) => throw new Error(s"Graph contains a cycle at node: ${cycleNode}.")
    }
  }

  def localize(sw: String): Unit = {
    val sink_inst = instructions.last
    dfg.topologicalSort() match {

      case Right(topOrder) => {
        println(topOrder)
        for (node <- topOrder.toList.reverse) {
          if (node.toOuter == sink_inst) {
            node.toOuter.table_psi += (sw -> sink_inst.table_sigma.filter(e => e.data(sink_inst.output) match {
              case p: Path => p.isTraverse(sw)
              case _ => false
            }))
          } else {
            var tbs = new ArrayBuffer[Table]()
            for (p <- node.diSuccessors) {
              tbs += p.toOuter.table_psi(sw)
            }
            tbs += node.toOuter.table
            val psi_t = tbs.reduce((t1, t2) => t1.join(t2))
            node.toOuter.table_psi += (sw -> psi_t)
          }
        }
      }
      case Left(cycleNode) => throw new Error(s"Graph contains a cycle at node: ${cycleNode}.")
    }
  }

  def dumpLocalizedTables(sw: String): Unit = {
    println("++++ localized tables +++++")
    instructions.foreach(inst => inst.table_psi(sw).project(inst.table.attributes).dump())
  }

  def getFinalTable(sw: String): Table = {
    val tp = instructions.last.table_psi(sw).project(Set(getValueByName("ingestion").orNull,
      getValueByName("pkt.l2.dst").orNull,
      getValueByName("v5").orNull))
    // here we change variables to per switch variables. e.g., path -> egress_port
    tp.update(getValueByName("ingestion").orNull, (e) => e.data(getValueByName("v5").orNull).asInstanceOf[Path].ingressOf(sw).orNull)
    tp.update(getValueByName("v5").orNull, (e) => e.data(getValueByName("v5").orNull).asInstanceOf[Path].egressOf(sw) match {
      case Some(ps) => ps.toList(0)
      case _ => "drop"
    })
    tp.updateAttribute(getValueByName("ingestion").orNull, new Value("standard_metadata.ingress_port"))
    tp.updateAttribute(getValueByName("v5").orNull, new Value("standard_metadata.egress_spec"))
    tp
  }

  def dump(): Unit = {
    instructions.foreach(println(_))
  }

  def tabulation(): Unit = {
    instructions.foreach(i => i.tabulation())
  }

  def dumpTables(): Unit = {
    instructions.foreach(i => i.table.dump())
  }

  def genP4(name: String, table: Table): String = {
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
        |    /* empty */
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
        |
        |    action t${table.hashCode() & Int.MaxValue}_action(egressSpec_t port) {
        |        standard_metadata.egress_spec = port;
        |    }
        |
        |    table t${table.hashCode() & Int.MaxValue} {
        |        key = {
        |            ${table.keys.map(a => a.toString + ": exact;").mkString("\n")}
        |        }
        |        actions = {
        |            ${table.hashCode() & Int.MaxValue}_action;
        |            drop;
        |            NoAction;
        |        }
        |        size = 1024;
        |        default_action = drop();
        |        const entries = {
        |        ${table.entries.map(e => s"(${table.keys.map(k => e.data(k)).mkString(", ")}): t${table.hashCode() & Int.MaxValue}_action(${table.attributes.diff(table.keys).map(o => e.data(o)).mkString(", ")});").mkString("\n")}
        |        }
        |
        |    }
        |
        |    apply {
        |            t${table.hashCode() & Int.MaxValue}.apply();
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
        |        /* TBD */
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
    val pw = new PrintWriter(new File(s"out/$name.p4" ))
    pw.write(result)
    pw.close()
    result
  }

}
