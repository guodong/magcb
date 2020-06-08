import scala.annotation.{StaticAnnotation, compileTimeOnly}
import scala.collection.mutable
import scala.language.experimental.macros
import scala.reflect.macros.whitebox.Context

@compileTimeOnly("enable macro paradise to expand macro annotations")
class Mthread extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro MthreadImpl.impl
}


object MthreadImpl {

  def impl(c: Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._
    val guardStack: mutable.Stack[String] = mutable.Stack("gi")
    var idx = 0
    var gidx = 0

    def newGuard(): String = {
      val name = "g" + gidx.toString
      gidx += 1
      name
    }

    def newValue(): String = {
      val name = "v" + idx.toString
      idx += 1
      name
    }

    var gs = "true"
    object trf extends Transformer {
      override def transform(tree: c.universe.Tree): c.universe.Tree = {
        tree match {
          case DefDef(mods, tname, tparams, paramss, tpt, expr) => // q"$mods def $tname[..$tparams](...$paramss): $tpt = $expr" =>
            val g = newGuard()
            guardStack.push(g)
            q"""
                $mods def gen[..$tparams](): Unit = {
                val pktL2Dst = IR.newValue("pkt.l2.dst")
                val ingestion = IR.newValue("ingestion")
                IR.newIfInst(IR.newSysInst(eiPorts, "in", List(ingestion)), () => ${transform(expr)}, ()=>())
               }"""
          case If(cond, thenp, elsep) =>
            val g = newGuard()
            val x = transform(cond)
            gs = "true"
            guardStack.push(g)
            val y = transform(thenp)
            gs = "false"
            val z = transform(elsep)
            guardStack.pop()
            gs = "true"
            val rt = q"""IR.newIfInst($x, () => $y, () => $z)"""
            val r =
              q"""
                val ${c.parse(g)} = $x
                $y
                $z
                val ret = IR.newUdfInst3(phi, List(${c.parse(g)}, ret1, ret2))
               """
            rt
          case Apply(a, b) =>
            val x = super.transformTrees(b)
            val l = x.length

            if (a.toString().contains("contains"))
              // Note: split on string using "." must be escaped
              q"""IR.newSysInst(${c.parse(a.toString().split("\\.").head)}, "in", ${transformTrees(b)})"""
            else if (a.toString() == "macTable")
              q"""IR.newSysInst(macTable, "get", ${transformTrees(b)})"""
            else if (l == 1)
              q"""IR.newUdfInst1($a, ${transformTrees(b)})"""
            else if (l == 2)
              q"""IR.newUdfInst2( $a, ${transformTrees(b)})"""
            else if (l == 3)
              q"""IR.newUdfInst3( $a, ${transformTrees(b)})"""
            else
              q"""new I2(${transform(a)}, ${super.transformTrees(b)})
                 """
          case Return(e) =>
            idx += 1
            val x = c.parse(s"""ret$idx""")
            q"""val ${x} = ${transform(e)}"""
          case Select(e) =>
            q"pktL2Dst"
          case t => super.transform(t)
        }
      }
    }
    println(showRaw(annottees(0)))
    val o = trf.transform(annottees(0).tree)
    c.Expr[Any](o)
  }
}
