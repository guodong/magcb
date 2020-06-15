//trait QueryAST {
//  type Table
//  type Schema = Vector[String]
//
//  // relational algebra ops
//  sealed abstract class Operator
//
//  case class Scan(name: Table, schema: Schema, delim: Char, extSchema: Boolean) extends Operator
//
//  case class PrintCSV(parent: Operator) extends Operator
//
//  case class Project(outSchema: Schema, inSchema: Schema, parent: Operator) extends Operator
//
//  case class Filter(pred: Predicate, parent: Operator) extends Operator
//
//  case class Join(parent1: Operator, parent2: Operator) extends Operator
//
//  case class Group(keys: Schema, agg: Schema, parent: Operator) extends Operator
//
//  case class HashJoin(parent1: Operator, parent2: Operator) extends Operator
//
//  // filter predicates
//  sealed abstract class Predicate
//
//  case class Eq(a: Ref, b: Ref) extends Predicate
//
//  sealed abstract class Ref
//
//  case class Field(name: String) extends Ref
//
//  case class Value(x: Any) extends Ref
//
//  // some smart constructors
//  def Schema(schema: String*): Schema = schema.toVector
//
//  def Scan(tableName: String): Scan = Scan(tableName, None, None)
//
//  def Scan(tableName: String, schema: Option[Schema], delim: Option[Char]): Scan
//}
//
//trait SQLParser {
//
//  import scala.util.parsing.combinator._
//
//  def parseSql(input: String) = Grammar.parseAll(input)
//
//  object Grammar extends JavaTokenParsers with PackratParsers {
//    def stm: Parser[Operator] =
//      selectClause ~ fromClause ~ whereClause ~ groupClause ^^ {
//        case p ~ s ~ f ~ g => g(p(f(s)))
//      }
//
//    def selectClause: Parser[Operator => Operator] =
//      "select" ~> ("*" ^^ { _ => (op: Operator) => op } | fieldList ^^ {
//        case (fs, fs1) => Project(fs, fs1, _: Operator)
//      })
//
//    def fromClause: Parser[Operator] =
//      "from" ~> joinClause
//
//    def whereClause: Parser[Operator => Operator] =
//      opt("where" ~> predicate ^^ { p => Filter(p, _: Operator) }) ^^ {
//        _.getOrElse(op => op)
//      }
//
//    def groupClause: Parser[Operator => Operator] =
//      opt("group" ~> "by" ~> fieldIdList ~ ("sum" ~> fieldIdList) ^^ {
//        case p1 ~ p2 => Group(p1, p2, _: Operator)
//      }) ^^ {
//        _.getOrElse(op => op)
//      }
//
//    def joinClause: Parser[Operator] =
//      ("nestedloops" ~> repsep(tableClause, "join") ^^ {
//        _.reduceLeft((a, b) => Join(a, b))
//      }) |
//        (repsep(tableClause, "join") ^^ {
//          _.reduceLeft((a, b) => HashJoin(a, b))
//        })
//
//    def tableClause: Parser[Operator] =
//      tableIdent ~ opt("schema" ~> fieldIdList) ~
//        opt("delim" ~> ("""\t""" ^^ (_ => '\t') | """.""".r ^^ (_.head))) ^^ {
//        case table ~ schema ~ delim => Scan(table, schema, delim)
//      } |
//        ("(" ~> stm <~ ")")
//
//    def fieldIdent: Parser[String] = """[\w\#]+""".r
//
//    def tableIdent: Parser[String] = """[\w_\-/\.]+""".r | "?"
//
//    def fieldList: Parser[(Schema, Schema)] =
//      repsep(fieldIdent ~ opt("as" ~> fieldIdent), ",") ^^ { fs2s =>
//        val (fs, fs1) = fs2s.map { case a ~ b => (b.getOrElse(a), a) }.unzip
//        (Schema(fs: _*), Schema(fs1: _*))
//      }
//
//    def fieldIdList: Parser[Schema] =
//      repsep(fieldIdent, ",") ^^ (fs => Schema(fs: _*))
//
//    def predicate: Parser[Predicate] =
//      ref ~ "=" ~ ref ^^ { case a ~ _ ~ b => Eq(a, b) }
//
//    def ref: Parser[Ref] =
//      fieldIdent ^^ Field |
//        """'[^']*'""".r ^^ (s => Value(s.drop(1).dropRight(1))) |
//        """[0-9]+""".r ^^ (s => Value(s.toInt))
//
//    def parseAll(input: String): Operator = parseAll(stm, input) match {
//      case Success(res, _) => res
//      case res => throw new Exception(res.toString)
//    }
//  }
//
//}
