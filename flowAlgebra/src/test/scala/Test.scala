import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

class Test extends FlatSpec with Matchers {
  val t = new Table("hah", Set("c1"))
  val t1 = t.insert(1, Map("c15" -> "ok"))
  println(t1)
  "A Stack" should "pop values in last-in-first-out order" in {
    val stack = new mutable.Stack[Int]
    stack.push(1)
    stack.push(2)
    stack.pop() should be (2)
    stack.pop() should be (1)
  }

  it should "throw NoSuchElementException if an empty stack is popped" in {
    val emptyStack = new mutable.Stack[Int]
    a [NoSuchElementException] should be thrownBy {
      emptyStack.pop()
    }
  }
}