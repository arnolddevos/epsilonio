package minio
package playground

import api2._
import nodes._

trait ExamplePolicy extends SupervisorT[Any, Any] {
  import Supervisory._

  def action = react {
    case s @ Stopped(_, _, ex) if !ex.succeeded => effectTotal(println(s))
    case s => effectTotal(println(s)) andThen action
  }
}

val sys = System[Throwable, Unit] 

val stage1, stage2 = queue[String](10)

val sup = new Supervisor(sys) with ExamplePolicy

val src = new Node(sys) with Output(stage1) with Name("data source") {
  def action = output("the message")
}

val enr = new Node(sys) with Input(stage1) with Output(stage2) with Name("data enrich") {
  def action = react { s => output("here it comes...") andThen output(s) andThen action }
}

val snk = new Node(sys) with Input(stage2) with Name("data sink") {
  def action = react { s => effect(println(s"received: $s")) andThen action }
}

object ExampleMain extends App {
  val envelope =
    for {
      fb <- sys.start.fork
      _  <- effect(Console.in.readLine)
      _  <- fb.interrupt
    }
    yield ()

  defaultRuntime.unsafeRunSync(envelope)
  
  
}
