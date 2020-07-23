package minio
package playground

import api2._
import nodes._

val exampleSuper: Supervisor[Any, Any] = {
  status => 
   new Node[Nothing, Unit] with Input(status) {
     import Supervisory._

     def action = react {
       case s @ Stopped(_, _, ex) if !ex.succeeded => effectTotal(println(s))
       case s => effectTotal(println(s)) andThen action
     }
   }
}

val sys = System[Throwable, Unit](exampleSuper) {
  val stage1, stage2 = queue[String](10)

  List(
    new Node with Output(stage1) with Name("data source") {
      def action = output("the message")
    },

    new Node with Input(stage1) with Output(stage2) with Name("data enrich") {
      def action = react { s => output("here it comes...") andThen output(s) andThen action }
    },

    new Node with Input(stage2) with Name("data sink") {
      def action = react { s => effect(println(s"received: $s")) andThen action }
    },
  )
}

object ExampleMain extends App {
  defaultRuntime.unsafeRunSync(sys.start)
}
