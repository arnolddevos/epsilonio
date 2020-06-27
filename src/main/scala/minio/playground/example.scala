package minio.playground

import minio.api2._
import minio.nodes.wiring._
import minio.nodes.{Supervisory, Supervisor, Node}

val stage1, stage2 = queue[String](10)
val errors = queue[Supervisory[Throwable]](10)

val nodes = List(

  new Supervised(errors) with Output(stage1) with Name("data source") {
    def action = output("the message")
  },

  new Supervised(errors) with Input(stage1) with Output(stage2) with Name("data enrich") {
    def action = react { s => output("here it comes...") andThen output(s) andThen action }
  },

  new Supervised(errors) with Input(stage2) with Name("data sink") {
    def action = react { s => fail(new RuntimeException(s)) }
  },

  new Supervisor[Throwable] with Input(errors) {
    def action = react { s => effectTotal(println(s)) }
  }
)

def assemble[G : In[E] : Out[E], E](gate: G)(ctors: G => Node*) = ctors.map(ctor => ctor(gate))

val nodes1 = assemble(errors)(

  new Supervised(_) with Output(stage1) with Name("data source") {
    def action = output("the message")
  },

  new Supervised(_) with Input(stage1) with Output(stage2) with Name("data enrich") {
    def action = react { s => output("here it comes...") andThen output(s) andThen action }
  },

  new Supervised(_) with Input(stage2) with Name("data sink") {
    def action = react { s => fail(new RuntimeException(s)) }
  },

  new Supervisor[Throwable] with Input(_) {
    def action = react { s => effectTotal(println(s)) }
  }

)

def start = foreach(nodes)(_.start).unit

object ExampleMain extends App {
  defaultRuntime.unsafeRunAsync(start)
  Console.in.readLine
}
