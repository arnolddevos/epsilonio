package minio
package nodes

import api2._
import scala.concurrent.duration._

/**
* A node in a dataflow graph.  
*/
sealed trait Node {
  type Failure
  def start: IO[Nothing, Fiber[Failure, Unit]]
  def action: IO[Failure, Unit]
}

/**
* A message to a supervisor node.
*/
enum Supervisory[+E] {
  case Started(node: Node, fiber: Fiber[E, Unit])
  case Stopped(node: Node, fiber: Fiber[E, Unit], exit: Exit[E, Unit])
}

/**
* An unsupervised node that does not allow for failure.  
*/
trait UnsupervisedT extends Node {
  type Failure = Nothing
  final def start: IO[Nothing, Fiber[Nothing, Unit]] = action.fork
}

/**
* A supervised node that reports start and finish supervisory events.  
*/
trait SupervisedT[E] extends Node {
  type Failure = E

  def supervisor: Supervisory[E] => IO[Nothing, Unit]

  final def start: IO[Nothing, Fiber[E, Unit]] = {
    import Supervisory._
    println(s"Start invoked: $this")

    def monitor(fiber: Fiber[E, Unit]) =
      for {
        ex <- fiber.await
        _  <- supervisor(Stopped(this, fiber, ex))
      }
      yield ()

    for {
      fiber <- action.fork
      _     <- supervisor(Started(this, fiber))
      _     <- monitor(fiber).fork
    }
    yield fiber
  }
}

/**
* A node that accepts input.  
*/
trait InputT[A] { this: Node =>

  def input: IO[Nothing, A]

  final def react[E]( step: A => IO[E, Unit]): IO[E, Unit] = 
    input.flatMap(step)

  final def reactWithin[E](amount: Duration)(step: Option[A] => IO[E, Unit]): IO[E, Unit] =
    input.map(Some(_)).race(delay(amount).as(None)).flatMap(step)

}

/**
* A node that accepts an alternative input.  
*/
trait WyeT[A] {  this: Node => 
  def wye: IO[Nothing, A]
}

/**
* A node that produces output.  
*/
trait OutputT[A] { this: Node => 
  def output: A => IO[Nothing, Unit]
}

/**
* A node that produces an alternative output.  
*/
trait TeeT[A] {  this: Node => 
  def tee: A => IO[Nothing, Unit]
}

/**
* A top level supervisor node.
*/
trait SupervisorT[E] extends UnsupervisedT with InputT[Supervisory[E]]

trait Supervised[G : Out[Supervisory[E]], E](g: G) extends SupervisedT[E] { this: Node =>
  val supervisor = g.output
}

trait Input[G : In[A], A](g: G) extends InputT[A] { this: Node =>
  val input = g.input
}

trait Wye[G : In[A], A](g: G) extends WyeT[A] { this: Node =>
  val wye = g.input
}

trait Output[G : Out[A], A](g: G) extends OutputT[A] { this: Node =>
  val output = g.output
}

trait Tee[G : Out[A], A](g: G) extends TeeT[A] { this: Node =>
  val tee = g.output
}

trait Supervisor[G : In[Supervisory[A]], A](g: G) extends SupervisorT[A] { this: Node =>
  val input = g.input
}

trait Name(name: String) { this: Node =>
  override def toString  = name
}

type Out = [A] =>> [G] =>> ConnectOut[G, A]
type In  = [A] =>> [G] =>> ConnectIn[G, A]

/**
* Typeclass for outbound connections from a node.
*/
trait ConnectOut[-G, -A] {
  def (g: G).output: A => IO[Nothing, Unit]
}

object ConnectOut {
  given gateConnectOut[A] as ConnectOut[Gate[A, Any], A] {
    def (g: Gate[A, Any]).output = g.offer
  }
}

/**
* Typeclass for inbound connections to a node.
*/
trait ConnectIn[-G, +A] {
  def (g: G).input: IO[Nothing, A]
}

object ConnectIn {
  given gateConnectIn[A] as ConnectIn[Gate[Nothing, A], A] {
    def (g: Gate[Nothing, A]).input = g.take
  }
}
