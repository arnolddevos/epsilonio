package minio
package nodes

import api2._
import scala.concurrent.duration._

/**
* A node in a dataflow graph.  
*/
trait Node[+E, +A] {
  def action: IO[E, A]
}

/**
* A message to a supervisor node.
*/
enum Supervisory[+E, +A] {
  case Started(node: Node[E, A], fiber: Fiber[E, A])
  case Stopped(node: Node[E, A], fiber: Fiber[E, A], exit: Exit[E, A])
}

/**
* A node that accepts input.  
*/
trait InputT[A] { this: Node[Any, Any] =>

  def input: IO[Nothing, A]

  final def react[E, B]( step: A => IO[E, B]): IO[E, B] = 
    input.flatMap(step)

  final def reactWithin[E, B](amount: Duration)(step: Option[A] => IO[E, B]): IO[E, B] =
    input.map(Some(_)).race(delay(amount).as(None)).flatMap(step)

}

/**
* A node that accepts an alternative input.  
*/
trait WyeT[A] {  this: Node[Any, Any] => 
  def wye: IO[Nothing, A]
}

/**
* A node that produces output.  
*/
trait OutputT[A] { this: Node[Any, Any] => 
  def output: A => IO[Nothing, Unit]
}

/**
* A node that produces an alternative output.  
*/
trait TeeT[A] {  this: Node[Any, Any] => 
  def tee: A => IO[Nothing, Unit]
}

/**
 *  Specify an input.
 */
trait Input[G : In[A], A](g: G) extends InputT[A] { this: Node[Any, Any] =>
  val input = g.input
}

/**
 *  Specify a second input.
 */
trait Wye[G : In[A], A](g: G) extends WyeT[A] { this: Node[Any, Any] =>
  val wye = g.input
}

/**
 *  Specify an output.
 */
trait Output[G : Out[A], A](g: G) extends OutputT[A] { this: Node[Any, Any] =>
  val output = g.output
}

/**
 *  Specify a second output.
 */
trait Tee[G : Out[A], A](g: G) extends TeeT[A] { this: Node[Any, Any] =>
  val tee = g.output
}

/**
 * A supervisor depends on a status input channel.
 */
 type Supervisor[-E, -A] = IO[Nothing, Supervisory[E, A]] => Node[Nothing, Unit]


/**
 * The default supervisor node. Interrupts all nodes if one fails.
 */
 val allFailSupervisor: Supervisor[Any, Any] = {
   status => 
    new Node[Nothing, Unit] with Input(status) {
      import Supervisory._

      def action = react {
        case Stopped(_, _, ex) if !ex.succeeded => unit
        case _ => action
      }
    }
 }

/**
 * A system of nodes with a supervisor.
 */
class System[E, A](supervisor: Supervisor[E, A] = allFailSupervisor)(nodes: => Iterable[Node[E, A]]) { 

  def start: IO[Nothing, Unit] = { 

    val status = queue[Supervisory[E, A]](1)

    def startNode(node: Node[E, A]): IO[Nothing, Unit] = {
      import Supervisory._

      def monitor(fiber: Fiber[E, A]) =
        for {
          ex <- fiber.await
          _  <- status.offer(Stopped(node, fiber, ex))
        }
        yield ()

      for {
        fiber <- node.action.fork
        _     <- status.offer(Started(node, fiber)).fork
        _     <- monitor(fiber).fork
      }
      yield ()
    }

    for {
      _ <- foreach(nodes)(startNode)
      s = supervisor(status.take)
      _ <- s.action
    }
    yield ()
  }
}

/**
 * Specify a name for a node.
 */
trait Name(name: String) { this: Node[Any, Any] =>
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
  given identityConnectIn[A] as ConnectIn[IO[Nothing, A], A] {
    def (i: IO[Nothing, A]).input = i
  }
}
