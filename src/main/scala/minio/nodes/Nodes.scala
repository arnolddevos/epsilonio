package minio
package nodes

import api3._
import scala.concurrent.duration._

/**
* A node in a dataflow system.  
*/
sealed trait NodeT[+E, +A] {
  def action: IO[E, A]
}

trait SupervisorT[+E, +A] extends NodeT[Nothing, Unit] with InputT[Supervisory[E, A]]

/**
* A worker node in a dataflow system.  
*/
trait Node[+E, +A](system: System[E, A]) extends NodeT[E, A] {
  system.registerNode(this)
}

/**
 * A supervisor node in a dataflow system.
 */
trait Supervisor[+E, +A](system: System[E, A]) extends SupervisorT[E, A] {
  val input = system.status.take
  system.registerSupervisor(this)
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
trait InputT[+A] { this: NodeT[Any, Any] =>

  def input: IO[Nothing, A]

  final def react[E, B]( step: A => IO[E, B]): IO[E, B] = 
    input.flatMap(step)

  final def reactWithin[E, B](amount: Duration)(step: Option[A] => IO[E, B]): IO[E, B] =
    input.map(Some(_)).race(delay(amount).as(None)).flatMap(step)

}

/**
* A node that accepts an alternative input.  
*/
trait WyeT[+A] {  this: NodeT[Any, Any] => 
  def wye: IO[Nothing, A]
}

/**
* A node that produces output.  
*/
trait OutputT[-A] { this: NodeT[Any, Any] => 
  def output: A => IO[Nothing, Unit]
}

/**
* A node that produces an alternative output.  
*/
trait TeeT[-A] {  this: NodeT[Any, Any] => 
  def tee: A => IO[Nothing, Unit]
}

/**
 *  Specify an input.
 */
trait Input[G : In[A], A](g: G) extends InputT[A] { this: NodeT[Any, Any] =>
  val input = g.input
}

/**
 *  Specify a second input.
 */
trait Wye[G : In[A], A](g: G) extends WyeT[A] { this: NodeT[Any, Any] =>
  val wye = g.input
}

/**
 *  Specify an output.
 */
trait Output[G : Out[A], A](g: G) extends OutputT[A] { this: NodeT[Any, Any] =>
  val output = g.output
}

/**
 *  Specify a second output.
 */
trait Tee[G : Out[A], A](g: G) extends TeeT[A] { this: NodeT[Any, Any] =>
  val tee = g.output
}

/**
 * The default supervisor node. Interrupts all nodes if one fails.
 */
 trait AllFail extends SupervisorT[Any, Any] {
  import Supervisory._

  def action = react {
    case Stopped(_, _, ex) if !ex.succeeded => unit
    case _ => action
  }
}

/**
 * A system of nodes with a supervisor.
 */
class System[E, A] { 

  import scala.collection.mutable.Buffer

  val status = queue[Supervisory[E, A]](10)
  private val nodes = Buffer[Node[E, A]]()
  private var supervisor: Option[Supervisor[E, A]] = None

  def registerNode(node: Node[E, A]): Unit = nodes += node
  def registerSupervisor(node: Supervisor[E, A]): Unit = supervisor = Some(node)

  private def startNode(node: Node[E, A]): IO[Nothing, Unit] = {
    import Supervisory._

    def monitor(fiber: Fiber[E, A]) =
      for {
        ex <- fiber.await
        _  <- status.offer(Stopped(node, fiber, ex))
      }
      yield ()

    for {
      fiber <- node.action.fork
      _     <- status.offer(Started(node, fiber))
      _     <- monitor(fiber).fork
    }
    yield ()
  }

  def start: IO[Nothing, Unit] = { 
    val visor = supervisor.getOrElse(new Supervisor(this) with AllFail)
    for {
      _ <- foreach(nodes)(startNode).andThen(idle).fork
      _ <- visor.action
    }
    yield ()
  }
}

/**
 * Specify a name for a node.
 */
trait Name(name: String) { this: NodeT[Any, Any] =>
  override def toString  = name
}

type Out = [A] =>> [G] =>> ConnectOut[G, A]
type In  = [A] =>> [G] =>> ConnectIn[G, A]

/**
* Typeclass for outbound connections from a node.
*/
trait ConnectOut[-G, -A] {
  extension (g: G) def output: A => IO[Nothing, Unit] 
}

object ConnectOut {
  given gateConnectOut[A] as ConnectOut[Gate[A, Any], A] {
    extension (g: Gate[A, Any]) def output = g.offer
  }
  given identityConnectOut[A] as ConnectOut[A => IO[Nothing, Unit], A] {
    extension (o: A => IO[Nothing, Unit]) def output = o
  }
}

/**
* Typeclass for inbound connections to a node.
*/
trait ConnectIn[-G, +A] {
  extension (g: G) def input: IO[Nothing, A]
}

object ConnectIn {
  given gateConnectIn[A] as ConnectIn[Gate[Nothing, A], A] {
    extension (g: Gate[Nothing, A]) def input = g.take
  }
  given identityConnectIn[A] as ConnectIn[IO[Nothing, A], A] {
    extension (i: IO[Nothing, A]) def input = i
  }
}
