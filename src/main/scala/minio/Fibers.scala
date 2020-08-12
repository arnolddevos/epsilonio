package minio

trait Fibers extends Signature { this: Synchronization =>

  final class Fiber[+E, +A](ea: IO[E, A]) extends FiberOps[E, A] {

    private enum FiberState {
      case Running
      case Managing(children: List[Fiber[Any, Any]])
      case Terminated(ex: Exit[E, A])
    }

    import FiberState._
    import Exit._
    import Status._

    private val state = new Transactor(Running)

    def isAlive = state.poll match { 
      case _: Terminated => false 
      case _ => true 
    }

    def start: IO[Nothing, Unit] =
      for {
        x <- ea.fold(Fail(_), Succeed(_))
        _ <- exit(x)
      }
      yield ()

    def fork[E1, A1](ea1: IO[E1, A1]): IO[Nothing, Fiber[E1, A1]] = {
      for {
        child <- effectTotal(new Fiber(ea1))
        _ <- state.transact {
          _ match {
            case Running            => Updated(Managing(child :: Nil), unit)
            case Managing(children) => Updated(Managing(child :: children.filter(_.isAlive)), unit)
            case Terminated(_)      => Observed(child.interruptFork)
          }
        }
      } yield child
    }

    private def exit(ex: Exit[E, A]): IO[Nothing, Exit[E, A]] =
      state.transact {
        _ match {
          case Running            => Updated(Terminated(ex), succeed(ex) )
          case Managing(children) => Updated(Terminated(ex), 
                                        foreach(children)(_.interruptFork).as(ex))
          case Terminated(ex0)    => Observed(succeed(ex0))
        }
      }

    def die(t: Throwable): IO[Nothing, Exit[E, A]] = exit(Die(t))

    def interruptFork: IO[Nothing, Unit] = exit(Interrupt()).unit

    private def awaitTx = state.transaction {
      _ match {
        case Terminated(ex) => Observed(ex)
        case _              => Blocked
      }
    }

    def await: IO[Nothing, Exit[E, A]] = state.transactTotal(awaitTx)

    def awaitNow(k: Exit[E, A] => Any): Unit = state.transactNow(awaitTx)(k)
  
    def join: IO[E, A] =
      for {
        x <- await
        a <- x.propagate
      }
      yield a

    def raceAll[E1 >: E, A1 >: A](fbs: Iterable[Fiber[E1, A1]]): IO[E1, A1] = {
      val run =
        for {
          arb <- effectTotal { new Arbiter[E1, A1](1 + fbs.size) }
          _   <- arb.register(this)
          _   <- foreach(fbs)(arb.register) 
          ex <- arb.await
        }
        yield ex
  
      val cleanup =
        for {
          _  <- this.interruptFork
          _  <- foreach(fbs)(_.interruptFork)
        }
        yield ()
  
      for {
        ex <- run.ensuring(cleanup)
        a  <- ex.propagate
      }
      yield a
    }
  }

  enum Exit[+E, +A] extends ExitOps[E, A] {

    case Succeed(a: A)
    case Fail(e: E)
    case Die(t: Throwable)
    case Interrupt()

    def propagate: IO[E, A] =
      this match {
        case Succeed(a) => succeed(a)
        case Fail(e)    => fail(e)
        case Interrupt()=> interrupt
        case Die(t)     => die(t)
    }

    def flatMap[E1 >: E, B](f: A => Exit[E1, B]): Exit[E1, B] =
      this match {
        case Succeed(a) => f(a)
        case Fail(e)    => Fail(e)
        case Die(t)     => Die(t)
        case Interrupt()=> Interrupt()
      }

    def map[B](f: A => B): Exit[E, B] = flatMap(a => Succeed(f(a)))

    def option: Option[A] =
      this match {
        case Succeed(a) => Some(a)
        case _          => None
      }

    def succeeded: Boolean =
      this match {
        case Succeed(_) => true
        case _          => false
      }
  }

  class Arbiter[E, A](quota: Int) {
    enum State {
      case Pending(count: Int)
      case Complete(exit: Exit[E,A])
    }
    import State._
    import Status._

    val accum = new Transactor(Pending(quota))

    private def signal(ex: Exit[E, A]) = accum.transaction(
      _ match {
        case Pending(n) if n == 1 || ex.succeeded => Updated(Complete(ex), ())
        case Pending(n)  => Updated(Pending(n-1), ())
        case Complete(_) => Observed(())
      }
    )

    def register(fb: Fiber[E, A]): IO[Nothing, Unit] = effectTotal(
      fb.awaitNow( ex => 
        accum.transactNow(signal(ex))(_ => ())
      )
    )

    val await: IO[Nothing, Exit[E, A]] = accum.transactTotal(
      _ match {
        case Pending(_)   => Blocked
        case Complete(ex) => Observed(ex)
      }
    )
  }

  class Runtime(val platform: Platform, runFiber: (Fiber[Any, Any], Runtime) => Unit) extends RuntimeOps {
    def safex(t: Throwable): Throwable = 
      if(platform.fatal(t)) platform.shutdown(t) else t
  
    def unsafeRunAsync[E, A](ea: => IO[E, A])(k: Exit[E, A] => Any): Unit = {
      val fiber = new Fiber(effectSuspendTotal(ea))
      fiber.awaitNow(k)
      runFiber(fiber, this)
    }

    def unsafeRunSync[E, A](ea: => IO[E, A]): Exit[E, A] = {
      val p = new java.util.concurrent.Exchanger[Exit[E, A]]
      unsafeRunAsync(ea)(p.exchange(_))
      p.exchange(null)
    }
  }
}