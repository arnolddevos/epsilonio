package minio

trait Fibers extends Signature { this: Synchronization =>

  final class Fiber[+E, +A](ea: IO[E, A], parent: Option[Fiber[Any, Any]]) extends FiberOps[E, A] {

    import State._
    import Exit._
    import Status._

    private enum State {
      case Running(interrupt: Boolean, mask: Int, children: List[Fiber[Any, Any]])
      case Cleanup(ex: Exit[E, A], children: List[Fiber[Any, Any]])
      case Terminated(ex: Exit[E, A])

      def running: Boolean = this match { case Running(_, _, _) => true case Cleanup(_, _) | Terminated(_) => false }
    }

    private val state = new Transactor(Running(false, 0, Nil))

    private def neverReturn: IO[Nothing, Nothing] = effectAsync(_ => ())

    private def notifyParent: IO[Nothing, Unit] = parent.fold(unit)(_.childTerminated(this))

    private def childTerminated(child: Fiber[Any, Any]): IO[Nothing, Unit] = {
      state.transact {
        _ match {
          case Running(i, m, children) =>  
            Updated(Running(i, m, children.filter(_ != child)), unit)

          case Cleanup(ex, children) =>
            val r = children.filter(_ != child)
            if( r.isEmpty) Updated(Terminated(ex), notifyParent) 
            else Updated(Cleanup(ex, r), unit)

          case Terminated(_) => 
            Observed(unit)
        }
      }
    }

    private def cleanup(ex: Exit[E, A], children: List[Fiber[Any, Any]]) = {
      if( children.isEmpty) Updated(Terminated(ex), notifyParent andThen neverReturn)
      else Updated(Cleanup(ex, children), foreach(children)(_.interruptFork).unit andThen neverReturn)
    }

    private def succeedAsync(a: A): IO[Nothing, Nothing] =
      state.transact {
        _ match {
          case Running(_, _, children)       => cleanup(Succeed(a), children)
          case Cleanup(_, _) | Terminated(_) => Observed(neverReturn)
        }
      }

    private def failAsync(e: E): IO[Nothing, Nothing] =
      state.transact {
        _ match {
          case Running(_, _, children)       => cleanup(Fail(e), children)
          case Cleanup(_, _) | Terminated(_) => Observed(neverReturn)
        }
      }
  
    // Methods for the current fiber only:-

    def isAlive = state.poll.running

    def start: IO[Nothing, Unit] = ea.flatMap(succeedAsync).catchAll(failAsync)
  
    def dieAsync(t: Throwable): IO[Nothing, Nothing] =
      state.transact {
        _ match {
          case Running(_, _, children)       => cleanup(Die(t), children)
          case Cleanup(_, _) | Terminated(_) => Observed(neverReturn)
        }
      }
  
    def check: IO[Nothing, Unit] =
      state.transact {
        _ match {
          case Running(true, 0, children) => cleanup(Interrupt(), children)
          case Running(_, _, _) | Cleanup(_, _) | Terminated(_) => Observed(unit)
        }
      }
  
    def awaitInterrupt: IO[Nothing, Nothing] = state.transact {
      _ match {
        case Running(true, _, children)    => cleanup(Interrupt(), children)
        case Running(false, _, _)          => Blocked
        case Terminated(_) | Cleanup(_, _) => Observed(neverReturn)
      }
    }
  
    def mask: IO[Nothing, Unit] = incrMask(+1)
    
    def unmask: IO[Nothing, Unit] = incrMask(-1)

    private def incrMask(d: Int): IO[Nothing, Unit] = 
      state.transact {
        _ match {
          case Running(i, m, children)       => Updated(Running(i, m+d, children), unit)
          case Cleanup(_, _) | Terminated(_) => Observed(unit)
        }
      }

    // Methods for any fiber:-

    def interruptFork: IO[Nothing, Unit] = 
      state.transact {
        _ match {
          case Running(false, m, children)   => Updated(Running(true, m, children), unit)
          case Running(_, _, _) | Cleanup(_, _) | Terminated(_) => Observed(unit)
        }
      }
  
    def fork[E1, A1](ea1: IO[E1, A1]): IO[Nothing, Fiber[E1, A1]] = {
      for {
        child <- effectTotal(new Fiber(ea1, Some(this)))
        _ <- state.transact {
          _ match {
            case Running(i, m, children) => Updated(Running(i, m, child :: children), unit)
            case Cleanup(_, _) | Terminated(_) => Observed(child.interruptFork)
          }
        }
      } yield child
    }
  
    // Methods for other fibers:-

    def interrupt: IO[Nothing, Exit[E, A]] =
      for {
        _  <- interruptFork
        ex <- await
      }
      yield ex

    private def awaitTx = state.transaction {
      _ match {
        case Terminated(ex)                   => Observed(ex)
        case Running(_, _, _) | Cleanup(_, _) => Blocked
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
          _  <- this.interrupt
          _  <- foreach(fbs)(_.interrupt)
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
      val fiber = new Fiber(effectSuspendTotal(ea), None)
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