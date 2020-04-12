package minio

trait Interpreter extends Signature { this: Structure & Synchronization =>

  final class Fiber[+E, +A](ea: IO[E, A]) extends FiberOps[E, A] {

    private enum FiberState {
      case Ready
      case Running
      case Managing(children: List[Fiber[Any, Any]])
      case Terminated(ex: Exit[E, A])
    }

    import FiberState._
    import Exit._

    private val state = new Transactor[FiberState](Ready)
    import state.Status._

    def isAlive = state.poll match { 
      case _: Terminated => false 
      case _ => true 
    }

    private def runToExit =
      for {
        x <- ea.fold(Fail(_), Succeed(_))
        _ <- exit(x)
      }
      yield ()

    def start: IO[Nothing, Unit] =
      for {
        ready <- state.transactTotal {
          _ match {
            case Ready => Updated(Running, true)
            case _     => Observed(false)
          }
        }
        _ <- if(ready) runToExit else unit
      }
      yield ()

    def adopt(cf: Fiber[Any, Any]): IO[Nothing, Unit] =
      state.transact {
        _ match {
          case Ready|Running => Updated(Managing(cf :: Nil), unit)
          case Managing(cfs) => Updated(Managing(cf :: cfs), unit)
          case Terminated(_) => Observed(cf.interrupt.map(_ => ()))
        }
      }

    private def exit(ex: Exit[E, A]): IO[Nothing, Exit[E, A]] =
      state.transact {
        _ match {
          case Terminated(ex0) => 
            Observed(succeed(ex0))
          case Managing(cfs) => 
            Updated(Terminated(ex), foreach(cfs)(_.interrupt).map(_ => ex))
          case _ => 
            Updated(Terminated(ex), succeed(ex) )
        }
      }

    def die(t: Throwable): IO[Nothing, Exit[E, A]] = exit(Die(t))

    def interrupt: IO[Nothing, Exit[E, A]] = exit(Interrupt())

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

  class Arbiter[E, A](quota: Int) {
    enum State {
      case Pending(count: Int)
      case Complete(exit: Exit[E,A])
    }
    import State._

    val accum = new Transactor(Pending(quota))
    import accum.Status._

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

  class Runtime(platform: Platform) extends RuntimeOps {
    import IO._
    import platform._

    private def runFiber[E, A](fiber: Fiber[E, A]): Unit = {

      executeAsync(
        try { runCPS(false, fiber.start, _ => (), _ => ()) }
        catch { case t => runCPS(true, fiber.die(check(t)), _ => (), _ => ()) }
      )

      def runCPS[E, A](masked: Boolean, ea: IO[E, A], ke: E => Unit, ka: A => Unit): Unit = {
        if( masked || fiber.isAlive ) 
          ea match {
            case Succeed(a)        => ka(a)
            case Fail(e)           => ke(e)
            case FlatMap(ex, f)    => runCPS(masked, ex, ke, x => runCPS(masked, f(x), ke, ka))
            case Map(ex, f)        => runCPS(masked, ex, ke, x => ka(f(x)))
            case CatchAll(xa, f)   => runCPS(masked, xa, x => runCPS(masked, f(x), ke, ka), ka)
            case EffectTotal(a)    => ka(a())
            case EffectSuspend(ea) => runCPS(masked, ea(), ke, ka)

            case Effect(a)        => 
              try { ka(a()) } 
              catch { case e => ke(check(e)) }

            case EffectBlocking(a)=> 
              executeBlocking(
                try { ka(a()) } 
                catch { case e => ke(check(e)) }
              )

            case EffectAsync(k)   => 
              k( ea1 => 
                executeAsync(
                  try { runCPS(masked, ea1, ke, ka) }
                  catch { case e => runCPS(true, fiber.die(check(e)), _ => (), _ => ()) }
                )
              )

            case Die(t)           => 
              runCPS(true, fiber.die(t()), _ => (), _ => ())

            case Interrupt()      => 
              runCPS(true, fiber.interrupt, _ => (), _ => ())
                
            case Fork(ea)         => 
              val child = new Fiber(ea)
              runCPS(
                true,
                fiber.adopt(child), 
                _ => (), 
                _ => { runFiber(child); ka(child) }
              )
              
            case Mask(ea)   =>
              runCPS(true, ea, ke, ka) 
          }
      }
    }

    def unsafeRunAsync[E, A](ea: => IO[E, A])(k: Exit[E, A] => Any): Unit = {
      val fiber = new Fiber(effectSuspendTotal(ea))
      runFiber(fiber)
      fiber.awaitNow(k)
    }

    def unsafeRunSync[E, A](ea: => IO[E, A]): Exit[E, A] = {
      val p = new java.util.concurrent.Exchanger[Exit[E, A]]
      unsafeRunAsync(ea)(p.exchange(_))
      p.exchange(null)
    }

    private def check(t: Throwable): Throwable = if(fatal(t)) shutdown(t) else t
  }
  
  lazy val defaultRuntime = new Runtime(Platform.default)

  trait Platform {
    def fatal(t: Throwable): Boolean
    def shutdown(t: Throwable): Nothing
    def executeAsync(k: => Unit): Unit
    def executeBlocking(k: => Unit): Unit
  }

  object Platform {
    lazy val default =
      new Platform {
        import java.util.concurrent._
        import ForkJoinPool._

        private val pool = new ForkJoinPool

        def fatal(t: Throwable): Boolean = 
          ! scala.util.control.NonFatal(t)

        def shutdown(t: Throwable): Nothing = {
          pool.shutdownNow()
          throw t
        }

        def executeAsync(k: => Unit): Unit = 
          pool.execute( new Runnable { def run() = k } )

        def executeBlocking(k: => Unit): Unit = {
          val blocker =
            new ManagedBlocker {
              var isReleasable = false
              def block(): Boolean = {
                isReleasable = true
                k
                true
              }
            }
          executeAsync(managedBlock(blocker))
        }
      }
  }
}