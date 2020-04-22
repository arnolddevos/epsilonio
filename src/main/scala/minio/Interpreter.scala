package minio

import scala.annotation.tailrec

trait Interpreter extends Signature { this: Structure & Fibers & Synchronization =>

  enum Tail {
    case Continue(tail: () => Tail)
    case Stop

    @tailrec 
    final def run: Unit = 
      this match {
        case Continue(t) => t().run
        case Stop        => ()
      }
  }

  class Runtime(platform: Platform) extends RuntimeOps {
    import IO._
    import platform._

    private def runFiber[E, A](fiber: Fiber[E, A]): Unit = {

      val ignore = (_: Any) => Tail.Stop

      fiberContinue(false, fiber.start, ignore, ignore)

      def fiberContinue[E, A](masked: Boolean, ea: IO[E, A], ke: E => Tail, ka: A => Tail): Unit =
        executeAsync(
          try { recurCPS(masked, ea, ke, ka).run }
          catch { case t => fiberDie(t) }
        )

      def fiberDie(t: Throwable): Unit = 
        runCPS(true, fiber.die(safex(t)), ignore, ignore).run
      
      def recurCPS[E, A](masked: Boolean, ea: IO[E, A], ke: E => Tail, ka: A => Tail): Tail =
        Tail.Continue(() => runCPS(masked, ea, ke, ka))
      
      @tailrec 
      def runCPS[E, A](masked: Boolean, ea: IO[E, A], ke: E => Tail, ka: A => Tail): Tail = {
        ea match {
          case Succeed(a)       => ka(a)
          case Fail(e)          => ke(e)
          case FlatMap(ex, f)   => runCPS(masked, ex, ke, x => recurCPS(masked, f(x), ke, ka))
          case Map(ex, f)       => runCPS(masked, ex, ke, x => ka(f(x)))
          case CatchAll(xa, f)  => runCPS(masked, xa, x => recurCPS(masked, f(x), ke, ka), ka)
          case EffectTotal(a)   => ka(a())
          case EffectSuspend(ea)=> runCPS(masked, ea(), ke, ka)

          case Effect(a)        => 
            try { ka(a()) } 
            catch { case e => ke(safex(e)) }

          case EffectBlocking(a)=> 
            if( masked || fiber.isAlive ) 
              executeBlocking(
                try { ka(a()).run } 
                catch { case e => ke(safex(e)).run }
              )
            Tail.Stop

          case EffectAsync(k)   => 
            if( masked || fiber.isAlive )
              k( 
                ea1 => 
                  if( masked || fiber.isAlive ) 
                    fiberContinue(masked, ea1, ke, ka)
              )
            Tail.Stop

          case Die(t)           => 
            fiberDie(t())
            Tail.Stop

          case Interrupt()      => 
            runCPS(true, fiber.interrupt, ignore, ignore)
              
          case Fork(ea)         => 
            if( masked || fiber.isAlive ) {
              val child = new Fiber(ea)
              runCPS(
                true,
                fiber.adopt(child), 
                ignore, 
                _ => { runFiber(child); ka(child) }
              )
            }
            else Tail.Stop

          case Mask(ea)         =>
            runCPS(true, ea, ke, ka) 

          case Check()          =>
            if( masked || fiber.isAlive ) 
              executeAsync(
                try { ka(()).run }
                catch { case t => fiberDie(t) }
              )
            Tail.Stop
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

    private def safex(t: Throwable): Throwable = if(fatal(t)) shutdown(t) else t
  }
  
  lazy val defaultRuntime = new Runtime(Platform.default)
}