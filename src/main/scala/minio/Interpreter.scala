package minio

import scala.annotation.tailrec

trait Interpreter extends Signature { this: Structure & Fibers & Synchronization =>

  private def runFiber(fiber: Fiber[Any, Any], platform: Platform): Unit = {
    import platform._
    import IO._

    def safex(t: Throwable): Throwable = 
      if(platform.fatal(t)) platform.shutdown(t) else t
  
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
          if( masked || fiber.isAlive ) {
            executeBlocking(
              try { ka(a()).run } 
              catch { case e => ke(safex(e)).run }
            )
            Tail.Stop
          }
          else runCPS(false, fiber.idle, ignore, ignore)
          
        case EffectAsync(k)   => 
          if( masked || fiber.isAlive ) {
            k( 
              ea1 => 
                if( masked || fiber.isAlive ) fiberContinue(masked, ea1, ke, ka)
                else fiberContinue(masked, fiber.idle, ke, ka)
            )
            Tail.Stop
          }
          else runCPS(false, fiber.idle, ignore, ignore)

        case Die(t)           => 
          fiberDie(t())
          Tail.Stop

        case Interrupt()      => 
          runCPS(true, fiber.interruptFork.andThen(fiber.idle), ignore, ignore)
            
        case Fork(ea)         => 
          if( masked || fiber.isAlive ) {
            runCPS(
              true,
              fiber.fork(ea), 
              ignore, 
              child => { runFiber(child, platform); ka(child) }
            )
          }
          else runCPS(false, fiber.idle, ignore, ignore)

        case Mask(ea)         =>
          runCPS(true, ea, ke, ka) 

        case Check()          =>
          if( masked || fiber.isAlive ) {
            executeAsync(
              try { ka(()).run }
              catch { case t => fiberDie(t) }
            )
            Tail.Stop
          }
          else runCPS(false, fiber.idle, ignore, ignore)

        case Never()          =>
          runCPS(false, fiber.idle, ignore, ignore)
      }
    }
  }
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

  lazy val defaultRuntime = new Runtime(Platform.default, runFiber)
}