package minio
import scala.annotation._

trait Direct extends Signature { this: Fibers with Synchronization => 

  import Tail._
  import Mask._
  
  abstract class IO[+E, +A] extends IOops[E, A] { parent =>

    def eval(ke: E => Tail, ka: A => Tail): Tail

    def flatMap[E1 >: E, B](f: A => IO[E1, B]): IO[E1, B] = new IO[E1, B] {
      def eval(ke: E1 => Tail, kb: B => Tail) = 
        parent.eval(ke, a => f(a).eval(ke, kb))
    }

    def catchAll[F, A1 >: A](f: E => IO[F, A1]): IO[F, A1] = new IO[F, A1] {
      def eval(kf: F => Tail, ka: A1 => Tail): Tail = 
        parent.eval(e => f(e).eval(kf, ka), ka)
    }

    def map[B](f: A => B): IO[E, B] = new IO[E, B] {
      def eval(ke: E => Tail, kb: B => Tail) = 
        parent.eval(ke, a => kb(f(a)))
    }

    def mapError[F](f: E => F): IO[F, A] = new IO[F, A]{
      def eval(kf: F => Tail, ka: A => Tail): Tail = 
        parent.eval(e => kf(f(e)), ka)
    }

    def zip[E1 >: E, B](other: IO[E1, B]): IO[E1, (A, B)] = new IO[E1, (A, B)] {
      def eval(ke: E1 => Tail, kab: ((A, B)) => Tail): Tail =
        parent.eval(ke, a => other.eval(ke, b => kab((a, b))))
    }

    def fold[B](f: E => B, g: A => B): IO[Nothing, B] = new IO[Nothing, B] {
      def eval(kn: Nothing => Tail, kb: B => Tail): Tail =
        parent.eval(e => kb(f(e)), a => kb(g(a)))
    }

    def fork: IO[Nothing, Fiber[E, A]] = new IO[Nothing, Fiber[E, A]] {
      def eval(ke: Nothing => Tail, ka: Fiber[E, A] => Tail): Tail = Continue {
        (fb, rt, mask) => 
          val child = new Fiber(parent)
          fb.adopt(child).eval(ignore, _ => {
            fiberContinue(child.start, ignore, ignore).run(child, rt, InterruptsOn)
            ka(child)
          })
      }
    }

    def raceAll[E1 >: E, A1 >: A](others: Iterable[IO[E1, A1]]): IO[E1, A1] =
      for {
        fb0 <- fork
        fbs <- foreach(others)(_.fork)
        a   <- fb0.raceAll(fbs)
      }
      yield a

    def race[E1 >: E, A1 >: A](other: IO[E1, A1]): IO[E1, A1] = 
      for {
        fb0 <- fork
        fb1 <- other.fork
        a   <- fb0.raceAll(List(fb1))
      }
      yield a

    def ensuring( finalize: IO[Nothing, Any]): IO[E, A] = 
      mask(
        for {
          c1 <- fork
          ex <- c1.await
          _  <- finalize
          a  <- ex.propagate
        }
        yield a 
      )

    def bracket[E1 >: E, B](release: A => IO[Nothing, Any])(use: A => IO[E1, B]): IO[E1, B] =
      for {
        a <- this
        b <- use(a).ensuring(release(a))
      }
      yield b
  }

  def succeed[A](a: A) = new IO[Nothing, A] {
    def eval(ke: Nothing => Tail, ka: A => Tail): Tail = ka(a)
  }

  def fail[E](e: E) = new IO[E, Nothing] {
    def eval(ke: E => Tail, ka: Nothing => Tail): Tail = ke(e)
  }

  def effectTotal[A](a: => A) = new IO[Nothing, A] {
    def eval(ke: Nothing => Tail, ka: A => Tail): Tail = ka(a)
  }

  def effect[A](a: => A): IO[Throwable, A] = new IO[Throwable, A] {
    def eval(kt: Throwable => Tail, ka: A => Tail): Tail = Continue {
      (_, rt, _) =>
        try { ka(a) }
        catch { case t => kt(rt.safex(t)) }
    }
  }

  def effectBlocking[A](a: => A): IO[Throwable, A] = new IO[Throwable, A] {
    def eval(kt: Throwable => Tail, ka: A => Tail): Tail = Continue {
      (fb, rt, mask) =>
        rt.platform.executeBlocking(
          try { ka(a).run(fb, rt, mask) }
          catch { case t => kt(rt.safex(t)).run(fb, rt, mask) }
        )
        Stop
    }
  }

  def effectAsync[E, A](register: (IO[E, A] => Unit) => Any): IO[E, A] = new IO[E, A] {
    def eval(ke: E => Tail, ka: A => Tail): Tail = 
      Continue {
        (fb, rt, mask) =>
          register(
            ea => 
              fiberContinue(ea, ke, ka).run(fb, rt, mask)
          )
          Stop
      }
  }

  def flatten[E, A](suspense: IO[E, IO[E, A]]): IO[E, A] = new IO[E, A] {
    def eval(ke: E => Tail, ka: A => Tail): Tail =
      suspense.eval(ke, ea => ea.eval(ke, ka))
  }

  def effectSuspend[A](suspense: => IO[Throwable, A]): IO[Throwable, A] = flatten(effect(suspense))

  def effectSuspendTotal[E, A](suspense: => IO[E, A]): IO[E, A] = new IO[E, A] {
    def eval(ke: E => Tail, ka: A => Tail): Tail = suspense.eval(ke, ka)
  }
  
  def foreach[E, A, B](as: Iterable[A])(f: A => IO[E, B]): IO[E, List[B]] =
    as.foldRight[IO[E, List[B]]](succeed(Nil)) { (a, ebs) => 
      for { 
        b  <- f(a) 
        bs <- ebs
      } 
      yield b :: bs
    }

  def interrupt: IO[Nothing, Nothing] = new IO[Nothing, Nothing] {
    def eval(ke: Nothing => Tail, ka: Nothing => Tail): Tail = Continue(
      (fb, _, _) =>
        fb.interrupt.eval(ignore, ignore)
    )
  }

  def die(t: => Throwable): IO[Nothing, Nothing] = new IO[Nothing, Nothing] {
    def eval(ke: Nothing => Tail, ka: Nothing => Tail): Tail = fiberDie(t)
  }

  def mask[E, A](ea: IO[E, A]): IO[E, A] = new IO[E, A] {
    def eval(ke: E => Tail, ka: A => Tail): Tail =
      WithMask(ea.eval(ke, ka))
  }

  def check: IO[Nothing, Unit] = new IO[Nothing, Unit] {
    def eval(ke: Nothing => Tail, ka: Unit => Tail): Tail =
      fiberContinue(unit, ke, ka)
  }

  class Runtime(val platform: Platform) extends RuntimeOps {
    def safex(t: Throwable): Throwable = 
      if(platform.fatal(t)) platform.shutdown(t) else t
  
    def unsafeRunAsync[E, A](ea: => IO[E, A])(k: Exit[E, A] => Any): Unit = {
      val fiber = new Fiber(effectSuspendTotal(ea))
      fiberContinue(fiber.start, ignore, ignore).run(fiber, this, InterruptsOn)
      fiber.awaitNow(k)
    }

    def unsafeRunSync[E, A](ea: => IO[E, A]): Exit[E, A] = {
      val p = new java.util.concurrent.Exchanger[Exit[E, A]]
      unsafeRunAsync(ea)(p.exchange(_))
      p.exchange(null)
    }
  }
  
  lazy val defaultRuntime = new Runtime(Platform.default)

  val ignore = (_: Any) => Stop

  def fiberContinue[E, A](ea: IO[E, A], ke: E => Tail, ka: A => Tail) = Continue {
    (fb, rt, mask) =>
      rt.platform.executeAsync(
        try { ea.eval(ke, ka).run(fb, rt, mask) }
        catch { case t => fiberDie(t).run(fb, rt, mask) }
      )
      Stop
  }

  def fiberDie(t: Throwable) = Continue(
    (fb, rt, _) =>
      fb.die(rt.safex(t)).eval(ignore, ignore))

  enum Mask {
    case InterruptsOn
    case InterruptsOff
  }
    
  enum Tail {
    case Continue(step: (Fiber[Any, Any], Runtime, Mask) => Tail)
    case WithMask(tail: Tail)
    case Stop

    def run(fb: Fiber[Any, Any], rt: Runtime, mask: Mask): Unit = {

      if(mask == InterruptsOff || fb.isAlive)
        loop(mask, this)

      @tailrec 
      def loop(mask: Mask, rec: Tail): Unit = 
        this match {
          case Continue(step)     => loop(mask, step(fb, rt, mask))
          case WithMask(tail)  => loop(InterruptsOff, tail)
          case Stop               => ()
        }
    }
  }


  
}