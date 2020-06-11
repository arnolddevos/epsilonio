package minio
import scala.annotation._

trait Direct extends Signature { this: Fibers with Synchronization => 
  import Tail._

  type Tail     = minio.Tail[Fiber[Any, Any]]
  val ignore    = (_: Any) => Stop
  val fiberDie  = (t: Throwable) => Access((fiber: Fiber[Any, Any]) => fiber.die(t).tail)
  val fiberLive = (fiber: Fiber[Any, Any]) => fiber.isAlive

  abstract class IO[+E, +A] extends IOops[E, A] { self =>

    def eval(ke: E => Tail, ka: A => Tail): Tail

    def tail = eval(ignore, ignore)

    def flatMap[E1 >: E, B](f: A => IO[E1, B]) = new IO[E1, B] {
      def eval(ke: E1 => Tail, kb: B => Tail) = 
        lazily(self.eval(ke, a => f(a).eval(ke, kb)))
    }

    def catchAll[F, A1 >: A](f: E => IO[F, A1]) = new IO[F, A1] {
      def eval(kf: F => Tail, ka: A1 => Tail) = 
        self.eval(e => f(e).eval(kf, ka), ka)
    }

    def map[B](f: A => B) = new IO[E, B] {
      def eval(ke: E => Tail, kb: B => Tail) = 
        lazily(self.eval(ke, a => kb(f(a))))
    }

    def mapError[F](f: E => F) = new IO[F, A]{
      def eval(kf: F => Tail, ka: A => Tail) = 
        self.eval(e => kf(f(e)), ka)
    }

    def zip[E1 >: E, B](other: IO[E1, B]) = new IO[E1, (A, B)] {
      def eval(ke: E1 => Tail, kab: ((A, B)) => Tail) =
        self.eval(ke, a => other.eval(ke, b => kab((a, b))))
    }

    def fold[B](f: E => B, g: A => B) = new IO[Nothing, B] {
      def eval(kn: Nothing => Tail, kb: B => Tail) =
        self.eval(e => kb(f(e)), a => kb(g(a)))
    }

    def fork = new IO[Nothing, Fiber[E, A]] {
      def eval(ke: Nothing => Tail, ka: Fiber[E, A] => Tail) = 
        Check(fiberLive, 
          Access( 
            _.fork(self).eval( ignore, 
                child => Fork( 
                  Shift(child.start.tail, fiberDie).provide(child), 
                  ka(child))
            )
          )
        )
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
    def eval(ke: Nothing => Tail, ka: A => Tail) = ka(a)
  }

  def fail[E](e: E) = new IO[E, Nothing] {
    def eval(ke: E => Tail, ka: Nothing => Tail) = ke(e)
  }

  def effectTotal[A](a: => A) = new IO[Nothing, A] {
    def eval(ke: Nothing => Tail, ka: A => Tail) = ka(a)
  }

  def effect[A](a: => A) = new IO[Throwable, A] {
    def eval(kt: Throwable => Tail, ka: A => Tail) = 
      Catch(() => a, ka, kt)
  }

  def effectBlocking[A](a: => A) = new IO[Throwable, A] {
    def eval(kt: Throwable => Tail, ka: A => Tail) = 
      Check( fiberLive, 
        Blocking( 
          Catch(() => a, 
            a => Check(fiberLive, Shift(ka(a), fiberDie)), 
            t => Shift(kt(t), fiberDie))))
  }

  def effectAsync[E, A](register: (IO[E, A] => Unit) => Any) = new IO[E, A] {
    def eval(ke: E => Tail, ka: A => Tail) = 
      Check( fiberLive, 
        Async( resume => 
          register( ea => 
            resume(
              Check(fiberLive, Shift(ea.eval(ke, ka), fiberDie))
            )
          )
        )
      )
  }

  def flatten[E, A](suspense: IO[E, IO[E, A]]) = new IO[E, A] {
    def eval(ke: E => Tail, ka: A => Tail) =
      suspense.eval(ke, ea => ea.eval(ke, ka))
  }

  def effectSuspend[A](suspense: => IO[Throwable, A]): IO[Throwable, A] = 
    flatten(effect(suspense))

  def effectSuspendTotal[E, A](suspense: => IO[E, A]) = 
    flatten(effectTotal(suspense))
  
  def foreach[E, A, B](as: Iterable[A])(f: A => IO[E, B]): IO[E, List[B]] =
    as.foldRight[IO[E, List[B]]](succeed(Nil)) { (a, ebs) => 
      for { 
        b  <- f(a) 
        bs <- ebs
      } 
      yield b :: bs
    }

  def interrupt = new IO[Nothing, Nothing] {
    def eval(ke: Nothing => Tail, ka: Nothing => Tail) = Access( _.interrupt.tail )
  }

  def die(t: => Throwable) = new IO[Nothing, Nothing] {
    def eval(ke: Nothing => Tail, ka: Nothing => Tail) = fiberDie(t)
  }

  def mask[E, A](ea: IO[E, A]) = new IO[E, A] {
    def eval(ke: E => Tail, ka: A => Tail) = Mask(ea.eval(e => Unmask(ke(e)), a => Unmask(ka(a))))
  }

  def check = new IO[Nothing, Unit] {
    def eval(ke: Nothing => Tail, ka: Unit => Tail) =
      Check( fiberLive, Shift(ka(()), fiberDie))
  }

  lazy val defaultRuntime = {
    def runTopLevelFiber(fiber: Fiber[Any, Any], runtime: Runtime) =
      Tail.run(Shift(fiber.start.tail, fiberDie).provide(fiber), runtime.platform)

    new Runtime( Platform.default, runTopLevelFiber(_, _))
  }
}
