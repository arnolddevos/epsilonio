package minio
import scala.annotation._

trait Simple extends Signature { this: Fibers with Synchronization => 

  enum IO[+E, +A] extends IOops[E, A] {

    def flatMap[E1 >: E, B](f: A => IO[E1, B]): IO[E1, B] = foldM(fail, f)
    def catchAll[F, A1 >: A](f: E => IO[F, A1]): IO[F, A1] = foldM(f, succeed)
    def map[B](f: A => B): IO[E, B] = flatMap(a => succeed(f(a)))
    def mapError[E1](f: E => E1): IO[E1, A] = catchAll(e => fail(f(e)))
    def zip[E1 >: E, B](other: IO[E1, B]): IO[E1, (A, B)] = flatMap(a => other.map(b => (a, b)))
    def fold[B](f: E => B, g: A => B): IO[Nothing, B] = foldM(e => succeed(f(e)), a => succeed((g(a))))

    def race[E1 >: E, A1 >: A](other: IO[E1, A1]): IO[E1, A1] =
      for {
        fb0 <- fork
        fb1 <- other.fork
        a   <- fb0.raceAll(List(fb1))
      }
      yield a

    def raceAll[E1 >: E, A1 >: A](others: Iterable[IO[E1, A1]]): IO[E1, A1] =
      for {
        fb0 <- fork
        fbs <- foreach(others)(_.fork)
        a   <- fb0.raceAll(fbs)
      }
      yield a

    def ensuring(finalize: IO[Nothing, Any]): IO[E, A] =
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
  
    def foldM[E1, A1](f: E => IO[E1, A1], g: A => IO[E1, A1]): IO[E1, A1] = FoldM(this, f, g)

    def fork: IO[Nothing, Fiber[E, A]] = 
      for {
        fb <- Sync(cx => if(cx.isAlive) cx.fiber.fork(this) else cx.fiber.idle)
        _  <- Sync(cx => Pure(runFiber(fb, cx.platform)))
      }
      yield fb

    case Pure(a: A)
    case Error(e: E)
    case Sync(run: Context => IO[E, A])
    case Async(run: (Context, IO[E, A] => Unit) => Unit)
    case FoldM[E1, A1, E2, A2](ea: IO[E1, A1], f: E1 => IO[E2, A2], g: A1 => IO[E2, A2]) extends IO[E2, A2]
    case MapC(f: Context => Context, ea: IO[E, A])
  }

  import IO._
  import Exit._

  final case class Context(platform: Platform, fiber: Fiber[Any, Any], mask: Boolean) {
    def masked = copy(mask=true)
    def isAlive = mask || fiber.isAlive

    def attempt[A](effect: => A): IO[Throwable, A] = 
      try Pure(effect) 
      catch { 
        case t if ! platform.fatal(t) => Error(t) 
        case t => platform.shutdown(t)
      }

    def sandbox(cont: => Unit): Unit =
      try cont
      catch {
        case t if ! platform.fatal(t) => fiber.die(t)
        case t => platform.shutdown(t)            
      }

    def block[A](effect: => A): IO[Throwable, A] = 
      platform.executeBlocking(attempt(effect))

    def shift(cont: => Unit): Unit =
      platform.executeAsync(sandbox(cont))
  }

  private def runFiber[E, A](fiber: Fiber[E, A], platform: Platform): Unit = {

    def push[E, A](cx: Context, ea: IO[E, A], dx: Int, ke: (E, Int) => Unit, ka: (A, Int) => Unit): Unit =
      if(dx < recurLimit) loop(cx, ea, dx+1, ke, ka)
      else shift(cx, ea, ke, ka)

    def shift[E, A](cx: Context, ea: IO[E, A], ke: (E, Int) => Unit, ka: (A, Int) => Unit): Unit =
      cx.shift(loop(cx, ea, 0, ke, ka))

    @tailrec
    def loop[E, A](cx: Context, ea: IO[E, A], dx: Int, ke: (E, Int) => Unit, ka: (A, Int) => Unit): Unit = 
      ea match {
        case Pure(a)          => ka(a, dx)
        case Error(e)         => ke(e, dx)
        case Sync(run)        => loop(cx, run(cx), dx, ke, ka)
        case Async(run)       => run(cx, shift(cx, _, ke, ka))
        case FoldM(ea1, f, g) => loop(cx, ea1, dx, 
          (e, dx) => push(cx, f(e), dx, ke, ka), 
          (a, dx) => push(cx, g(a), dx, ke, ka))
        case MapC(f, ea1)     => loop(f(cx), ea1, dx, ke, ka)
      }

    shift(Context(platform, fiber, false), fiber.start, (_, _) => (), (_, _) => ())
  }

  private val recurLimit = 100

  def succeed[A](a: A): IO[Nothing, A] = Pure(a)
  def fail[E](e: E): IO[E, Nothing] = Error(e)
  def effect[A](effect: => A): IO[Throwable, A] = Sync(_.attempt(effect))
  def effectTotal[A](effect: => A): IO[Nothing, A] = Sync(_ => Pure(effect))

  def effectBlocking[A](effect: => A): IO[Throwable, A] = 
    Sync(cx => 
      if(cx.isAlive) {
        val ea = cx.block(effect)
        if(cx.isAlive) ea else cx.fiber.idle
      }
      else cx.fiber.idle
    )

  def effectAsync[E, A](register: (IO[E, A] => Unit) => Any): IO[E, A] = 
    Async((cx, k) =>       
      if(cx.isAlive) register(ea => k(if(cx.isAlive) ea else cx.fiber.idle))
      else k(cx.fiber.idle)
    )

  def flatten[E, A](suspense: IO[E, IO[E, A]]): IO[E, A] =
      suspense.flatMap(identity)

  def effectSuspend[A](suspense: => IO[Throwable, A]): IO[Throwable, A] =
    flatten(effect(suspense))

  def effectSuspendTotal[E, A](suspense: => IO[E, A]): IO[E, A] =
    flatten(effectTotal(suspense))

  def foreach[E, A, B](as: Iterable[A])(f: A => IO[E, B]): IO[E, List[B]] =
    as.foldRight[IO[E, List[B]]](succeed(Nil)) { (a, ebs) => 
      for { 
        b  <- f(a) 
        bs <- ebs
      } 
      yield b :: bs
    }

  val interrupt: IO[Nothing, Nothing]            = Sync(cx => cx.fiber.interruptFork.andThen(cx.fiber.idle))
  def die(t: => Throwable): IO[Nothing, Nothing] = Sync(cx => cx.fiber.die(t).andThen(cx.fiber.idle))
  def mask[E, A](ea: IO[E, A]): IO[E, A]         = MapC(_.masked, ea)
  val check: IO[Nothing, Unit]                   = Sync(cx => if(cx.isAlive) unit else cx.fiber.idle)
  val never: IO[Nothing, Nothing]                = Sync(_.fiber.idle)

  lazy val defaultRuntime = new Runtime( Platform.default, runFiber )
}
