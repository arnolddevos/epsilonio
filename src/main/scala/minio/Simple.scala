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
        fb <- Sync(_.fiber.fork(this))
        _  <- Sync(cx => Pure(runFiber(fb, cx.platform)))
      }
      yield fb

    case Pure(a: A)
    case Error(e: E)
    case Sync(run: Cx => IO[E, A])
    case Async(run: (Cx, IO[E, A] => Unit) => Unit)
    case FoldM[E1, A1, E2, A2](ea: IO[E1, A1], f: E1 => IO[E2, A2], g: A1 => IO[E2, A2]) extends IO[E2, A2]
    case Provide(f: Cx => Cx, ea: IO[E, A])
  }

  import IO._
  import Exit._

  final case class Cx(platform: Platform, fiber: Fiber[Any, Any], mask: Boolean) 

  private def runFiber[E, A](fiber: Fiber[E, A], platform: Platform): Unit = {

    def push[E, A](cx: Cx, ea: IO[E, A], dx: Int, ke: (E, Int) => Unit, ka: (A, Int) => Unit): Unit =
      if(dx < recurLimit) loop(cx, ea, dx+1, ke, ka)
      else shift(cx, ea, ke, ka)

    def shift[E, A](cx: Cx, ea: IO[E, A], ke: (E, Int) => Unit, ka: (A, Int) => Unit): Unit =
      platform.executeAsync(
        try loop(cx, ea, 0, ke, ka)
        catch {
          case t if ! cx.platform.fatal(t) => cx.fiber.die(t)
          case t => cx.platform.shutdown(t)            
        }
      )

    @tailrec
    def loop[E, A](cx: Cx, ea: IO[E, A], dx: Int, ke: (E, Int) => Unit, ka: (A, Int) => Unit): Unit = 
      ea match {
        case Pure(a)   => ka(a, dx)
        case Error(e)  => ke(e, dx)
        case Sync(run)  => loop(cx, run(cx), dx, ke, ka)
        case Async(run) => run(cx, push(cx, _, dx, ke, ka))
        case FoldM(x, f, g) => loop(cx, x, dx, 
          (e, dx) => push(cx, f(e), dx, ke, ka), 
          (a, dx) => push(cx, g(a), dx, ke, ka))
        case Provide(f, ea1) => loop(f(cx), ea1, dx, ke, ka)
      }

    shift(Cx(platform, fiber, false), fiber.start, (_, _) => (), (_, _) => ())
  }

  private val recurLimit = 100

  def succeed[A](a: A): IO[Nothing, A] = Pure(a)
  def fail[E](e: E): IO[E, Nothing] = Error(e)

  def effect[A](effect: => A): IO[Throwable, A] = 
    Sync(cx => 
      try Pure(effect) 
      catch { case t if ! cx.platform.fatal(t) => Error(t) }
    )

  def effectTotal[A](effect: => A): IO[Nothing, A] = 
    Sync(_ => Pure(effect))

  def effectBlocking[A](effect: => A): IO[Throwable, A] = 
    Async((cx, k) => 
      cx.platform.executeBlocking(
        try k(
          try Pure(effect) 
          catch { case t if ! cx.platform.fatal(t) => Error(t) }
        )
        catch { 
          case t if ! cx.platform.fatal(t) => cx.fiber.die(t)
          case t => cx.platform.shutdown(t)
        }
      )
    )

  def effectAsync[E, A](register: (IO[E, A] => Unit) => Any): IO[E, A] = 
    Async((cx, k) => 
      register(ea => 
        cx.platform.executeAsync(
          try k(ea)
          catch {
            case t if ! cx.platform.fatal(t) => cx.fiber.die(t)
            case t => cx.platform.shutdown(t)
          }
        )
      )
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

  val interrupt: IO[Nothing, Nothing] = Sync(_.fiber.interruptFork.andThen(never))
  def die(t: => Throwable): IO[Nothing, Nothing] = Sync(_.fiber.die(t).andThen(never))
  def mask[E, A](ea: IO[E, A]): IO[E, A] = Provide(_.copy(mask=true), ea)
  val check: IO[Nothing, Unit] = Sync(cx => if(cx.fiber.isAlive) unit else never)
  val never: IO[Nothing, Nothing] = effectAsync(_ => ())


  lazy val defaultRuntime = new Runtime( Platform.default, runFiber )
}

