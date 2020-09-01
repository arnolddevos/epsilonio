package minio

trait Structure extends Signature { 

  enum IO[+E, +A] extends IOops[E, A] {

    case Succeed(a: A)
    case Fail(e: E)

    case EffectTotal(a: () => A)
    case Effect(a: () => A) extends IO[Throwable, A]
    case EffectBlocking(a: () => A) extends IO[Throwable, A]
    case EffectAsync(register: (IO[E, A] => Unit) => Any)
    case EffectSuspend(a: () => IO[E, A])

    case FlatMap[E, A, B](a: IO[E, A], f: A => IO[E, B]) extends IO[E, B]
    case Map[E, A, B](a: IO[E, A], f: A => B) extends IO[E, B]
    case CatchAll[E, A, F](e: IO[E, A], f: E => IO[F, A]) extends IO[F, A]

    case Fork(a: IO[E, A]) extends IO[Nothing, Fiber[E, A]]
    case Mask(a: IO[E, A])
    case Interrupt()
    case Die(t: () => Throwable)
    case Check() extends IO[Nothing, Unit]
    case Never()

    def flatMap[E1 >: E, B](f: A => IO[E1, B]): IO[E1, B] = 
      this match {
        case FlatMap(x, g)  => FlatMap(x, y => g(y).flatMap(f))
        case EffectTotal(a) => EffectSuspend(() => f(a()))
        case Map(x, g)      => x.flatMap(y => f(g(y)))
        case Succeed(a)     => f(a)
        case Fail(e)        => Fail(e)
        case _              => FlatMap(this, f)
      }
      
    def catchAll[F, A1 >: A](f: E => IO[F, A1]): IO[F, A1] = CatchAll(this, f)
    
    def map[B](f: A => B): IO[E, B] = 
      this match {
        case EffectTotal(a)   => EffectTotal(() => f(a()))
        case Map(x, g)        => Map(x, y => f(g(y)))
        case Succeed(a)       => Succeed(f(a))
        case Fail(e)          => Fail(e)
        case _                => Map(this, f)
      }

    def mapError[E1](f: E => E1): IO[E1, A] = catchAll(e => fail(f(e)))
    def zip[E1 >: E, B](other: IO[E1, B]): IO[E1, (A, B)] = flatMap(a => other.map(b => (a, b)))
    def fold[B](f: E => B, g: A => B): IO[Nothing, B] = map(g).catchAll(e => succeed(f(e)))
    def fork: IO[Nothing, Fiber[E, A]] = Fork(this)

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

  import IO._

  def succeed[A](a: A): IO[Nothing, A] = Succeed(a)
  def fail[E](e: E): IO[E, Nothing] = Fail(e)

  def effectTotal[A](effect: => A): IO[Nothing, A] = EffectTotal(() => effect)
  def effect[A](effect: => A): IO[Throwable, A] = Effect(() => effect)
  def effectBlocking[A](effect: => A): IO[Throwable, A] = EffectBlocking(() => effect)
  def effectAsync[E, A](register: (IO[E, A] => Unit) => Any): IO[E, A] = EffectAsync(register)
  def flatten[E, A](suspense: IO[E, IO[E, A]]): IO[E, A] = suspense.flatMap(ea => ea)
  def effectSuspend[A](suspense: => IO[Throwable, A]): IO[Throwable, A] = flatten(effect(suspense))
  def effectSuspendTotal[E, A](suspense: => IO[E, A]): IO[E, A] = EffectSuspend(() => suspense)

  def effectAsyncMaybe[E, A](run: (IO[E, A] => Unit) => Option[IO[E, A]]): IO[E, A] = 
    effectAsync(k =>
      run(k) match {
        case Some(ea) => k(ea)
        case None     => ()
      }
    )
  
  def foreach[E, A, B](as: Iterable[A])(f: A => IO[E, B]): IO[E, List[B]] =
    as.foldRight[IO[E, List[B]]](succeed(Nil)) { (a, ebs) => 
      for { 
        b  <- f(a) 
        bs <- ebs
      } 
      yield b :: bs
    }

  def interrupt: IO[Nothing, Nothing] = Interrupt()
  def die(t: => Throwable): IO[Nothing, Nothing] = Die(() => t)
  def mask[E, A](ea: IO[E, A]): IO[E, A] = Mask(ea)
  def check: IO[Nothing, Unit] = Check()
  def idle: IO[Nothing, Nothing] = Never()
}
