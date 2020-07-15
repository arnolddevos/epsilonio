package test.minio
import minio.api2._
import probably._
import scala.concurrent.duration._

object Main extends App {
  val test = Runner()
  val rt = defaultRuntime


  println(s"using runtime $rt")

  def testf[A, B](name: String)(value: A, expect: A => B)(article: A => IO[Nothing, B]) =
    test(name, trial=s"value=$value"){
      rt.unsafeRunSync(article(value)).option
    }.assert {
      case Some(b) if expect(value) == b => true
      case _ => false
    }

  test("an OOM error would be fatal") {
    rt.platform.fatal(new OutOfMemoryError())
  }.assert(x => x)

  test("evaluate a pure success") {
    rt.unsafeRunSync(succeed(42)).option
  }.assert {
    case Some(42) => true
    case _        => false
  }

  test("evaluate a pure failure") {
    rt.unsafeRunSync(fail(42))
  }.assert {
    ! _.succeeded
  }

  test("run a total effect") {
    var x: Int = 0
    rt.unsafeRunSync(effectTotal{x = 42})
    x
  }.assert {
    _ == 42
  }

  test.async[Option[Int]]("unsafeRunAsync produces a value") {
    promise =>
      rt.unsafeRunAsync(succeed(42))(ex => promise.success(ex.option))
  }.assert { 
    case Some(42) => true
    case _        => false
  }

  test("flatMap pure effects") {
    rt.unsafeRunSync(succeed(42).flatMap(n => succeed(n-1))).option
  }.assert {
    case Some(41) => true
    case _        => false
  }

  testf("traverse a list of effects")(Iterable(1, 2, 3), identity) {
    xs => foreach(xs)(succeed)
  }

  testf("map effect")(2, _ + 6) {
    n => succeed(n).map(_ + 6)
  }

  testf("zip effects")(("the", 2), identity) {
    (s, n) => succeed(s).zip(succeed(n))
  }

  testf("exercise a queue")("payload", identity) {
    x =>
      for {
        q <- effectTotal(queue[String](10))
        _ <- q.offer(x)
        y <- q.take
      }
      yield y
  }

  testf("exercise a queue little harder")("payload", identity) {
    x =>
      for {
        q <- effectTotal(queue[String](10))
        _ <- q.offer("")
        _ <- q.offer("")
        _ <- q.offer(x)
        _ <- q.take
        _ <- q.take
        y <- q.take
      }
      yield y
  }

  testf("exercise just enqueue ops")(Iterable(1, 2, 3), xs => xs.map(_ => ())) {
    xs =>
      for {
        q <- effectTotal(queue[Int](10))
        ys <- foreach(xs)(q.offer)
      }
      yield ys
  }

  testf("exercise a queue harder")(Iterable(1, 2, 3, 5, 6), identity) {
    xs =>
      for {
        q <- effectTotal(queue[Int](10))
        _ <- foreach(xs)(q.offer)
        ys <- foreach(xs)(_ => q.take)
      }
      yield ys
  }

  testf("producer and consumer")(Iterable(1, 2, 3), identity) {
    xs =>
      for {
        q  <- effectTotal(queue[Int](10))
        c  <- foreach(xs)(_ => q.take).fork
        _  <- foreach(xs)(q.offer)
        ys <- c.join
      }
      yield ys
  }

  println(test.report().formatted)
}
