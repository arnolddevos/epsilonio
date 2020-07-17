package test.minio
import minio.api2._
import probably._
import scala.concurrent._
import scala.concurrent.duration._
import scala.collection.mutable.Buffer

object Main extends App {
  val test = Runner()
  val rt = defaultRuntime
  val pl = rt.platform


  println(s"using runtime $rt main thread is ${Thread.currentThread.getId}")

  test("an OOM error would be fatal") {
    pl.fatal(new OutOfMemoryError())
  }.assert(x => x)

  test.suite("IO tests", repeat=10) { test =>

    def testf[A, B](name: String)(value: A, expect: A => B)(article: A => IO[Nothing, B]) =
      test(name, trial=s"value=$value"){
        rt.unsafeRunSync(article(value)).option
      }.assert {
        case Some(b) if expect(value) == b => true
        case _ => false
      }
    
    testf("evaluate a pure success")(42, identity) {
      succeed(_)
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

    testf("flatMap pure effects")(42, _ - 1) {
      x => succeed(x).flatMap(n => succeed(n-1))
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
          _ <- q.take
          _ <- q.offer(x)
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

    testf("producer and consumer")(1 to 1000, identity) {
      xs =>
        for {
          q  <- effectTotal(queue[Int](10))
          c  <- foreach(xs)(_ => q.take).fork
          _  <- foreach(xs)(q.offer)
          ys <- c.join
        }
        yield ys
    }
  }

  test.suite("Tail tests", repeat=10) { test =>
    import minio.Tail
    import Tail._

    def tid() = Thread.currentThread.getId

    test.async("the platform can execute a thunk asynchronously") {
      p => pl.executeAsync(p.success(tid()))
    }.assert(_ != tid())

    class TestEnv[-A](buffer: Buffer[A], promise: Promise[Buffer[A]]) {
      def stop: Unit = promise.success(buffer)
      def log(a: => A): Unit = buffer += a
    }

    def log[A](a: => A): Tail[TestEnv[A]] = Access(e => Catch(() => e.log(a), stop, stop))
    def stop: Any => Tail[TestEnv[Nothing]] = _ => Access(e => Catch(() => e.stop, _ => Stop, _ => Stop))

    def testt[A](name: String)(a: A)(t: Tail[TestEnv[A]])(p: Buffer[A] => Boolean) =
      test.async(name, trial=a.toString) { 
        (pr: Promise[Buffer[A]]) =>
          val e = TestEnv(Buffer(a), pr)
          Tail.run(Provide(e, t), pl)
      }.assert(p)

    testt("a successful effect")(0) {
      Access( e => Catch({() => e.log(1); 2}, log, stop))
    } {
      case Buffer(0, 1, 2) => true
      case _ => false
    }

    testt("a throwing effect")(new Throwable()) {
      Catch(() => throw Exception("expected"), stop, log)
    } {
      case Buffer(_, ex) if ex.getMessage == "expected" => true
      case _ => false
    }

    testt("interrupt an effect")(0) {
      Check(_ => false, log(1), log(2))
    } {
      case Buffer(0, 2) => true
      case _ => false
    }

    testt("mask an interrupt")(0) {
      Mask(Check(_ => false, log(1), log(2)))
    } {
      case Buffer(0, 1) => true
      case _ => false
    }

    testt("unmask an interrupt")(0) {
      Mask(Unmask(Check(_ => false, log(1), log(2))))
    } {
      case Buffer(0, 2) => true
      case _ => false
    }

    testt("shift an effect to another thread")(tid()) {
      Access( e => Catch(() => e.log(tid()), _ => Shift(log(tid()), stop), stop))
    } {
      case Buffer(_, t2, t3) if t2 != t3 => true
      case _ => false
    }
  }

  println(test.report().formatted)
}
