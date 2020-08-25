package test.minio
import minio.api3._
import probably._
import scala.concurrent._
import scala.concurrent.duration._
import scala.collection.mutable.Buffer

object Main extends App {
  val test = Runner()
  val rt = defaultRuntime
  val pl = rt.platform

  def [A, B](test: Runner).io(name: String)(value: A, expect: A => B)(article: A => IO[Nothing, B]) =
    test(name, trial=s"value=$value"){
      defaultRuntime.unsafeRunSync(article(value)).option
    }.assert {
      case Some(b) if expect(value) == b => true
      case _ => false
    }

  println(s"using runtime $rt main thread is ${Thread.currentThread.getId}")

  test("an OOM error would be fatal") {
    pl.fatal(new OutOfMemoryError())
  }.assert(x => x)

  test.suite("IO tests", repeat=10) { test =>

    test.io("evaluate a pure success")(42, identity) {
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

    test.io("flatMap pure effects")(42, _ - 1) {
      x => succeed(x).flatMap(n => succeed(n-1))
    }

    test.io("traverse a list of effects")(Iterable(1, 2, 3), identity) {
      xs => foreach(xs)(succeed)
    }

    test.io("map effect")(2, _ + 6) {
      n => succeed(n).map(_ + 6)
    }

    test.io("zip effects")(("the", 2), identity) {
      (s, n) => succeed(s).zip(succeed(n))
    }

    test.io("exercise a queue")("payload", identity) {
      x =>
        for {
          q <- effectTotal(queue[String](10))
          _ <- q.offer(x)
          y <- q.take
        }
        yield y
    }

    test.io("exercise a queue little harder")("payload", identity) {
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

    test.io("exercise just enqueue ops")(Iterable(1, 2, 3), xs => xs.map(_ => ())) {
      xs =>
        for {
          q <- effectTotal(queue[Int](10))
          ys <- foreach(xs)(q.offer)
        }
        yield ys
    }

    test.io("exercise a queue harder")(Iterable(1, 2, 3, 5, 6), identity) {
      xs =>
        for {
          q <- effectTotal(queue[Int](10))
          _ <- foreach(xs)(q.offer)
          ys <- foreach(xs)(_ => q.take)
        }
        yield ys
    }

    test.io("producer and consumer")(1 to 1000, identity) {
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

    test.async("the platform runs asynchronous thunks") {
      p => pl.executeAsync(p.success(tid()))
    }.assert(_ != tid())

    class TestEnv[-A](buffer: Buffer[A], promise: Promise[Buffer[A]]) {
      def stop: Unit = promise.success(buffer)
      def log(a: => A): Unit = buffer += a
    }

    def log[A](a: => A): Tail[TestEnv[A]] = Access(e => Catch(() => e.log(a), stop, stop))
    def stop: Any => Tail[TestEnv[Nothing]] = _ => Access(e => Catch(() => e.stop, _ => Stop, _ => Stop))

    def [A](test: Runner).tail(name: String)(a: A)(t: Tail[TestEnv[A]])(p: Buffer[A] => Boolean) =
      test.async(name, trial=a.toString) { 
        (pr: Promise[Buffer[A]]) =>
          val e = TestEnv(Buffer(a), pr)
          Tail.run(Provide(e, t), pl)
      }.assert(p)

    test.tail("a successful effect")(0) {
      Access( e => Catch({() => e.log(1); 2}, log, stop))
    } {
      case Buffer(0, 1, 2) => true
      case _ => false
    }

    test.tail("a throwing effect")(new Throwable()) {
      Catch(() => throw Exception("expected"), stop, log)
    } {
      case Buffer(_, ex) if ex.getMessage == "expected" => true
      case _ => false
    }

    test.tail("an asynchonous effect")(0) {
      Async(cb => cb(log(1)))
    } {
      case Buffer(0, 1) => true
      case _ => false
    }

    test.tail("interrupt an effect")(0) {
      Check(_ => false, log(1), log(2))
    } {
      case Buffer(0, 2) => true
      case _ => false
    }

    test.tail("mask an interrupt")(0) {
      Mask(Check(_ => false, log(1), log(2)))
    } {
      case Buffer(0, 1) => true
      case _ => false
    }

    test.tail("unmask an interrupt")(0) {
      Mask(Unmask(Check(_ => false, log(1), log(2))))
    } {
      case Buffer(0, 2) => true
      case _ => false
    }

    test.tail("fork an effect")(0) {
      Access(e => Fork(Provide(e, log(1)), log(2)))
    } {
      case Buffer(0, 1, 2) => true
      case _ => false
    }

    test.tail("shift an effect to another thread")(tid()) {
      Access( e => Catch(() => e.log(tid()), _ => Shift(log(tid()), stop), stop))
    } {
      case Buffer(_, t2, t3) if t2 != t3 => true
      case _ => false
    }

    test.tail("a blocking effect (5ms)")(0) {
      Blocking(Catch(() =>Thread.sleep(5), _ => log(1), stop))
    } {
      case Buffer(0, 1) => true
      case _ => false     
    }
  }

  test.suite("Node tests", repeat=10){ test =>
    import minio.nodes._
    import Supervisory._

    def run(e: IO[Nothing, Any]) = defaultRuntime.unsafeRunAsync(e)(_ => ())

    test.async[Int]("start an isolated node") { p =>
      val sys = System[Nothing, Unit]
      new Node(sys) {
        def action = effectTotal(p.success(42)).unit
      }
      run(sys.start)
    }.assert {
      _ == 42
    }

    test.async[String]("pass a message between nodes") { p => 
      val sys = System[Nothing, Unit]
      val q1 = queue[String](10)
      new Node(sys) with Output(q1) {
        def action = output("Hello world!")
      }
      new Node(sys) with Input(q1) {
        def action = react { m => effectTotal(p.success(m)).unit }
      }
      run(sys.start)
    }.assert {
      _ == "Hello world!"
    }

    test.async[String]("start a supervised node", timeout=20.milli) { p =>
      val sys = System[Throwable, Unit]
      new Supervisor(sys) {
        def action = react { 
          case Started(n, f) => 
            react {
              case Stopped(`n`, `f`, x) if x.succeeded => 
                effectTotal(p.success("OK!")).unit
              case Stopped(n, f, x) => 
                effectTotal(p.success(s"Nah. Stop parameters unexpected: $n, $f, $x")).unit
              case _ => 
                effectTotal(p.success("Nah. Second startup.")).unit
            }
          case x => 
            effectTotal(p.success(s"Nah. Stopped before start. $x")).unit
        }
      }
      new Node(sys) {
        def action = succeed(())
      }
      run(sys.start)
    }.assert {
      v =>
        println(v)
        v == "OK!"
    }

  }

  println(test.report().formatted)
}
