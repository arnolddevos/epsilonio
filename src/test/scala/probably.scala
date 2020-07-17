package probably

import scala.util.{Try, Success, Failure}
import scala.concurrent.{Promise, Future, Await}
import scala.concurrent.duration._
import scala.collection.immutable.ListMap

class Runner(val asserts: Boolean=true, indent: Int=0, repeat: Int=1) { test =>
  def apply[A](name: String, trial: => String="")(action: => A) = Test(name, () => trial, () => action)

  class Test[A](val name: String, trial: () => String, action: () => A) {

    def attempt(predicate: A => Boolean): Try[A] = {

      @annotation.tailrec
      def loop(n: Int): Try[A] = {
      val t0 = System.currentTimeMillis
      val ta = Try(action())
      val td = System.currentTimeMillis - t0

      import Outcome._

      val outcome = ta match {
        case Success(a) => 
          Try(predicate(a)) match {
            case Success(true)  => Passed
            case Success(false) => Failed(trial())
            case Failure(e)     => CheckThrows(trial(), e)
          }
        case Failure(e)         => TestThrows(trial(), e)
      }

      val nf = if(outcome.passed) 0 else 1
      record(Summary(name, indent, 1, nf, 0, td, td, td, outcome))
        if(n <= 1) ta else loop(n-1)
    }

      loop(repeat)
    }

    def check(predicate: A => Boolean): A =
      attempt(predicate).get

    def assert(predicate: A => Boolean): Unit =
      if(asserts) attempt(predicate)
  }

  def async[A](name: String, trial: => String="", timeout: Duration=10.milli)(action: Promise[A] => Unit) = {
    def article = {
      val p = Promise[A]()
      action(p)
      val f = p.future
      Await.ready(f, timeout)
      f.value.get.get
    }
    Test(name, () => trial, () => article)
  }

  def suite(name: String, asserts: Boolean=asserts, repeat: Int=1)(action: Runner => Unit): Unit = {
    val report = test(name) {
      val runner = Runner(asserts=asserts, indent=indent+1, repeat=repeat)
      action(runner)
      runner.report()  
    }.check(_.passed)

    report.results.foreach(record)
  }

  @volatile
  private var results = ListMap[String, Summary]()

  private def record(s: Summary): Unit = synchronized {
    def slog = { println(s"Found test: ${s.name}"); s }
    results = results.updated(s.name, results.get(s.name).fold(slog)(_.merge(s)))
  }

  def report(): Report = Report(results.values.toList)

}

enum Outcome {
  case Passed
  case Failed(trial: String)
  case TestThrows(trial: String, error: Throwable)
  case CheckThrows(trial: String, error: Throwable)

  def merge(other: Outcome) = 
    this match {
      case Passed => other
      case _      => this
    }

  def passed = 
    this match {
      case Passed => true
      case _      => false
    }
}

case class Summary(
  name: String,    // name of the test
  indent: Int,     // nesting depth of the test in suites
  count: Int,      // number of trials
  fails: Int,      // number of failures
  index: Int,      // index of the the test that produced outcome
  tmin: Long,      // minimum of test durations
  ttot: Long,      // total of test durations
  tmax: Long,      // maximum of the test durations
  outcome: Outcome // the first failure or `Pass`
) {
  def merge(other: Summary) =
    Summary(
      name,
      indent, 
      count+other.count, 
      fails+other.fails,
      if(outcome.passed) count+other.index else index,
      tmin.min(other.tmin), 
      ttot + other.ttot, 
      tmax.max(other.tmax), 
      outcome.merge(other.outcome)
    )

  def avg: Double = ttot.toDouble/count/1000.0
  def min: Double = tmin.toDouble/1000.0
  def max: Double = tmax.toDouble/1000.0
}

case class Report(results: List[Summary]) {
  def passed = results.forall(_.outcome.passed)
  def formatted: String = results.map(detail).mkString("\n")
    
  private def detail(s: Summary): String = {
    import Outcome._
    import s._

    val spaces1 = " "*(indent+1)
    val spaces2 = " "*(40-name.size-indent).max(1)
    val symbol =
      outcome match {
        case Passed            => "P"
        case Failed(_)         => "F"
        case TestThrows(_, _)  => "!"
        case CheckThrows(_, _) => "?"
      }
    val debug =
      outcome match {
        case Passed                 => ""
        case Failed(trial)          => trial
        case TestThrows(trial, ex)  => s"$trial ${ex.getMessage}"
        case CheckThrows(trial, ex) => s"$trial ${ex.getMessage}"
      }
    val reset = Console.RESET
    val black = Console.BLACK
    val (color, backg) = 
      outcome match {
        case Passed            => (Console.GREEN, Console.GREEN_B)
        case Failed(_)         => (Console.RED, Console.RED_B)
        case TestThrows(_, _)  => (Console.YELLOW, Console.YELLOW_B)
        case CheckThrows(_, _) => (Console.YELLOW, Console.YELLOW_B)
      }
  
    f"$backg$black $symbol $reset$spaces1$color$name$reset$spaces2$min%1.3f $avg%1.3f $max%1.3f $debug"
  }
}
