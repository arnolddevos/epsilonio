package probably

import scala.util.{Try, Success, Failure}
import scala.concurrent.{Promise, Future, Await}
import scala.concurrent.duration._
import scala.collection.immutable.ListMap

class Runner(val asserts: Boolean=true, indent: Int=0) { test =>
  def apply[A](name: String, trial: => String="")(action: => A) = Test(name, () => trial, () => action)

  def async[A](name: String, trial: => String="", timeout: Duration=1.milli)(action: Promise[A] => Unit) = {
    def article = {
      val p = Promise[A]()
      action(p)
      val f = p.future
      Await.ready(f, timeout)
      f.value.get.get
    }
    Test(name, () => trial, () => article)
  }

  class Test[A](val name: String, trial: () => String, action: () => A) {

    def attempt(predicate: A => Boolean): Try[A] = {

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
      ta
    }

    def check(predicate: A => Boolean): A =
      attempt(predicate).get

    def assert(predicate: A => Boolean): Unit =
      if(asserts) attempt(predicate)
  }

  def suite(name: String)(action: Runner => Unit): Unit = {
    val report = test(name) {
      val runner = new Runner(asserts, indent+1)
      action(runner)
      runner.report()  
    }.check(_.passed)

    report.results.foreach(record)
  }

  @volatile
  private var results = ListMap[String, Summary]()

  private def record(s: Summary): Unit = synchronized {
    results = results.updated(s.name, results.get(s.name).fold(s)(_.merge(s)))
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
  name: String,
  indent: Int,
  count: Int,
  fails: Int,
  index: Int,
  tmin: Long,
  ttot: Long,
  tmax: Long,
  outcome: Outcome
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
  
  def formatted: String = {
    import Outcome._

    val spaces1 = " "*(indent+1)
    val spaces2 = " "*(50-name.size).max(1)
    val symbol =
      outcome match {
        case Passed            => "P"
        case Failed(_)         => "F"
        case TestThrows(_, _)  => "X"
        case CheckThrows(_, _) => "C"
      }
    val debug =
      outcome match {
        case Passed                 => ""
        case Failed(trial)          => trial
        case TestThrows(trial, ex)  => s"$trial ${ex.getMessage}"
        case CheckThrows(trial, ex) => s"$trial ${ex.getMessage}"
      }
  
    s"$symbol$spaces1$name$spaces2$min $avg $max $debug"
  }
}

case class Report(results: List[Summary]) {
  def passed = results.forall(_.outcome.passed)
  def formatted: String = results.map(_.formatted).mkString("\n")
}
