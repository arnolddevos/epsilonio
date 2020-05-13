package minio
import scala.annotation._

enum Interrupts {
  case On
  case Off
}
  
enum Tail[-A] {
  case Continue(step: A => Tail[A])
  case ContraMap[A, B](f: A => B, tail: Tail[B]) extends Tail[A]
  case Mask(tail: Tail[A])
  case Check(live: A => Boolean, tail: Tail[A])
  case Fork[A, B](child: B, start: Tail[B], tail: Tail[A]) extends Tail[A]
  case Async(linkage: (Tail[A] => Unit) => Any)
  case Blocking(tail: Tail[A])
  case Shift(tail: Tail[A])
  case CheckShift(live: A => Boolean, tail: () => Tail[A], recover: Throwable => Tail[A])
  case Catch(tail: () => Tail[A], recover: Throwable => Tail[A])
  case Stop
}

object Tail {

  def run(tail: Tail[Any], platform: Platform): Unit = {
    loop((), Interrupts.On, tail)

    def rentry[A](a: A, mask: Interrupts, next: Tail[A]): Unit = loop(a, mask, next)

    @tailrec 
    def loop[A](a: A, mask: Interrupts, next: Tail[A]): Unit = {

      def attempt(tail: () => Tail[A], recover: Throwable => Tail[A]): Unit =
        try rentry(a, mask, tail()) 
        catch {
          case t if platform.fatal(t) => platform.shutdown(t)
          case t                      => rentry(a, mask, recover(t))
      }

      next match {

        case Continue(step)           => loop(a, mask, step(a))
        case ContraMap(f, tail)       => loop(f(a), mask, tail)
        case Mask(tail)               => loop(a, Interrupts.Off, tail)
        case Check(live, tail)        => if(mask == Interrupts.Off || live(a)) loop(a, mask, tail)
        case Fork(child, start, tail) => rentry(child, Interrupts.On, start); loop(a, mask, tail)

        case Async(linkage)           => linkage(rentry(a, mask, _))
        case Blocking(tail)           => platform.executeBlocking(rentry(a, mask, tail))
        case Shift(tail)              => platform.executeAsync(rentry(a, mask, tail))
        case Catch(tail, recover)     => attempt(tail, recover)
        case Stop                     => ()

        case CheckShift(live, tail, recover) =>
          if(mask == Interrupts.Off || live(a)) 
            platform.executeAsync(attempt(tail, recover))
      }
    }
  }
}