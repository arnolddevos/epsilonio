package minio
import scala.annotation._
 
enum Tail[-A] {
  case Continue(step: A => Tail[A])
  case ContraMap[A, B](f: A => B, tail: Tail[B]) extends Tail[A]
  case Mask(tail: Tail[A])
  case Unmask(tail: Tail[A])
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
    loop((), 0, tail)

    def rentry[A](a: A, masks: Int, next: Tail[A]): Unit = loop(a, masks, next)

    @tailrec 
    def loop[A](a: A, masks: Int, next: Tail[A]): Unit = {

      def attempt(tail: () => Tail[A], recover: Throwable => Tail[A]): Unit =
        try rentry(a, masks, tail()) 
        catch {
          case t if platform.fatal(t) => platform.shutdown(t)
          case t                      => rentry(a, masks, recover(t))
      }

      next match {

        case Continue(step)           => loop(a, masks, step(a))
        case ContraMap(f, tail)       => loop(f(a), masks, tail)
        case Mask(tail)               => loop(a, masks+1, tail)
        case Unmask(tail)             => loop(a, masks-1, tail)
        case Check(live, tail)        => if(masks > 0 || live(a)) loop(a, masks, tail)
        case Fork(child, start, tail) => rentry(child, 0, start); loop(a, masks, tail)

        case Async(linkage)           => linkage(rentry(a, masks, _))
        case Blocking(tail)           => platform.executeBlocking(rentry(a, masks, tail))
        case Shift(tail)              => platform.executeAsync(rentry(a, masks, tail))
        case Catch(tail, recover)     => attempt(tail, recover)
        case Stop                     => ()

        case CheckShift(live, tail, recover) =>
          if(masks > 0 || live(a)) 
            platform.executeAsync(attempt(tail, recover))
      }
    }
  }
}