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

  def run(a: A, platform: Platform, mask: Interrupts): Unit = {
    loop(a, mask, this)

    @tailrec 
    def loop[A](a: A, mask: Interrupts, next: Tail[A]): Unit = 
      next match {

        case Continue(step)           => loop(a, mask, step(a))
        case ContraMap(f, tail)       => loop(f(a), mask, tail)
        case Mask(tail)               => loop(a, Interrupts.Off, tail)
        case Check(live, tail)        => if(mask == Interrupts.Off || live(a)) loop(a, mask, tail)
        case Fork(child, start, tail) => start.run(child, platform, Interrupts.On); loop(a, mask, tail)
        case Async(linkage)           => linkage(tail => tail.run(a, platform, mask))
        case Blocking(tail)           => platform.executeBlocking(tail.run(a, platform, mask))
        case Shift(tail)              => platform.executeAsync(tail.run(a, platform, mask))
        case Stop                     => ()

        case Catch(tail, recover)     =>
          try { tail().run(a, platform, mask) } 
          catch { 
            case t if platform.fatal(t) => platform.shutdown(t)
            case t                      => loop(a, mask, recover(t))
          }

        case CheckShift(live, tail, recover) =>
          if(mask == Interrupts.Off || live(a)) 
            platform.executeAsync(
              try { tail().run(a, platform, mask) } 
              catch { 
                case t if platform.fatal(t) => platform.shutdown(t)
                case t                      => recover(t).run(a, platform, mask)
              }
            )
      }
  }
}