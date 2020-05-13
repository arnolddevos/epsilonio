package minio
import scala.annotation._
 
enum Tail[-A] {
  case Continue(step: A => Tail[A])
  case Mask(tail: Tail[A])
  case Unmask(tail: Tail[A])
  case Check(tail: Tail[A])
  case Context(a: A, live: A => Boolean, recover: Throwable => Tail[A], tail: Tail[A]) extends Tail[Any]
  case Fork(child: Tail[Any], tail: Tail[A]) extends Tail[A]
  case Async(linkage: (Tail[A] => Unit) => Any)
  case Blocking(tail: Tail[A])
  case Shift(tail: Tail[A])
  case CheckShift(tail: Tail[A])
  case Catch[A, X](effect: () => X, tail: X => Tail[A], recover: Throwable => Tail[A]) extends Tail[A]
  case Stop
}

object Tail {

  def run(tail: Tail[Any], platform: Platform): Unit = {
    boot(tail)

    def boot(tail: Tail[Any]) = context((), _ => true, t => throw t, tail)

    def context[A](a: A, live: A => Boolean, recover: Throwable => Tail[A], tail: Tail[A]): Unit = {
      sandbox(0, tail)

      def sandbox(masks: Int, tail: Tail[A]): Unit =
        try loop(masks, tail)
        catch {
          case t if ! platform.fatal(t) => context(a, live, platform.shutdown(_), recover(t))
          case t => platform.shutdown(t)
        }

      def attempt[X](x: => X) = 
        try Right(x)
        catch {
          case t if ! platform.fatal(t) => Left(t)
          case t => platform.shutdown(t)
        }
    
      @tailrec 
      def loop(masks: Int, next: Tail[A]): Unit = {

        next match {

          case Continue(step)     =>  loop(masks, step(a))
          case Context(a, live, recover, tail)
                                  =>  context(a, live, recover, tail)
          case Mask(tail)         =>  loop(masks+1, tail)
          case Unmask(tail)       =>  loop(masks-1, tail)
          case Check(tail)        =>  if(masks > 0 || live(a)) 
                                        loop(masks, tail)
          case Fork(child, tail)  =>  boot(child)
                                      loop(masks, tail)
          case Async(linkage)     =>  linkage(sandbox(masks, _))
          case Blocking(tail)     =>  platform.executeBlocking(sandbox(masks, tail))
          case Shift(tail)        =>  platform.executeAsync(sandbox(masks, tail))
          case CheckShift(tail)   =>  if(masks > 0 || live(a)) 
                                        platform.executeAsync(sandbox(masks, tail))
          case Catch(effect, tail, recover) 
                                  =>  loop(masks, attempt(effect()).fold(recover, tail))
          case Stop               => ()
        }
      }
    }
  }
}