package minio
import scala.annotation._

enum Tail[-A] {
  case Provide[A](a: A, tail: Tail[A]) extends Tail[Any]
  case Access(tail: A => Tail[A])
  case Mask(tail: Tail[A])
  case Unmask(tail: Tail[A])
  case Check(live: A => Boolean, tail: Tail[A], alt: Tail[A])
  case Push(child: Tail[A], tail: Tail[A])
  case Async(linkage: (Tail[A] => Unit) => Any)
  case Blocking(tail: Tail[A])
  case Shift(tail: Tail[A], fail: Throwable => Tail[A])
  case Effect[A, X](effect: () => X, tail: X => Tail[A], recover: Throwable => Tail[A]) extends Tail[A]
  case Stop extends Tail[Any]

  def contraMap[B](f: B => A): Tail[B] = Access(b => Provide(f(b), this))
  // def provide(a: A): Tail[Any] = Provide(a, this)
  // def mask: Tail[A] = Mask(this)
  // def unmask: Tail[A] = Unmask(this)
  // def check[A1 <: A](live: A1 => Boolean): Tail[A1] = Check(live, this)
  // def shift[A1 <: A](fail: Throwable => Tail[A1]): Tail[A1] = Shift(this, fail)
  // def blocking: Tail[A] = Blocking(this)
  // def fork(child: Tail[Any]): Tail[A] = Push(child, this)
}

object Tail {

  // def lazily[A](tail: => Tail[A]): Tail[A] = Access(_ => tail)

  def run(tail0: Tail[Any], platform: Platform): Unit = {
    reenter((), 0, tail0)

    def reenter[A](a: A, masks: Int, next: Tail[A]): Unit =
      try loop(a, masks, next)
      catch {
        case t => platform.shutdown(t)
      }

    def sandbox[A](a: A, masks: Int, tail: Tail[A], fail: Throwable => Tail[A]): Unit =
      try loop(a, masks, tail)
      catch {
        case t if ! platform.fatal(t) => reenter(a, masks, fail(t))
        case t => platform.shutdown(t)
      }

    def attempt[X](x: => X) = 
      try Right(x)
      catch {
        case t if ! platform.fatal(t) => Left(t)
        case t => platform.shutdown(t)
      }
  
    @tailrec 
    def loop[A](a: A, masks: Int, next: Tail[A]): Unit = {

      next match {

        case Provide(b, tail)   =>  loop(b, masks, tail)
        case Access(tail)       =>  loop(a, masks, tail(a))
        case Mask(tail)         =>  loop(a, masks+1, tail)
        case Unmask(tail)       =>  loop(a, masks-1, tail)
        case Check(live, tail, alt)  
                                =>  loop(a, masks, if(masks > 0 || live(a)) tail else alt)      
        case Push(child, tail)  =>  reenter(a, 0, child)
                                    loop(a, masks, tail)
        case Async(linkage)     =>  linkage(reenter(a, masks, _))
        case Blocking(tail)     =>  platform.executeBlocking(reenter(a, masks, tail))
        case Shift(tail, fail)  =>  platform.executeAsync(sandbox(a, masks, tail, fail))
        case Effect(effect, tail, recover) 
                                =>  loop(a, masks, attempt(effect()).fold(recover, tail))
        case Stop               => ()
      }
    }
  }
}