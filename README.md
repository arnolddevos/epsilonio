# epsilonIO 

An IO monad borrowing from flowlib and ZIO but with fewer features.  Highlights:

* Typed error channel.
* Fibers, racing and interruption.
* Similar in use to basic ZIO.
* Transactor for non-blocking concurrent data structures.
* compact implementation in scala 3.

## Why?

* As an update to flowlib.
* ZIO for minimalists.
* As a tutorial (because the code is compact).
* As a test bed for a scala 3 effects framework.

## Test Bed

Here are some of the scala 3 features that might change how we do effects:

* `enum` and GADT's.  These are safe and easy to use in scala 3. In this first iteration, IO is implemented as `enum IO[+E, +A] { ... }` and the interpreter loops over a pattern match. The scala 3 type deductions are almost psychic.

* `given` and context functions.  These provide the equivalent of a Reader monad. Perhaps they can supplant the environment features of ZIO.  Challenge: There is plenty of skepticism about any use of givens beyond coherent type classes.

* meta-programing and, specifically, staging. Can the IO interpreter be replaced with a staged program?  Challenge: This seems more promising for an applicative EDSL than a monad.  

## Usage

In the base directory of a dotty 0.24.0-RC1 project:

```sh
git clone arnolddevos/epsilonio
echo '(unmanagedSourceDirectories in Compile) += baseDirectory.value / "epsilonio/src/main/scala"' > epsilonio.sbt
```

In scala code:

```scala
import minio.api._
```

See `Signature.scala` for the complete API.

## Architecture

The API is defined in `trait Signature { ... }`. The idea is that alternative implementations can be tried.   There are two implementations so far.

* The `Structure` and `Interpreter` implementation realized in object `api1`. 

* The `Direct` implementation realized in `api2`.

These rely on common modules `Fibers` and `Synchronization`.  

### Structure 

Defines `IO` and its combinators and constructors.  

An `enum IO[+E, +A] { ... }` is pure data structure. The computation it represents may eventually succeed with a value of `A` or fail with a value of `E`. Constructors include `effect` and `effectAsync` and combinators include `flatMap`, `zip` and `race`. 

### Interpreter

Defines the logic to execute an effect data structure. 

### Direct

Defines `IO` and its combinators and constructors directly in terms of a trampoline data structure, `Tail`. An alternative to the foregoing approach.

### Fibers

Defines `Fiber`, `Arbiter` and `Runtime`.

A `Runtime` provides methods to run effects: `unsafeRunAsync` and `unsafeRunSync`.  It depends on a `Platform` which, in this version, encapsulates a java `ForkJoinPool`. 

A `Fiber` represents a lightweight thread running an effect. Top level fibers are created by a `Runtime`. Child fibers are subsequently created by `fork`. 

Fiber operations include `join`, `await` and `interrupt`.  The semantics are intended to be the same as ZIO.

Class `Arbiter` is not part of the API. An arbiter manages a group of fibers and provides the `race` operation.  

### Synchronization

Defines `Transactor`. 

The state of each `Fiber` and `Arbiter` is held in a `Transactor[State]`. This is an asynchronous variable that is modified by atomic `Transaction`s. 

Operations on fibers and arbiters such as `fork`, `join` and `interrupt` are transactions.

A transaction is modeled as a pure function on state which may return a new state and a result effect. Or it may return the value `Blocked`.  Blocked transactions are retained in the transactor until they can produce an effect.

The transactor provides `transact[E, A](tx: Transaction[State, IO[E, A]]): IO[E, A]`.  This effect embodies the state change and result effect. 