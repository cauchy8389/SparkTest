package functionalprogramming.testing

import java.util.concurrent.{ExecutorService, Executors}

import functionalprogramming.parallelism.Par
import functionalprogramming.state._
import functionalprogramming.testing.Gen.pint2
import functionalprogramming.testing.Prop.{equal, forAllPar}

object TestingObj {
  def main(args: Array[String]): Unit = {
    val ES: ExecutorService = Executors.newCachedThreadPool
    val p1 = Prop.forAll(Gen.unit(Par.unit(1)))(i =>
      Par.map(i)(_ + 1)(ES).get == Par.unit(2)(ES).get)

    Prop.run(p1)
    Prop.run(Prop.p2)
    Prop.run(Prop.check(false))

    Gen.choose(0,10).map(x => println(x))

    Gen.genStringIntFn(Gen.unit(2))

    val pint = Gen.choose(0,10) map (Par.unit(_))
    val p4 = forAllPar(pint)(n => equal(Par.map(n)(y => y), n))
    Prop.run(Prop.p4)

    Prop.run(Prop.p3)


    //val forkProp = Prop.forAllPar(pint2)(i => equal(Par.fork(i), i)) tag "fork"
    //Prop.run(Prop.forkProp)

    Prop.run(Gen.sortedProp)
    Prop.run(Gen.maxProp1)

    println(List.fill(5)("aaaa"))
  }

}
