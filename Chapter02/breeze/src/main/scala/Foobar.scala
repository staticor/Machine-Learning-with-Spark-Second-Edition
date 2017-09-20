import breeze.linalg._

import breeze.math.Complex
import breeze.linalg.{DenseVector, SparseVector}
import breeze.linalg.DenseMatrix
import breeze.linalg.norm
import breeze.linalg._
import breeze.stats.{mean, median}
object Foobar {
  def main(args: Array[String]): Unit = {

      val splitLine = "======================"
      println("part1:  Breeze Learn, 复数")
      val i = Complex.i  // import breeze.math.Complex
      //add
      println( (1 + 2 * i) + (2 + 3 * i))

      // divide
      println( (1 + 2 * i) / (2 + 3 * i))
      // mul
      println( (1 + 2 * i) * (2 + 3 * i))

      println(splitLine)


      println("part2:  Breeze Learn, 向量")
      val dv = DenseVector(2f, 0f, 3f, 2f, -1f)
      dv.update(3, 6f) // 2f ---> 6f
      println(dv)

      val sv : SparseVector[Double] = SparseVector(5)()
      sv(0) = 1
      sv(2) = 5
      sv(4) = 3
      val m: SparseVector[Double] = sv.mapActivePairs( (i, x) => x+1)
      println(m)

      val v1 = DenseVector(3, 7, 8.1, 4, 5)
      val v2 = DenseVector(1, 9, 3, 2.3, 8)

      def add() : Unit = {
          println(v1 + v2)
      }
      def dotproduct() : Unit = {
          println(v1 dot v2)
      }
      add()
      dotproduct()
      println(mean(v1))
      println(median(v1))
      println(splitLine)


      println("Densematrix======")
      val m1 = DenseMatrix((1,2), (3, 4))
      val m2 = DenseMatrix.zeros[Int](4, 5)

      println(m1)

      print(splitLine)
      println(m2)

  }
}
