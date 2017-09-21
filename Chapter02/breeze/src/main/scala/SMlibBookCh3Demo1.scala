import breeze.linalg._
import breeze.numerics.pow


object SMlibBookCh3Demo1 {

  def main(args: Array[String]): Unit = {
    // create a matrix full of zeros, size: 2* 3
    val m1 = DenseMatrix.zeros[Double](2, 3)
    println(m1)

    // a vector full of zeros, lenght: 3
    val v1 = DenseVector.zeros[Double](3)

    val v2 = DenseVector.ones[Double](3)

    val v3 = DenseVector.fill[Double](3){5.0}
    println(v3)
    // 5.0, 5.0, 5.0

    val v4 = DenseVector.range(1, 10, 2)

    // identity matrix  3*3,
    val m2= DenseMatrix.eye[Double](3)

    val v6 = diag(v4)
    println(v6)

    val m3 = DenseMatrix((1.0, 2.0), (3.0, 99.0))

      // transpose a vector
    val v9 = DenseVector(1,2,3,4,5).t
    println(v9)

    val v10 = DenseVector.tabulate(20){i => pow(2, i)}
    println(v10)


    // 向量的取值

    val a = v10
    println(a(0))
    println(a(1 to 4))
    println(a(1 to -1))

    //矩阵拼接
    // DenseMatrix.vertcat
    // DenseMatrix.horzcat


    println(a.map(x => x + 1200000000))
  }

}
