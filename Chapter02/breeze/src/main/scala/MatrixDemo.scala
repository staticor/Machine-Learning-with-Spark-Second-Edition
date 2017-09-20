import breeze.linalg.DenseMatrix
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.linalg.SingularValueDecomposition
object MatrixDemo {
  def main(args : Array[String]): Unit = {
    val spconf = new SparkConf().setMaster("local").setAppName("MatrixDemo App")
    val sc = new SparkContext(spconf)
    sc.setLogLevel("ERROR")
    val denseData = Seq(
      Vectors.dense(0.0, 1.0, 2.1),
      Vectors.dense(3.0, 2.0, 4.0),
      Vectors.dense(5.0, 7.0, 8),
      Vectors.dense(9.0, 0.0, 1.1)
    )
    val sparseData = Seq(
      Vectors.sparse(3, Seq((1, 1.0), (2, 2.1))),
      Vectors.sparse(3, Seq((0, 3.0), (1, 2.0), (2, 4.0))),
      Vectors.sparse(3, Seq((0, 5.0), (1, 7.0), (2, 8.0))),
      Vectors.sparse(3, Seq((0, 9.0), (2, 1.0)))
    )

    val denseMat = new RowMatrix(sc.parallelize(denseData, 2))
    val sparseMat = new RowMatrix((sc.parallelize(sparseData, 2)))

    println("Dense Matrix - Num of Rows:" + denseMat.numRows())
    println("Dense Matrix - Num of Cols:" + denseMat.numCols())
    println(denseMat)
    println("Sprase Matrix - Num of Rows:" + sparseMat.numRows())
    println("Sprase Matrix - Num of Cols:" + sparseMat.numCols())
    println(sparseMat)

    val entries = sc.parallelize(Seq(
      (0, 0, 1.0),
      (0, 1, 2.0),
      (1, 1, 3.0),
      (1, 2, 4.0),
      (2, 2, 5.0),
      (2, 3, 6.0),
      (3, 0, 7.0),
      (3, 3, 8.0),
      (4, 1, 9.0)), 3).map { case (i, j, value) =>
      MatrixEntry(i, j, value)
    }
    val coordinateMat = new CoordinateMatrix(entries)
    println("Coordinate Matrix - No of Rows: " +
      coordinateMat.numRows())
    println("Coordinate Matrix - No of Cols: " +
      coordinateMat.numCols())

    val msData = DenseMatrix((2.5,2.4), (0.5,0.7), (2.2,2.9), (1.9,2.2), (3.1,3.0),
    (2.3,2.7), (2.0,1.6), (1.0,1.1), (1.5,1.6), (1.1,0.9))

    val pca = breeze.linalg.princomp(msData)

    // covariance matrix of the data
    print("covariance matrix", pca.covmat)
    // the eigenvalues of the covariance matrix, IN sorted order
    print("eigen values", pca.eigenvalues)

    // eigenvectors
    print("eigen vectors", pca.loadings)
    println(pca.scores)
  }
}
