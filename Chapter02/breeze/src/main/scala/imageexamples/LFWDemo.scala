package imageexamples

import java.awt.image.BufferedImage
import javax.imageio.ImageIO
import java.io.File
import org.apache.spark.{SparkConf, SparkContext}

object LFWDemo {
  def main(args: Array[String]): Unit = {
     val path = "/Users/steve/lfw/*"

     val spConfig = new SparkConf().setMaster("local").setAppName("0921")
     val sc = new SparkContext(config=spConfig)
     val rdd = sc.wholeTextFiles(path)

    val first = rdd.first()

    //println(first)
    val files = rdd.map{case (fileName, content) =>
        fileName.replace("file:", "")
    }
    println(files.first())
    // files count
    println(files.count())

    val aePath = "/Users/steve/lfw/Aaron_Eckhart/Aaron_Eckhart_0001.jpg"

    val aeImage = loadImageFormat(aePath)

    println(aeImage)

    val gradeImage = processImage(aeImage, 100, 100)
    ImageIO.write(gradeImage, "jpg", new File("/Users/steve/aeGray.jpg"))
  }

  def loadImageFormat(path: String) : BufferedImage = {
    import javax.imageio.ImageIO
    import java.io.File
    ImageIO.read(new File(path))
  }

  def processImage(image: BufferedImage, width: Int, height: Int) : BufferedImage = {
     val bwImage = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY)
    val g = bwImage.getGraphics()
    g.drawImage(image, 0, 0, width, height, null)
    g.dispose()
    bwImage
  }
}
