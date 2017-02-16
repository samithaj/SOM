/**
  * Created by sam on 11/24/16.
  */
import scala.math._
import org.apache.spark.{SparkConf, SparkContext}

object SoMScala{

  def main(args: Array[String]): Unit = {
   // System.setProperty("hadoop.home.dir","C:/hadoop-2.6.4")

    val conf = new SparkConf()
      .setAppName("SoM")
      .setMaster("local[1]")
    val sc = new SparkContext(conf)

    //Specify learning rate
    val learningRate = 0.001

    val rand = scala.util.Random
    val dataArray: Array[Double] = new Array[Double](500)

    //generating 500 random inputs
    for(i <- 0 until 500){
      dataArray(i) = rand.nextDouble()
    }
    val dataRDD = sc.parallelize(dataArray)
    dataRDD.saveAsTextFile("output/inputs.txt")

    //generating 10 weights
    val weightArr : Array[Double]= new Array[Double](10)
    for (i <- 0 until 10){
      weightArr(i) = rand.nextDouble()
    }
    val weightRDD = sc.parallelize(weightArr)
    weightRDD.saveAsTextFile("output/weights.txt")

    // Finding the wining neuron
    var minDistance : Double = 0
    var winningIndex : Int = 0
    for (i <- 0 until 1){
      val input = dataArray(i)
      minDistance = input
      for (j <- 0 until 10){
        val weightValue = weightArr(j)
        val distance = math.abs(weightValue-input)
        if (distance < minDistance) {
          minDistance = distance
          winningIndex = j
        }
      }
    }

    println("winning neuron index is "+winningIndex)

    //creating the lattice
    val lattice = Array.ofDim[Int](2, 5)
    for (i <- 0 to 1 ){
      for (j <- 0 until 5){
        if (i == 0){
          lattice(i)(j) = j+1
        }else{
          lattice(i)(j) = j+6
        }
      }
    }
    for(i <- 0 until 1){
      val inputVal = dataArray(i)
      for (j <- 0 until 10){
        val distance = calc_distance(winningIndex,j)
        val zigma = calc_zigma(j)
        val topo = calc_topological(distance,zigma)

        val delW = learningRate * topo *(inputVal-winningIndex)

        weightArr(j) = weightArr(j)+ delW
      }
    }

    //printing the updated weight values
    for (x <- weightArr){
      println(x)
    }


  }

  //find the neighbours
  def calc_distance(winningIndex : Int, neighbourIndex:Int): Double={
    if((winningIndex <5 && neighbourIndex <5 ) || (winningIndex>5 && neighbourIndex >5)){
      math.abs(neighbourIndex-winningIndex)
    }else if(winningIndex%5 == neighbourIndex%5) {
      1
    }else {
      math.sqrt(math.pow(math.abs((winningIndex % 5) - (neighbourIndex % 5)), 2) + 1)
    }
  }


  //find zigma value
  def calc_zigma(iterationVal:Int):Double={
    val sigma:Double = 1
    val lamda:Double = 8
    sigma * math.exp(-iterationVal / lamda)
  }

  def calc_topological(distance:Double,sigma:Double): Double ={
    math.exp(-1 * math.pow(distance, 2) / (2 * math.pow(sigma, 2)))
  }

}