
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by jiaqige on 12/18/14.
 */

class FrequentItemsets {

}

object FrequentItemsets{


  //given a dataset and a threshold, find all frequent items
  def findOneLengthFrequentSets(originalData:RDD[Array[String]],freqThreshold:Int):RDD[(String,Int)] = {
    val itemCount = originalData.map(_.toSet)
      .flatMap(_.toTraversable)
      .map( item => (item,1))
      .reduceByKey(_ + _)
      .filter(pattern => pattern._2 >= freqThreshold)

    itemCount
  }


  //give a dataset and a threshold, find all frequent two-length itemsets
  def findTwoLengthItemsetsRDD(data:RDD[Array[String]],freqThreshold:Int):RDD[(String,Int)] ={
    val largeSet = data.mapPartitions(partition =>{
      //[A,B,C] => [A|B,A|C,B|C]
      partition.flatMap(transaction => {
        val candidates = new ArrayBuffer[(String,Int)]
        for( i <- 0 until transaction.length){
          for(j <- i+1 until transaction.length){
            candidates += (((transaction(i)+","+ transaction(j)),1))
          }
        }
        candidates.iterator
      })
    }).reduceByKey(_ + _)
      .filter(x => x._2 >= freqThreshold)
      .sortByKey()

    largeSet
  }


  def findAllItemsets(originalData:RDD[Array[String]], freqThreshold:Int, maxLength: Int, sc:SparkContext):RDD[(String,Int)]={
    val data = originalData.map(x => x.distinct.sorted)

    var largeSet = findTwoLengthItemsetsRDD(data,freqThreshold)
    var freqItemsets = largeSet

    if(largeSet.count() == 0)
      return freqItemsets

    var isComplete = false
    var k = 3

    while(k <= maxLength && !isComplete){
      val candidates = selfJoin(largeSet).collect()
      if(candidates.isEmpty){
        isComplete = true
      }else{
        //start a new round
        val bcCandidates = sc.broadcast(candidates.sorted)

        val kFrequentSetRDD =  data.mapPartitions(transactions => {
          val candidatesArray = bcCandidates.value
          val itemset = new mutable.HashSet[String]()
          candidatesArray.foreach(x => {
            val items = x.split(",")
            itemset.++=(items)
          })

          val candidateTrie = new CandidateTrie

          candidates.sorted.foreach(x => candidateTrie.insert(x.split(",")))

          transactions.flatMap(transaction =>{

            candidateTrie.search(transaction.filter(x => itemset.contains(x))).filter(x => !x.isEmpty).map(x => (x,1))
          })
        }).reduceByKey(_ + _)
          .filter(x => x._2 >= freqThreshold)
          .sortByKey()

        //add kfrequent itemsets to all-frequent itemset

        if(kFrequentSetRDD.count() == 0){
          isComplete = true
        }else{
          freqItemsets = freqItemsets.union(kFrequentSetRDD)
          largeSet = kFrequentSetRDD
          k += 1
        }
      }
    }//end while

    freqItemsets
  }


  // an itemset is represented by <items,...,items>
  def itemsetToString(set:Array[String]): String ={
    val sb = new StringBuilder

    if(set.isEmpty)
      return ""

    if(set.size == 1)
      return ""+set.toSeq(0)

    set.foreach(x => {sb.append(x);sb.append(",")})
    sb.deleteCharAt(sb.length-1)
    sb.toString
  }

  // given frequent itemsets to generate candidates by self-joining
  def selfJoin(largeSets:RDD[(String,Int)]):RDD[String]= {
    //transfer to pair RDD
    val removeFirst = largeSets.map(x => {
      //remove the first item
      val pattern = x._1
      var i = 0;
      var isFound = false
      while (i < pattern.length && !isFound) {
        if (pattern(i) == ',')
          isFound = true
        else
          i = i + 1
      }

      val key = pattern.substring(i + 1)
      (key, pattern)

    })

    val removeLast = largeSets.map(x => {
      //remove last item
      val pattern = x._1
      var i = pattern.length-1
      var isFound = false

      while (i >= 0 && !isFound) {
        if (pattern(i) == ',') {
          isFound = true
        } else {
          i = i - 1
        }
      }
      val key = pattern.substring(0, i)
      (key, pattern)
    })

    val jointSet = removeFirst.join(removeLast).map(x => {
      val s1 = x._2._1
      val s2 = x._2._2

      //get the last item of s2
      val items = s2.split(",")
      val lastItem = items(items.length - 1)

      //return by appending lastItem to s1
      s1 + "," + lastItem
    })
    jointSet
  }



  def main(args:Array[String]) = {

    if(args == null || args.length < 4){
      System.err.println("Usage:FrequentItemsets<master><inputpath><outputPath><threshold>!")
      System.exit(-1)
    }


    val mode = args(0)
    val inputPath = args(1)
    val outputPath = args(2)
    val threshold = args(3).toInt

    val sc = new SparkContext(mode,"Transwarp",new SparkConf)
    /*add jar for yarn-client mode*/

    if(!mode.equals("local"))
        sc.addJar("apriori_2.10-1.1.jar")

    val fileRDD = sc.textFile(inputPath, 4).map(x =>{
      val elements = x.split(" ");
      (elements(0),elements(2))})
      .groupBy(x => x._1)
      .map(x => {x._2.toArray.map(x => x._2)})

    val freqItemset = findAllItemsets(fileRDD,threshold,6,sc).repartition(1).map(x=> (("<"+x._1+">"),x._2))
    //      .map(x=> (("("+x._1+")"),x._2))
        freqItemset.saveAsTextFile(outputPath)

    if(mode.equals("local"))
      System.out.print(freqItemset.collect().mkString)
  }

}