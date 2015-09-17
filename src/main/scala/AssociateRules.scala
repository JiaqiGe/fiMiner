import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/* Created by jiaqige on 12/30/14.*/

class AssociateRules {

}

object AssociateRules{



  def findAllAssociatedRules(data:RDD[Array[String]], freqThreshold:Int, confidence:Double, maxLength:Int, sc:SparkContext) = {
    val freqItems = FrequentItemsets.findOneLengthFrequentSets(data, freqThreshold)
    val freqItemsets = FrequentItemsets.findAllItemsets(data,freqThreshold,maxLength,sc)
    val allFreqItemsets = freqItems.union(freqItemsets).sortByKey().collect()

    assert(confidence <= 1 && confidence > 0)


    val allFreqItemsetsBC = sc.broadcast(allFreqItemsets)

    val rules = freqItemsets.mapPartitions(partition => {
      val allFreqItemsetsValue = allFreqItemsetsBC.value
      partition.flatMap(x => findAssociatedRulesFromItemset(x._1,allFreqItemsetsValue,confidence))
    })

    rules.map(x => x._1)

  }


  def findAssociatedRulesFromItemset(pattern:String,freqItemsets:Array[(String,Int)], confidence:Double) = {

    val items = pattern.split(",").toSet


    val patternCount = binarySearchForCounts(pattern, freqItemsets)

    val subsets = items.subsets.filter(x => !x.isEmpty && x.size != items.size)

    val result = subsets.map(subset => {
      val X = FrequentItemsets.itemsetToString(subset.toArray.sorted)
      val Y = FrequentItemsets.itemsetToString(items.filter(x => !subset.contains(x)).toArray.sorted)

      val X_Count = binarySearchForCounts(X,freqItemsets)

      (X+"=>"+Y,patternCount/X_Count)
    }).filter(x => x._2 >= confidence)

    result
  }



  def binarySearchForCounts(target:String, patterns:Array[(String,Int)]):Double = {
    if(patterns == null || patterns.isEmpty)
      return -1

    var lo = 0; var hi = patterns.size

    while(lo <= hi){
      val mid = (lo + hi)/2

      if(patterns(mid)._1 == target){
        return patterns(mid)._2.toDouble
      }else if(target < patterns(mid)._1){
        hi = mid - 1
      }else{
        lo = mid + 1
      }
    }
    -1
  }


  //test
  def main(args:Array[String]) = {
    if(args == null || args.length < 5){
      System.err.println("Usage:FrequentItemsets<master><inputpath><outputPath><min_sup><min_conf>!")
      System.exit(-1)
    }


    val mode = args(0)
    val inputPath = args(1)
    val outputPath = args(2)
    val minSup = args(3).toInt
    val minConf = args(4).toDouble

    val sc = new SparkContext(mode,"Transwarp",new SparkConf)

    //add jar for yarn-client mode

    if(!mode.equals("local"))
      sc.addJar("apriori_2.10-1.1.jar")

    val fileRDD = sc.textFile(inputPath, 4).map(x =>{
      val elements = x.split(" ");
      (elements(0),elements(2))})
      .groupBy(x => x._1)
      .map(x => {x._2.toArray.map(x => x._2)})

    val associatedRules = findAllAssociatedRules(fileRDD,minSup,minConf,6,sc).repartition(1).map(x => "("+x+")")
    //      .map(x=> (("("+x._1+")"),x._2))

    associatedRules.saveAsTextFile(outputPath)

    if(mode.equals("local"))
      System.out.print(associatedRules.collect().mkString)
  }
}

