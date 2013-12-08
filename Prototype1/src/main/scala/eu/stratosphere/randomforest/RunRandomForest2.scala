package eu.stratosphere.randomforest;

import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._
import scala.collection.mutable.HashMap
import java.io.File
import eu.stratosphere.tutorial.Util
import mnist.tools.MnistManager
import java.util.Map
import scala.util.Random
import java.util.ArrayList
import eu.stratosphere.scala.DataSource
import scala.Array.canBuildFrom
import eu.stratosphere.pact.client.LocalExecutor
import eu.stratosphere.pact.common.`type`.base.PactInteger
import eu.stratosphere.pact.common.`type`.base.PactString
import eu.stratosphere.scala.operators._
import eu.stratosphere.scala.ScalaPlan
import eu.stratosphere.scala.DataSet
import eu.stratosphere.scala.analysis.GlobalSchemaPrinter
import eu.stratosphere.scala.DataSource
import eu.stratosphere.scala.ScalaPlan
import eu.stratosphere.scala.TextFile
import eu.stratosphere.pact.common.`type`.base.PactInteger
import eu.stratosphere.pact.common.`type`.base.PactString

class RandomForest2 (trees : Array[Array[TreeNode2]]) extends PlanAssembler with PlanAssemblerDescription with Serializable {
  override def getDescription() = {
    "Usage: [inputPath] [outputPath] ([numSubtasks])"
  }
  
  override def getPlan(args: String*) = {
    val inputPath = args(0)
    val outputPath = args(1)

    val source = TextFile(inputPath)
    
	val dataMap = source
		// line => (index, label, features)
		.map(line => (line.split(":")(0).toInt, line.split(":")(1).split(";")(0).toInt, line.split(":")(1).split(";")(1).split(",").map(element => element.trim().toInt)))
		.flatMap { case (lineIndex, lineLabel, lineFeatures) => 
		  trees
		  	//get leaf nodes that need processing - with features that are available
		  	.flatMap(tree => getNodesToSplit(tree))
		  	//see if current data point valid for node
		  	.filter { node => node.baggings.contains(lineIndex) }
		  	.flatMap { node =>  node.candidateFeatures
		  	  	// get values for corresponding features and nodes
		  	  	.map(feature => (node.treeId + "_" + node.nodeId, (feature, lineFeatures(feature), lineLabel, node.nodeId, lineIndex)))
		  	}
    }
    	
    val dataReduce = dataMap
      .groupBy { _._1 }
      .reduceGroup { values => 
      	val buffered = values.buffered
      	val (treeAndNode, _) = buffered.head
      	val treeId = treeAndNode.split("_")(0).toInt
      	val nodeId = treeAndNode.split("_")(1).toInt
      	
      	//prepare data for histograms
      	//feature id, value, label, node id, bagging id
      	val histogram = buffered
      		.map { case (mapId, mapResult) => (mapResult._1, mapResult._2, mapResult._3, mapResult._4, mapResult._5) }.toArray
      		// group by feature id
      		.groupBy( _._1 );
      	
      	val bestSplit = histogram
  			//Get split info for all features available for the node
  			.map { case (featureId, featureHistogram) => getSplitInfo(featureId, featureHistogram) }
  			//Get the best split
  			.maxBy( _._2 )
      	
  		
      	if (bestSplit == null || bestSplit._2 < 10)
      	{
      		//TODO: change node to leaf node with a label???
      		(treeId, nodeId, null)
      	}
      	else
      	{
      		//TODO: output new children node tuple
      		(treeId, nodeId, bestSplit)
      	}
      }
    
    val sink = dataReduce.write(outputPath, CsvOutputFormat("\n", ","))
    
    new ScalaPlan(Seq(sink))
  }
  
  //tree id, feature id, value, label, data id / bagging id
  def getSplitInfo(featureId : Int, histogram : Array[(Int, Int, Int, Int, Int)]) = {
    // { (P1, M1, L1, nodeId, dataId), ... (Pn, Mn, Ln, nodeId, dataId) }
    
    //featureId, gain, new bagging table
    
    // 1) DELTA?
    // 2) Number?
    //(featureId, Random.nextInt(100), Random.nextInt(255))
    (featureId, Random.nextInt(100), Random.nextInt(255))
  }
  
  def getNodesToSplit(tree : Array[TreeNode2]) = {
    var c: Array[TreeNode2] = Array();
    if (tree == null) c
    	
    c = c :+ (tree(0))
    c
  }
}

///baggings: data for finding the best feature to split
///features: all features left for this node
///candidateFeatures: m features (subspace) from all features
///feature: the selected feature index for this node
///featureValue: the selected feature value (left child LESS than value, right side MORE than value)
@serializable
class TreeNode2(
    var baggings : Array[Int],
    var features : Set[Int],
    var candidateFeatures : Array[Int],
    var feature : Int,
    var featureValue : Int,
    var treeId : Int,
    var nodeId : Int,
    var leaf : Boolean)
{
  def resetCandidateFeatures(featureSubspaceCount : Int, totalFeatureCount : Int){
    if (candidateFeatures == null)
    	candidateFeatures = generateFeatureSubspace(featureSubspaceCount, totalFeatureCount)
  }
  
  def generateFeatureSubspace(randomCount : Int, maxRandomNumber : Int) : Array[Int] = {
    var arrayList = new ArrayList[Int]();
    // Generate an arrayList of all Integers
    for(i <- 0 until maxRandomNumber){
        arrayList.add(i);
    }
    var arr : Array[Int] = Array()
    arr = Array(randomCount)
    arr = Array.fill(randomCount)(0)
    for(i <- 0 until randomCount)
    {
        var random = new Random().nextInt(arrayList.size());
        arr(i)=arrayList.remove(random);
    }
    arr;
  }
}

object RunRandomForest2 {
  def main(args: Array[String]) {    
    val treeCount = 10
    val totalFeatureCount = 784
    val dataCount = 100 //TODO: make this dynamic!!
    
    var featureSubspaceCount = math.round(math.log(totalFeatureCount).toFloat + 1);
    
    //prepare trees and their bagging tables    
    var trees = Array.fill(treeCount)(Array.fill(1)(new TreeNode2(
        Array.fill(dataCount)(Random.nextInt(dataCount-1)), //baggings : Array[Int]
        (0 until totalFeatureCount).toSet, //features : Set[Int]
        null, //candidateFeatures : Array[Int]
        -1, //feature
        -1, //featureValue
        -1, //treeId
        1, //nodeId
        false //leaf : Boolean
        )))
        
    // Assign id's to trees
    for (treeId <- 0 to trees.length - 1)
      trees(treeId)(0).treeId = treeId
      
    // Assign id's to trees
    for (treeId <- 0 to trees.length - 1)
      trees(treeId)(0).resetCandidateFeatures(featureSubspaceCount, totalFeatureCount)
    
	System.out.println("Generating '" + treeCount + "' trees, feature count: " + totalFeatureCount + ", with '" + featureSubspaceCount + "' feature subspace, data length: " + dataCount);
    
    val inputPath = new File("newdata.txt").toURI().toString()
    println("Reading input from " + inputPath)
    val outputPath = new File("C:\\Users\\Silver\\AppData\\Local\\Temp\\output").toURI().toString()
    println("Writing output to " + outputPath)
    
    //var hasAnyNewTreeNodes = true;
    //do
    //{
	    val plan = new RandomForest2(trees).getPlan(inputPath, outputPath)
	    Util.executePlan(plan)
	    
	    //var treeTup = parseOutput(job)
	    //hasAnyNewTreeNodes = updateForests(trees,tree-tup)
    //}while (hasAnyNewTreeNodes)

    Util.deleteAllTempFiles()
    System.exit(0)
  }
  
}
