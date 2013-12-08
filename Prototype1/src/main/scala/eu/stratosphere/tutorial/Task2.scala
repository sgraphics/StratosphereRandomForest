package eu.stratosphere.tutorial

import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._
import scala.collection.mutable.HashMap
import java.io.File

import collection.mutable.HashMap

class Task2 extends PlanAssembler with PlanAssemblerDescription with Serializable {
  override def getDescription() = {
    "Usage: [inputPath] [outputPath] ([numSubtasks])"
  }
  
  override def getPlan(args: String*) = {
    val inputPath = args(0)
    val outputPath = args(1)
    val numSubTasks = if (args.size >= 3) args(2).toInt else 1
    
    val source = TextFile(inputPath)
    
    // Here you should split the line into the doc id and the content.
    // Then output a (word, count) tuple for every word in the document
    // You could use a map to keep counts of the words.
    val termFrequencies = source flatMap { line => line
		.toLowerCase()
		.replaceAll("^\\d+,", "") //get rid of doc nr
		.replaceAll("[^\\w\\s]", " ") // get rid of other characters
		.replaceAll("\\s{2,}", " ") //get rid of double spaces
		.trim()
		.split(" ")
		.filter( !Util.STOP_WORDS.contains(_))
		.groupBy(x => x) // same words in same group
		.map{x => (line.split(",")(0).toInt, x._1, x._2.count(_ => true))}
    }
    
    val sink = termFrequencies.write(outputPath, CsvOutputFormat("\n", ","))
    
    new ScalaPlan(Seq(sink))
    
  }
}

object RunTask2 {
  def main(args: Array[String]) {
    // Write test input to temporary directory
    val inputPath = Util.createTempDir("input")

    Util.createTempFile("input/1.txt", "1,Big Hello to Stratosphere! :-)")
    Util.createTempFile("input/2.txt", "2,Hello to Big Big Data.")

    // Output
    // Replace this with your own path, e.g. "file:///path/to/results/"
    val outputPath = new File("C:\\Users\\Silver\\AppData\\Local\\Temp\\output").toURI().toString()

    // Results should be:
    //
    // Document 1:
    // big 1
    // hello 1
    // stratosphere 1
    //
    // Document 2:
    // hello 1
    // big 2
    // data 1

    println("Reading input from " + inputPath)
    println("Writing output to " + outputPath)

    val plan = new Task2().getPlan(inputPath, outputPath)
    Util.executePlan(plan)

    //Util.deleteAllTempFiles()
    System.exit(0)
  }
}
