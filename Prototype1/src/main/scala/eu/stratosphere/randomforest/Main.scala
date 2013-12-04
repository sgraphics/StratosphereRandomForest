package eu.stratosphere.randomforest;


import mnist.tools.MnistManager;

object MainClass {
  def main(args: Array[String]) {
    

	  	val m = new MnistManager( "train-images.idx3-ubyte",
			  					"train-labels.idx1-ubyte")
		val labels = m.getLabels()
		val num_samples = labels.getCount() 
		System.out.println( "samples found " + labels.getCount() )
		System.out.println( "image-format " + m.getImages().getCols()+" cols, "+m.getImages().getRows()+" rows" )
		System.out.println();
		for (i <- 0 to 100) {
			//System.out.println("read sample "+i)
			val image = m.readImage()
			
			//System.out.println("Label:" + m.readLabel());
			
			//System.out.println(i + ":" + m.readLabel() + ";" + image.map(line => line.mkString(",")).mkString(","))
			
			System.out.print(i + ":" + m.readLabel() + ";" + image.map(line => line.mkString(",")).mkString(",") + "\\r\\n")
	
			//System.out.println("Image length: " + m.getImages().getEntryLength());
			//val images = m.getImages()
			
			//System.out.println("Current Index: " + m.getImages().getCurrentIndex());
			
			//System.out.println("Label length: " + m.getLabels().getEntryLength());
			//System.out.println("Label Index: " + m.getLabels().getCurrentIndex());
		}
  }
}