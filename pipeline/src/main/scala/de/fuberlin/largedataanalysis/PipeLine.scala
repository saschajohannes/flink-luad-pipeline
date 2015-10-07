import de.fuberlin.largedataanalysis.{Trainer, PreProcess, Input}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.slf4j.LoggerFactory

object PipeLine {
  def main (args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.getConfig.disableSysoutLogging

    //the main logger
    val logger = LoggerFactory.getLogger("PipeLine")

    logger.info( "Start PipeLine" )

    //process the input
    Input.process( args(0), env, logger )

    //preprocess the data matrix
    val preProcessed = PreProcess.process( Input.dataMatrix.get , Input.probeIDAssociation.values.toSet, Input.sampleIDAssociation.values.toSet, Input.pcThreshold, env, logger )

    //train the svm
    Trainer.process( preProcessed, Input.samplesTraining.map( x => ( Input.sampleIDAssociation.get(x._1).get , x._2 ) ), env, logger )

    //predict the tumorous state
    val predicted = Trainer.predict( preProcessed, Input.samplesPrediction.map( Input.sampleIDAssociation.get( _ ).get ), env, logger )

    //convert sample ids back to their name
    val data = predicted.map( x => ( Input.sampleIDAssociation.map( y => y._2 -> y._1 ).getOrElse( x._1, "Unknown" ), x._2 ) )

    //print or write output
    Input.outputFile match {
      case None => data.print()
      case Some( p ) => {
        data.writeAsCsv(p.replace("%s%", System.currentTimeMillis.toString), fieldDelimiter = "\t", writeMode = WriteMode.OVERWRITE)
        env.execute( "PipeLine" )
      }
    }

    logger.info( "End PipeLine" )
  }
}