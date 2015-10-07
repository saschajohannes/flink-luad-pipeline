package de.fuberlin.largedataanalysis


import org.apache.flink.util.Collector
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

import scala.collection.mutable

import org.slf4j.LoggerFactory
import org.slf4j.Logger

object Input {

  /*
   * process the definition file which was given as input
   *
   * Parameters:
   *    defFile: the path of the definition file
   *    env: flink's execution environment
   *    LOG: a logger
   *
   */
  def process( defFile: String , env: ExecutionEnvironment, LOG: Logger  = LoggerFactory.getLogger("Input") ) = {

    LOG.info( "Start Input" )

    //read the file
    val parameters
      = env.readCsvFile[DefFileFormat]( defFile, fieldDelimiter = "\t", lenient = true, ignoreComments = "#" )


    //analyse file
    val dataToLoad = parameters.reduceGroup( ( in, out: Collector[ (String, AvailableInputs) ] ) => {

      //given samples
      val samples = mutable.Set[ String ]()
      //ginve samples to predict
      val prediction = mutable.Set[ String ]()
      //samples which are tomorous
      val tumorous = mutable.Set[ String ]()
      //diffrent input types
      val types = mutable.Set[ String ]()

      val inputDataSet = mutable.Map[String, AvailableInputs]()

      //check each line
      for( x <- in ) {
        x.f1 match {
            //definitions of samples, types states and output
          case "def" =>
            x.f2 match {
              case "sample" => samples.add( x.f3 )
              case "sample-type" => types.add( x.f3 )
              case "predictive" => prediction.add( x.f3 )
              case "output" => outputFile = Some( x.f3 )
              case "pc-threshold" => if( x.f3.toLowerCase == "none" ) {
                pcThreshold = None
              } else {
                pcThreshold = Some( x.f3.toDouble )
              }
            }
            //diagnosis of a specific samples
          case "diagnosis" =>
            if( x.f3.equals( "TN" ) ) {
              tumorous.add( x.f2 )
            }
            //files of types
          case any =>
            if( !inputDataSet.contains( x.f2 ) ) {
              inputDataSet( x.f2 ) =  AvailableInputs()
            }
            inputDataSet.get( x.f2 ).get.data( any ) = x.f3
        }
      }

      LOG.info( "Number of Types {}", types.size )
      LOG.info( "Number of Training Samples {}", samples.size )
      LOG.info( "Number of pos Training Samples {}", samples.size - samples.intersect( tumorous ).size )
      LOG.info( "Number of neg Training Samples {}", samples.intersect( tumorous ).size )
      LOG.info( "Number of Predictive Samples {}", prediction.size )

      //build required structure
      for( x <- inputDataSet ) {
        //only of sample name occurs in sample or prediction lists, otherwise skipp
        if( samples.contains( x._1 ) || prediction.contains( x._1 ) ) {
          //check all given files, process only type is given
          val a = mutable.Map[String, String]()
          for (files <- x._2.data) {
            if( types.contains( files._1 ) && files._2.nonEmpty ) {
              a( files._1 ) = files._2
            }
          }

          //if data is available, publish it
          if( a.nonEmpty ) {
            out.collect( x._1, new AvailableInputs( a, prediction.contains( x._1 ), tumorous.contains( x._1 ) ) )
          }
        }
      }
    } )

    //convert sample names to ids
    val sampleIDAssoc_ = dataToLoad
      .map( _._1 )
      .collect
      .zipWithIndex
      .map( x => x._1 -> x._2 ).toMap

    //the resulting matrix as option
    var dMatrix: Option[ DataSet[ (Int, String, Double) ] ] = None

    //foreach sample
    dataToLoad.map( x => ( sampleIDAssoc_.get( x._1 ).get, x._2 ) ).collect.foreach( x => {
      //foreach data file
      for( s <- x._2.data ) {
        LOG.info( "Read Sample {} type {} file {} ", x._1.toString, s._1, s._2 )

        //read the file and convert to requested format
        val data = env
          .readCsvFile[ InputFileFormat ]( s._2, fieldDelimiter = "\t", lenient = true, includedFields = Array( 0, 1 ) )
          .map( y => ( x._1, y.id, y.value ) )

        //append to data matrix
        dMatrix match {
          case None => dMatrix = Some( data )
          case Some( d ) => {
            dMatrix = Some( d.union( data ) )
          }
        }
      }
    } )

    LOG.info( "Finish reading files" )

    //convert probe names to ids
    val probeIDAssoc_ = mutable.Map[String, Int]()
    dMatrix.get
      .map( _._2 )
      .collect
      .toSet[String]
      .foreach( x => probeIDAssoc_( x ) = probeIDAssoc_.size )

    LOG.info( "Number of Probes {}", probeIDAssoc_.size )

    //publlish data after converting probe names to probe ids
    dataMatrix = Some( dMatrix.get.map( x => ( x._1, probeIDAssoc_.getOrElse( x._2, -1 ), x._3 ) ) )

    //LOG.info( "Number of data points {}", dMatrix.get.count )

    //probe name / sample name id association
    probeIDAssociation = probeIDAssoc_.toMap
    sampleIDAssociation = sampleIDAssoc_.toMap

    //samples for training including their tumorous state
    samplesTraining = dataToLoad.filter( x => !x._2.prediction ).map( x => ( x._1, x._2.tumorous ) ).collect.toSet

    //samples to predict
    samplesPrediction  = dataToLoad.filter( x => x._2.prediction ).map( x => ( x._1 ) ).collect.toSet

    LOG.info( "End Input" )
  }

  //input format of definition file
  case class DefFileFormat( f1: String,  f2: String, f3: String )
  //input format of data files
  case class InputFileFormat( id: String,  value: Double )
  //data storage
  case class AvailableInputs( data: mutable.Map[String, String] = mutable.Map[String, String](), prediction: Boolean = false, tumorous :Boolean = false )

  //the data matrix
  var dataMatrix: Option[ DataSet[ (Int, Int, Double) ] ] = None

  var samplesTraining = Set[(String, Boolean)]()
  var samplesPrediction = Set[String]()

  var probeIDAssociation = Map[String, Int]()
  var sampleIDAssociation = Map[String, Int]()

  // optional part
  //write prediction result to this file
  var outputFile: Option[ String ] = None

  //threshold for correlation
  var pcThreshold: Option[ Double ] = Some( .8 )


}
