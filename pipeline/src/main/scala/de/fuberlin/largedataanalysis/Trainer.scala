package de.fuberlin.largedataanalysis

import org.apache.flink.api.scala._
import org.apache.flink.ml.classification.SVM
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.util.Collector


import org.slf4j.LoggerFactory
import org.slf4j.Logger



object Trainer {

  /*
   * Train a predictor using a given sample set
   * Parameters:
   *    data: the complete data matrix
   *    samples: sample ids which should be used for training (includes tumorous state)
   *    env: flink's execution environment
   *    LOG: a logger
   *
   */
  def process( data: DataSet[(Int, Int, Double)], samples: Set[(Int, Boolean)],  env: ExecutionEnvironment, LOG: Logger  = LoggerFactory.getLogger("Trainer") ) = {

    LOG.info( "Start Training" )

    //the svm
    val svm = SVM()

    //convert set to map
    val state = samples.map( x => x._1 -> x._2 ).toMap

    //convert matrix and sample set to required vector format
    val training: DataSet[LabeledVector] = data
      //remove all samples which are not in the sample set
      .filter( x => state.contains( x._1 ) )
      //group by sample id
      .groupBy( 0 )
      .reduceGroup( ( in, out: Collector[ LabeledVector ] ) => {
        //values to array
        val d = in.toArray
        //convert data to dense vector
        val vec = DenseVector( d.map( y => ( y._2, y._3) ).sortBy( _._1  ).map( _._2 ) )
        LOG.info( "Vector is for training: {}", vec.size )
        //publish data and state
        out.collect( LabeledVector( if( state.get( d( 0 )._1 ).get ) 1d else -1d, vec ) )
      } )

    LOG.info( "Start SVM Training with {} samples", training.count )

    //train the svm
    svm.fit( training )

    predictorSVM = Some( svm )

    LOG.info( "End Training" )

  }

  /*
   * Predict the tumorous state
   *
   * Parameters:
   *    data: the complete data matrix
   *    samples: sample ids which should be used for prediction
   *    env: flink's execution environment
   *    LOG: a logger
   *
   */
  def predict( data: DataSet[(Int, Int, Double)], samples: Set[Int], env: ExecutionEnvironment, LOG: Logger  = LoggerFactory.getLogger("Trainer") ) : DataSet[ (Int, Double) ]  = {

    predictorSVM match {

      case None => null

      case Some( svm ) => {

        LOG.info( "Start Prediction" )
        //extraxt the data to predict
        val predict = data
          //keep only data which shoud be predicted
          .filter( x => samples.contains( x._1 ) )
          //group by sample id
          .groupBy( 0 )
          .reduceGroup( ( in, out: Collector[ (Int, DenseVector)] ) => {
            //store data as array
            val d = in.toArray

            //convert to dense vector
            val vec = DenseVector( d.map( y => ( y._2, y._3) ).sortBy( _._1  ).map( _._2 ) )
            LOG.info( "Vector is for prediction size: {}", vec.size )
            //publish the data
            out.collect( ( d(0)._1, vec ) )
          } )

        LOG.info( "Size of samples for prediction {}, expected {},  datasize {}", predict.count.toString, samples.size.toString, data.count .toString)

        //predict the values
        val predicted = svm
          .predict( predict.map( _._2 ) )
          //join with the original data
          .join( predict )
          //where the desnse vectors are equal
          .where( 0 ).equalTo( 1 )
          //keep only the sample id and the prediction result
          .map( x => ( x._2._1, x._1._2 ) )
        LOG.info( "End Prediction" )

        predicted
      }
    }

  }

  //the svm
  private var predictorSVM: Option[ SVM ] = None
}
