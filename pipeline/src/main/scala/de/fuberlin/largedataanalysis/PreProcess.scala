package de.fuberlin.largedataanalysis


import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.recommendation.ALS
import org.apache.flink.util.Collector
import org.slf4j.{LoggerFactory, Logger}


object  PreProcess {

  type IndexType = scala.Int

  /*
   * Calculate the pearson correlation coefficients of each column against each column
   *
   * Parameters:
   *    dataSet: the data matrix
   *    filter: the treshold, only absolut values larger than the treshold are published
   *    LOG: a logger
   *
   */
  def pearsonCorrelation( dataSet: DataSet[ (IndexType,IndexType,Double) ], filter: Double, LOG: Logger  = LoggerFactory.getLogger("PreProcess") ): DataSet[ (IndexType,IndexType,Double) ] = {

    LOG.info( "Start Correlation" )

    //some preproccesing
    val preProccesed = dataSet
      //group accoring to their probe
      .groupBy( 1 )
      //reduce each group to their sum, squared sum and value list
      .reduceGroup( (in, out: Collector[(IndexType, Double, Double, List[(IndexType, Double)])]) => {

        //the probe id
        var key: Option[IndexType] = None
        //map values to sample id, value, squared value and sample id
        val data = in.map( x =>  ( x._2, x._3, x._3 * x._3, x._1) ).toList

        //the sum of all values
        val X = data.map( _._2 ).sum
        //the squared sum of all values
        val X2 = data.map( _._3 ).sum

        //the sample id
        for (x <- data) {
          key = Some(x._4)
        }

        //publish data
        out.collect(( key.getOrElse(-1), X, X2, data.map( x => ( x._1, x._2 ) ) ))
      } )

    //get the complete set of probes
    val set = preProccesed.collect

    //correlate
    preProccesed
      //group by sample id
      .groupBy( 0 )
      .reduceGroup( (in, out) => {
        var doLog = true
        //for each probe in sample
        for( x <- in ) {
          //for each probe in sample
          for( y <- set ){
            //calculate only upper triangle of resulting matrix
            if( x._1 > y._1 ) {
              //calculate the sum of all products of x and y
              val sumXY = x._4.zip( y._4 ).map( v => v._1._2 * v._2._2 ).sum
              //the resulting calue
              val value = (x._4.size * sumXY - x._2 * y._2 ) / math.sqrt( ( x._4.size * x._3 - x._2 * x._2 ) * ( y._4.size * y._3 - y._2 * y._2 ) )
              //publish only abs value larger than the threshold
              if( !value.isInfinite && !value.isNaN && value.abs >= filter )
                out.collect( ( x._1, y._1, value ) )
            }
          }
        }
      } )

  }

  /*
   * PreProcess the input
   *
   * Parameters:
   *    data: the input data matrix
   *    probeSet: all available probe ids
   *    sampleSet: all available sample ids
   *    pcThreshold the threshold for the co expression filter process
   *    env: flink's execution environmen
   *    LOG: a logger
   *
   */
  def process( data: DataSet[(Int, Int, Double)], probeSet: Set[Int], sampleSet: Set[Int], pcThreshold: Option[Double], env: ExecutionEnvironment, LOG: Logger  = LoggerFactory.getLogger("PreProcess") ): DataSet[(IndexType
    , IndexType, Double)] = {

    LOG.info( "Start PreProcess" )

    //get indices of missing entries
    val missing = data// = env.fromCollection( sampleSet ).cross( env.fromCollection( probeSet  ) )
      //remove the value
      .map( x => (x._1, x._2) )
      //group by sample id
      .groupBy( 0 )
      .reduceGroup( (in, out: Collector[ (Int, Int) ] ) => {
        //the set of available probe ids
        val set = in.toSet
        //the sample id
        val x = set.map( _._1 ).head


        probeSet
          //get difference if available probe ids and given probe ids
          .diff( set.map( _._2 ) )
          //publish missing
          .foreach( y => out.collect( (x, y) ) )
      } )

    LOG.info( "Missing entries {} expected {}", missing.count, probeSet.size * sampleSet.size - data.count )

    //the resulting data
    var pData = Some( data )

    //complete matrix only if there is something to complete
    if( missing.count > 0) {

      //set up ALS
      val als = ALS()
        .setIterations(10)
        .setNumFactors(10)
        .setBlocks(100)

      //add paramters
      val parameters = ParameterMap()
        .add(ALS.Lambda, 0.9)
        .add(ALS.Seed, 42L)

      LOG.info( "Train Completion" )
      //train the ALS
      als.fit(data, parameters)

      LOG.info( "Start Completion of {} entries", missing.count )
      // predict missing entries
      val predicted = als.predict( missing )


      LOG.info( "End Completion" )

      //create union of available and predicted data
      pData = Some( data.union( predicted ).rebalance )
    }

    //check if threshold is given
    pcThreshold match {

      case None => { pData.get }

      case Some( t ) => {

        //start a peasrson correlation of data matrix
        val pC = pearsonCorrelation( env.fromCollection( pData.get.collect ), t, LOG )

        LOG.info("Correlation size: {}", pC.count)

        //extract edges of correaltion( filtered )
        val edges = pC.flatMap(x => Seq((x._1, x._2), (x._2, x._1)))

        //vertcies equals all available probe ids
        val vertices = env.fromCollection( probeSet ).map( x => (x, x) )

        LOG.info("Start Community Detection")

        //===========================================================================
        //the connected component analysis
        // taken from: https://ci.apache.org/projects/flink/flink-docs-master/apis/examples.html#connected-components

        val verticesWithComponents = vertices.iterateDelta(vertices, 100, Array("_1")) {
          (s, ws) =>
            // apply the step logic: join with the edges
            val allNeighbors = ws.join(edges).where(0).equalTo(0) { (vertex, edge) =>
              (edge._2, vertex._2)
            }.withForwardedFieldsFirst("_2->_2").withForwardedFieldsSecond("_2->_1")

            // select the minimum neighbor
            val minNeighbors = allNeighbors.groupBy(0).min(1)

            // update if the component of the candidate is smaller
            val updatedComponents = minNeighbors.join(s).where(0).equalTo(0) {
              (newVertex, oldVertex, out: Collector[(IndexType, IndexType)]) =>
                if (newVertex._2 < oldVertex._2) out.collect(newVertex)
            }.withForwardedFieldsFirst("*")

            // delta and new workset are identical
            (updatedComponents, updatedComponents)
        }
        //===========================================================================

        LOG.info("End Community Detection")

        //extract the remaining probes ids (only one per component)
        val newProbes = verticesWithComponents
          //group by component id
          .groupBy(1)
          .reduceGroup((in, out: Collector[IndexType]) => {
            //publish the first
            out.collect(in.next._1)
          })
          .collect.toSet

        LOG.info( "Probeset reduced to {} was {}", newProbes.size, probeSet.size )

        //filter the data matrix
        pData.get.filter(x => newProbes.contains(x._2))
      }
    }
  }

}
