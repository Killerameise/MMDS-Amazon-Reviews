package de.hpi.mmds.clustering

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ConnectedComponents
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD

/**
 * Created by axel on 07.07.16.
 */
object graphops {
  def getConnectedComponents(edges: RDD[MatrixEntry]): JavaRDD[(Long, Long)] = {
    //val verts: RDD[Long] = edges.flatMap(tuple => mutable.LinkedList(tuple.i, tuple.j)).distinct()
    val edges2: RDD[(VertexId, VertexId)] = edges.map(edge => new Tuple2(edge.i, edge.j))
    val graph = Graph.fromEdgeTuples(edges2, 1)
    val cc = ConnectedComponents.run(graph).vertices
    return cc.toJavaRDD()
  }
}
