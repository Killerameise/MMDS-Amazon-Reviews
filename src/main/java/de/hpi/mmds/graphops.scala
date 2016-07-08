package de.hpi.mmds

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.graphx.lib.ConnectedComponents
import org.apache.spark.graphx.{VertexRDD, Edge, Graph, VertexId}
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Created by axel on 07.07.16.
 */
object graphops {
  def getConnectedComponents(edges: RDD[MatrixEntry]) = {
    //val verts: RDD[Long] = edges.flatMap(tuple => mutable.LinkedList(tuple.i, tuple.j)).distinct()
    val edges2: RDD[(VertexId, VertexId)] = edges.map(edge => new Tuple2(edge.i, edge.j))
    val graph  = Graph.fromEdgeTuples(edges2, 1)
    val cc = ConnectedComponents.run(graph)
    val aggregated = cc

  }

}
