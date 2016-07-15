package de.hpi.mmds.clustering

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
/**
 * Taken and slightly adapted from http://stackoverflow.com/a/31862441
 */
object transpose {
  def transposeRowMatrix(m: IndexedRowMatrix): RowMatrix = {
    val transposedRowsRDD = m.rows.map{case (row: IndexedRow)  => rowToTransposedTriplet(row.vector, row.index)}
      .flatMap(x => x) // now we have triplets (newRowIndex, (newColIndex, value))
      .groupByKey()
      .sortByKey().map(_._2) // sort rows and remove row indexes
      .map(buildRow) // restore order of elements in each row and remove column indexes
    new RowMatrix(transposedRowsRDD)
  }


  def rowToTransposedTriplet(row: Vector, rowIndex: Long): Array[(Long, (Long, Double))] = {
    val indexedRow = row.toArray.zipWithIndex
    indexedRow.map{case (value: Double, colIndex) => (colIndex.toLong, (rowIndex, value))}
  }

  def buildRow(rowWithIndexes: Iterable[(Long, Double)]): Vector = {
    val resArr = new Array[Double](rowWithIndexes.size)
    rowWithIndexes.foreach{case (index, value) =>
      resArr(index.toInt) = value
    }
    Vectors.dense(resArr)
  }
}
