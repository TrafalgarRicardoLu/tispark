package org.apache.spark.sql

import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{
  Expression,
  NullOrdering,
  SortDirection,
  SortOrder,
  FieldReference
}
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering

class DistributedWrite(numPartitions: Int) extends RequiresDistributionAndOrdering {
  override def requiredDistribution(): Distribution = {
    val cluster = Array[Expression](FieldReference("i"), FieldReference("s"))
    val order =
      Array[SortOrder](TiSortOrder(FieldReference("i")), TiSortOrder(FieldReference("s")))

    Distributions.clustered(cluster)
  }

  override def requiredOrdering(): Array[SortOrder] = {
    val order =
      Array[SortOrder](TiSortOrder(FieldReference("i")), TiSortOrder(FieldReference("s")))
    order
  }

  override def requiredNumPartitions(): Int = {
    numPartitions
  }
}

case class TiSortOrder(expr: Expression) extends SortOrder {

  override def expression(): Expression = expr

  override def direction(): SortDirection = SortDirection.ASCENDING

  override def nullOrdering(): NullOrdering = NullOrdering.NULLS_FIRST

  override def describe(): String = {
    expr.toString
  }
}
