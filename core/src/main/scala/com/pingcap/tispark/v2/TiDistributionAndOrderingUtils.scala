package org.apache.spark.sql.execution.datasources.v2

import com.pingcap.tispark.v2.TiHashPartitioning
import org.apache.spark.sql.catalyst.expressions.{Expression, SortOrder}
import org.apache.spark.sql.catalyst.expressions.V2ExpressionUtils.toCatalyst
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.plans.physical.{
  Partitioning,
  RangePartitioning,
  HashPartitioning,
  RoundRobinPartitioning,
  SinglePartition
}
import org.apache.spark.sql.connector.distributions.{
  ClusteredDistribution,
  OrderedDistribution,
  UnspecifiedDistribution
}
import org.apache.spark.sql.connector.write.{RequiresDistributionAndOrdering, Write}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression

object TiDistributionAndOrderingUtils {

  def prepareQuery(write: Write, query: LogicalPlan, conf: SQLConf): LogicalPlan =
    write match {
      case write: RequiresDistributionAndOrdering =>
        val numPartitions = write.requiredNumPartitions()
        val distribution = write.requiredDistribution match {
          case d: OrderedDistribution => d.ordering.map(e => toCatalyst(e, query))
          case d: ClusteredDistribution => d.clustering.map(e => toCatalyst(e, query))
          case _: UnspecifiedDistribution => Array.empty[Expression]
        }

        val queryWithDistribution = if (distribution.nonEmpty) {
          val finalNumPartitions = if (numPartitions > 0) {
            numPartitions
          } else {
            conf.numShufflePartitions
          }
          // the conversion to catalyst expressions above produces SortOrder expressions
          // for OrderedDistribution and generic expressions for ClusteredDistribution
          // this allows RepartitionByExpression to pick either range or hash partitioning
          TiRepartitionByExpression(distribution, query, finalNumPartitions)
        } else if (numPartitions > 0) {
          throw QueryCompilationErrors
            .numberOfPartitionsNotAllowedWithUnspecifiedDistributionError()
        } else {
          query
        }

        val ordering = write.requiredOrdering.toSeq
          .map(e => toCatalyst(e, query))
          .asInstanceOf[Seq[SortOrder]]

        val queryWithDistributionAndOrdering = if (ordering.nonEmpty) {
          Sort(ordering, global = false, queryWithDistribution)
        } else {
          queryWithDistribution
        }

        queryWithDistributionAndOrdering

      case _ =>
        query
    }
}

object TiRepartitionByExpression {
  def apply(
      partitionExpressions: Seq[Expression],
      child: LogicalPlan,
      numPartitions: Int): RepartitionByExpression = {
    new TiRepartitionByExpression(partitionExpressions, child, Some(numPartitions))
  }
}

class TiRepartitionByExpression(
    partitionExpressions: Seq[Expression],
    child: LogicalPlan,
    optNumPartitions: Option[Int])
    extends RepartitionByExpression(
      partitionExpressions: Seq[Expression],
      child: LogicalPlan,
      optNumPartitions: Option[Int]) {
  override val partitioning: Partitioning = {
    val (sortOrder, nonSortOrder) = partitionExpressions.partition(_.isInstanceOf[SortOrder])

    require(
      sortOrder.isEmpty || nonSortOrder.isEmpty,
      s"${getClass.getSimpleName} expects that either all its `partitionExpressions` are of type " +
        "`SortOrder`, which means `RangePartitioning`, or none of them are `SortOrder`, which " +
        "means `HashPartitioning`. In this case we have:" +
        s"""
           |SortOrder: $sortOrder
           |NonSortOrder: $nonSortOrder
       """.stripMargin)

    if (numPartitions == 1) {
      SinglePartition
    } else if (sortOrder.nonEmpty) {
      println("Sort")
      RangePartitioning(sortOrder.map(_.asInstanceOf[SortOrder]), numPartitions)
    } else if (nonSortOrder.nonEmpty) {
      println("Hashing")
      new TiHashPartitioning(nonSortOrder, numPartitions)
    } else {
      RoundRobinPartitioning(numPartitions)
    }
  }

  override def shuffle: Boolean = true

  override protected def withNewChildInternal(newChild: LogicalPlan): RepartitionByExpression =
    copy(child = newChild)
}
