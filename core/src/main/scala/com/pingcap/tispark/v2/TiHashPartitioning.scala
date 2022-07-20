package com.pingcap.tispark.v2

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Murmur3Hash, Pmod}
import org.apache.spark.sql.catalyst.plans.physical.{
  AllTuples,
  ClusteredDistribution,
  Distribution,
  HashClusteredDistribution,
  HashPartitioning,
  UnspecifiedDistribution
}
import org.apache.spark.sql.types.{DataType, IntegerType}

class TiHashPartitioning(expressions: Seq[Expression], numPartitions: Int)
    extends HashPartitioning(expressions: Seq[Expression], numPartitions: Int) {

  override def children: Seq[Expression] = expressions
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def satisfies0(required: Distribution): Boolean = {
    required match {
      case UnspecifiedDistribution => true
      case AllTuples => numPartitions == 1
      case h: HashClusteredDistribution => {
        println("Hash")
        expressions.length == h.expressions.length && expressions.zip(h.expressions).exists {
          case (l, r) => l.semanticEquals(r)
        }
      }
      case ClusteredDistribution(requiredClustering, _) => {
        println("Cluster")
        expressions.exists(x => requiredClustering.exists(_.semanticEquals(x)))
      }
      case _ => false
    }
  }

  /**
   * Returns an expression that will produce a valid partition ID(i.e. non-negative and is less
   * than numPartitions) based on hashing expressions.
   */
  override def partitionIdExpression: Expression =
    Pmod(new Murmur3Hash(expressions), Literal(numPartitions))

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): HashPartitioning = copy(expressions = newChildren)
}
