package cc.dbtxtalign

import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.refectorie.user.kedarb.dynprog.segment.{Segment, Segmentation}
import cc.refectorie.user.kedarb.dynprog.{InferSpec}
import cc.refectorie.user.kedarb.dynprog.types.{FtrVec, Hypergraph, Indexer}

import params.SegmentParams

/**
 * @author kedar
 */

trait ASegmentationInferencer[Feature, Example <: AFeatMentionExample[Feature]]
  extends ASegmentationBasedInferencer[Feature, Segmentation, Example, SegmentParams] {
  type Widget = Segmentation

  // allowed segment for span [i, j) for label a
  def newWidget = new Segmentation(N)

  override def transitionParams = params.transitions.transitions

  override def transitionCounts = counts.transitions.transitions

  override def startParams = params.transitions.starts

  override def startCounts = counts.transitions.starts

  override def emissionParams = params.emissions.emissions

  override def emissionCounts = counts.emissions.emissions

  def createHypergraph(H: Hypergraph[Segmentation]) {
    def gen(a: Int, i: Int): Object = {
      if (i == N) H.endNode
      else {
        val node = (a, i)
        if (H.addSumNode(node)) {
          genTransitionSegments(a, i, (segment: Segment) => {
            require(segment.begin == i, "Invalid begin index " + segment.begin + " != " + i + " for transition!")
            val b = segment.label
            val j = segment.end
            H.addEdge(node, gen(b, j), new Info {
              def getWeight = scoreTransition(a, b, i, j) + scoreEmission(b, i, j)

              def setPosterior(v: Double) {
                updateTransition(a, b, i, j, v)
                updateEmission(b, i, j, v)
              }

              def choose(widget: Widget) = {
                require(widget.append(segment), "Could not add segment: " + segment)
                widget
              }
            })
          })
        }
        node
      }
    }

    genStartSegments((segment: Segment) => {
      require(segment.begin == 0, "Invalid begin index at start!")
      val a = segment.label
      val i = segment.begin
      val j = segment.end
      H.addEdge(H.sumStartNode(), gen(a, j), new Info {
        def getWeight = scoreStart(a, j) + scoreEmission(a, i, j)

        def setPosterior(v: Double) {
          updateStart(a, j, v)
          updateEmission(a, i, j, v)
        }

        def choose(widget: Widget) = {
          require(widget.append(segment), "Could not add segment: " + segment)
          widget
        }
      })
    })
  }

  override def updateCounts {
    super.updateCounts
    counts.synchronized {
      forIndex(L, (a: Int) => {
        forIndex(N, (k: Int) => {
          updateSingleEmissionCached(a, k, cachedEmissionCounts(a)(k))
        })
      })
    }
  }
}

class HMMSegmentationInferencer(val labelIndexer: Indexer[String], val maxLengths: Seq[Int], val ex: FeatMentionExample,
                                val params: SegmentParams, val counts: SegmentParams, val ispec: InferSpec)
  extends ASegmentationInferencer[Int, FeatMentionExample] {
  lazy val featSeq: Seq[Int] = ex.featSeq

  def scoreSingleEmission(a: Int, k: Int) = score(emissionParams(a), featSeq(k))

  def updateSingleEmissionCached(a: Int, k: Int, x: Double) {
    update(emissionCounts(a), featSeq(k), x)
  }
}

class CRFSegmentationInferencer(val labelIndexer: Indexer[String], val maxLengths: Seq[Int], val ex: FeatVecMentionExample,
                                val params: SegmentParams, val counts: SegmentParams, val ispec: InferSpec)
  extends ASegmentationInferencer[FtrVec, FeatVecMentionExample] {
  lazy val featSeq: Seq[FtrVec] = ex.featSeq

  def scoreSingleEmission(a: Int, k: Int) = score(emissionParams(a), featSeq(k))

  def updateSingleEmissionCached(a: Int, k: Int, x: Double) {
    update(emissionCounts(a), featSeq(k), x)
  }
}
