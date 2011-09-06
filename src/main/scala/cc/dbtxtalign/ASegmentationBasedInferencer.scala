package cc.dbtxtalign

import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.refectorie.user.kedarb.dynprog.segment.{Segment, Segmentation}
import cc.refectorie.user.kedarb.dynprog.{InferSpec, AHypergraphInferState}
import params.SegmentParams
import cc.refectorie.user.kedarb.dynprog.types.{FtrVec, Hypergraph, Indexer}

/**
 * @author kedar
 */

trait ASegmentationBasedInferencer[Feature, Example <: AFeatMentionExample[Feature]]
  extends AHypergraphInferState[Segmentation, Example, SegmentParams] {
  type Widget = Segmentation

  def labelIndexer: Indexer[String]

  def maxLengths: Seq[Int]

  lazy val L: Int = labelIndexer.size

  lazy val otherLabelIndex: Int = labelIndexer.indexOf_!("O")

  lazy val N: Int = ex.numTokens

  lazy val isRecord: Boolean = ex.isRecord

  lazy val words: Seq[String] = ex.words

  lazy val trueSegmentation: Segmentation = ex.trueSegmentation

  lazy val cachedEmissionScores: Array[Array[Double]] = mapIndex(L, (l: Int) => mapIndex(N, (n: Int) => Double.NaN))

  lazy val cachedEmissionCounts: Array[Array[Double]] = mapIndex(L, (l: Int) => mapIndex(N, (n: Int) => 0.0))

  lazy val cachedPossibleEnds: Array[Boolean] = mapIndex(N + 1, (j: Int) => ex.isPossibleEnd(j))

  // allowed segment for span [i, j) for label a
  def newWidget = new Segmentation(N)

  def allowedSegment(a: Int, i: Int, j: Int): Boolean = {
    if (trueInfer) trueSegmentation.contains(Segment(i, j, a))
    else {
      if (isRecord) {
        // only allow sub-segments that are a subset of segment at i
        val optionSegmentAtI = trueSegmentation.segmentAt(i)
        optionSegmentAtI.isDefined && optionSegmentAtI.get.end >= j &&
          (a == otherLabelIndex || cachedPossibleEnds(j) || optionSegmentAtI.get.end == j)
      } else {
        // allow all possible segments
        a == otherLabelIndex || cachedPossibleEnds(j)
      }
    }
  }

  def allowedStart(a: Int): Boolean = true

  def allowedTransition(a: Int, b: Int): Boolean = true

  def transitionParams = params.transitions.transitions

  def transitionCounts = counts.transitions.transitions

  def startParams = params.transitions.starts

  def startCounts = counts.transitions.starts

  def emissionParams = params.emissions.emissions

  def emissionCounts = counts.emissions.emissions

  def scoreTransition(a: Int, b: Int, i: Int, j: Int): Double = {
    if (isRecord) 0.0
    else score(transitionParams(a), b)
  }

  def scoreStart(a: Int, j: Int): Double = {
    if (isRecord) 0.0
    else score(startParams, a)
  }

  def scoreSingleEmission(a: Int, k: Int): Double

  def updateTransition(a: Int, b: Int, i: Int, j: Int, x: Double) {
    if (!isRecord) update(transitionCounts(a), b, x)
  }

  def updateStart(a: Int, j: Int, x: Double) {
    if (!isRecord) update(startCounts, a, x)
  }

  def updateSingleEmission(a: Int, k: Int, x: Double) {
    cachedEmissionCounts(a)(k) += x
  }

  def scoreEmission(a: Int, i: Int, j: Int): Double = {
    var sumScore = 0.0
    forIndex(i, j, (k: Int) => {
      if (cachedEmissionScores(a)(k).isNaN) cachedEmissionScores(a)(k) = scoreSingleEmission(a, k)
      sumScore += cachedEmissionScores(a)(k)
    })
    sumScore
  }

  def updateEmission(a: Int, i: Int, j: Int, x: Double) {
    forIndex(i, j, (k: Int) => updateSingleEmission(a, k, x))
  }

  def updateSingleEmissionCached(a: Int, k: Int, x: Double): Unit

  def createHypergraph(H: Hypergraph[Segmentation]) {
    def gen(a: Int, i: Int): Object = {
      if (i == N) H.endNode
      else {
        val node = (a, i)
        if (H.addSumNode(node)) {
          forIndex(L, (b: Int) => {
            forIndex(i + 1, math.min(i + maxLengths(b), N) + 1, (j: Int) => {
              if (allowedTransition(a, b) && allowedSegment(b, i, j)) {
                H.addEdge(node, gen(b, j), new Info {
                  def getWeight = scoreTransition(a, b, i, j) + scoreEmission(b, i, j)

                  def setPosterior(v: Double) {
                    updateTransition(a, b, i, j, v)
                    updateEmission(b, i, j, v)
                  }

                  def choose(widget: Widget) = {
                    val seg = Segment(i, j, b)
                    require(widget.append(seg), "Could not add segment: " + seg)
                    widget
                  }
                })
              }
            })
          })
        }
        node
      }
    }

    forIndex(L, (a: Int) => {
      forIndex(1, math.min(maxLengths(a), N) + 1, (i: Int) => {
        if (allowedStart(a) && allowedSegment(a, 0, i)) {
          H.addEdge(H.sumStartNode, gen(a, i), new Info {
            def getWeight = scoreStart(a, i) + scoreEmission(a, 0, i)

            def setPosterior(v: Double) {
              updateStart(a, i, v)
              updateEmission(a, 0, i, v)
            }

            def choose(widget: Widget) = {
              val seg = Segment(0, i, a)
              require(widget.append(seg), "Could not add segment: " + seg)
              widget
            }
          })
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
  extends ASegmentationBasedInferencer[Int, FeatMentionExample] {
  lazy val featSeq: Seq[Int] = ex.featSeq

  def scoreSingleEmission(a: Int, k: Int) = score(emissionParams(a), featSeq(k))

  def updateSingleEmissionCached(a: Int, k: Int, x: Double) {
    update(emissionCounts(a), featSeq(k), x)
  }
}

class CRFSegmentationInferencer(val labelIndexer: Indexer[String], val maxLengths: Seq[Int], val ex: FeatVecMentionExample,
                                val params: SegmentParams, val counts: SegmentParams, val ispec: InferSpec)
  extends ASegmentationBasedInferencer[FtrVec, FeatVecMentionExample] {
  lazy val featSeq: Seq[FtrVec] = ex.featSeq

  def scoreSingleEmission(a: Int, k: Int) = score(emissionParams(a), featSeq(k))

  def updateSingleEmissionCached(a: Int, k: Int, x: Double) {
    update(emissionCounts(a), featSeq(k), x)
  }
}
