package cc.dbtxtalign

import cc.refectorie.user.kedarb.dynprog.AParams
import cc.refectorie.user.kedarb.dynprog.types.Hypergraph
import cc.refectorie.user.kedarb.dynprog.segment.{Segment, Segmentation}
import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import collection.mutable.HashSet

/**
 * @author kedar
 */

trait AMatchSegmentationBasedInferencer[Feature, Example <: AFeatAlignmentMentionExample[Feature], Params <: AParams]
  extends ASegmentationBasedInferencer[Feature, MatchSegmentation, Example, Params] {
  type Widget = MatchSegmentation

  lazy val otherIds: Seq[String] = ex.otherIds

  lazy val otherWordsSeq: Seq[Seq[String]] = ex.otherWordsSeq

  lazy val otherSegmentations: Seq[Segmentation] = ex.otherSegmentations

  def scoreSimilarity(otherIndex: Int, b: Int, i: Int, j: Int, oi: Int, oj: Int): Double

  def updateSimilarity(otherIndex: Int, b: Int, i: Int, j: Int, oi: Int, oj: Int, v: Double)

  def scoreMatch(otherIndex: Int): Double = 0.0

  def updateMatch(otherIndex: Int, v: Double) {}

  def newWidget = new Widget(new HashSet[String], new Segmentation(N))

  def trueMatchInfer: Boolean

  def trueSegmentInfer: Boolean

  def allowedMatch(otherId: Option[String]): Boolean = {
    if (!trueInfer && !trueMatchInfer) true
    else {
      if (otherId.isDefined) ex.trueWidget.matchIds(otherId.get)
      else ex.trueWidget.matchIds.isEmpty
    }
  }

  override def allowedSegment(a: Int, i: Int, j: Int): Boolean = {
    if (trueSegmentInfer) {
      allowedSegmentTruth(a, i, j)
    } else {
      super.allowedSegment(a, i, j)
    }
  }

  def createHypergraph(H: Hypergraph[Widget]) {
    def genMatchTransition(otherIndex: Int, a: Int, i: Int): Object = {
      if (i == N) H.endNode
      else {
        val node = (otherIndex, a, i)
        if (H.addSumNode(node)) {
          genTransitionSegments(a, i, (segment: Segment) => {
            require(segment.begin == i, "Invalid begin index " + segment.begin + " != " + i + " for transition!")
            val b = segment.label
            val j = segment.end
            val segmentEmitScore = scoreTransition(a, b, i, j) + scoreEmission(b, i, j)

            def addEdgeWithMatch(oi: Int, oj: Int) {
              H.addEdge(node, genMatchTransition(otherIndex, b, j), new Info {
                def getWeight = segmentEmitScore + {
                  if (otherIndex >= 0 && otherIndex < otherIds.size) scoreSimilarity(otherIndex, b, i, j, oi, oj)
                  else 0.0
                }

                def setPosterior(prob: Double) {
                  updateTransition(a, b, i, j, prob)
                  updateEmission(b, i, j, prob)
                  if (otherIndex >= 0 && otherIndex < otherIds.size) updateSimilarity(otherIndex, b, i, j, oi, oj, prob)
                }

                def choose(widget: MatchSegmentation) = {
                  require(widget.segmentation.append(segment), "Could not add segment: " + segment)
                  widget
                }
              })
            }

            // align with NULL segment
            addEdgeWithMatch(0, 0)

            // align with otherId's segments
            if (otherIndex >= 0 && otherIndex < otherSegmentations.size)
              forIndex(otherSegmentations(otherIndex).numSegments, (k: Int) => {
                if (otherSegmentations(otherIndex).segment(k).label == b) {
                  val otherSegment = otherSegmentations(otherIndex).segment(k)
                  addEdgeWithMatch(otherSegment.begin, otherSegment.end)
                }
              })
          })
        }
        node
      }
    }

    def genMatchStart(otherIndex: Int): Object = {
      val node = (otherIndex, 0)
      if (H.addSumNode(node)) {
        genStartSegments((segment: Segment) => {
          require(segment.begin == 0, "Invalid begin index at start!")
          val b = segment.label
          val i = segment.begin
          val j = segment.end
          val segmentEmitScore = scoreStart(b, j) + scoreEmission(b, i, j)

          def addEdgeWithMatch(oi: Int, oj: Int) {
            if (allowedSegment(b, i, j)) H.addEdge(node, genMatchTransition(otherIndex, b, j), new Info {
              def getWeight = segmentEmitScore + {
                if (otherIndex >= 0 && otherIndex < otherIds.size) scoreSimilarity(otherIndex, b, i, j, oi, oj)
                else 0.0
              }

              def setPosterior(prob: Double) {
                updateStart(b, j, prob)
                updateEmission(b, i, j, prob)
                if (otherIndex >= 0 && otherIndex < otherIds.size) updateSimilarity(otherIndex, b, i, j, oi, oj, prob)
              }

              def choose(widget: MatchSegmentation) = {
                require(widget.segmentation.append(segment), "Could not add segment: " + segment)
                widget
              }
            })
          }

          // align with NULL segment
          addEdgeWithMatch(0, 0)

          // align with otherId's segments
          if (otherIndex >= 0 && otherIndex < otherSegmentations.size)
            forIndex(otherSegmentations(otherIndex).numSegments, (k: Int) => {
              if (otherSegmentations(otherIndex).segment(k).label == b) {
                val otherSegment = otherSegmentations(otherIndex).segment(k)
                addEdgeWithMatch(otherSegment.begin, otherSegment.end)
              }
            })
        })
      }
      node
    }

    for (otherIndex <- -1 until otherIds.size) {
      val otherIdOpt = if (otherIndex < 0) None else Some(otherIds(otherIndex))
      if (allowedMatch(otherIdOpt)) {
        H.addEdge(H.sumStartNode(), genMatchStart(otherIndex), new Info {
          def getWeight = scoreMatch(otherIndex)

          def setPosterior(prob: Double) {
            updateMatch(otherIndex, prob)
          }

          def choose(widget: MatchSegmentation) = {
            if (otherIndex >= 0) widget.matchIds += otherIds(otherIndex)
            widget
          }
        })
      }
    }
  }
}