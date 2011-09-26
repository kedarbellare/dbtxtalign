package cc.dbtxtalign

import cc.refectorie.user.kedarb.dynprog.AParams
import cc.refectorie.user.kedarb.dynprog.types.Hypergraph
import cc.refectorie.user.kedarb.dynprog.segment.{Segment, Segmentation}
import cc.refectorie.user.kedarb.dynprog.utils.Utils._

/**
 * @author kedar
 */


trait AMatchSegmentationBasedInferencer[Feature, Example <: AFeatAlignmentMentionExample[Feature], Params <: AParams]
  extends ASegmentationBasedInferencer[Feature, MatchSegmentation, Example, Params] {
  type Widget = MatchSegmentation

  lazy val otherId: String = ex.otherId

  lazy val otherWords: Seq[String] = ex.otherWords

  lazy val otherSegmentation: Segmentation = ex.otherSegmentation

  def scoreSimilarity(a: Int, phrase: Seq[String], otherPhrase: Seq[String]): Double

  def updateSimilarity(a: Int, phrase: Seq[String], otherPhrase: Seq[String], v: Double)

  def scoreSimilarity(a: Int, i: Int, j: Int, otherPhrase: Seq[String]): Double =
    scoreSimilarity(a, words.slice(i, j), otherPhrase)

  def updateSimilarity(a: Int, i: Int, j: Int, otherPhrase: Seq[String], v: Double) {
    updateSimilarity(a, words.slice(i, j), otherPhrase, v)
  }

  def scoreMatch(isMatch: Boolean): Double = 0.0

  def updateMatch(isMatch: Boolean, v: Double) {}

  def newWidget = new Widget(false, new Segmentation(N))

  def trueMatchInfer: Boolean

  def trueSegmentInfer: Boolean

  def allowedMatch(isMatch: Boolean): Boolean = (!trueInfer && !trueMatchInfer) || isMatch == ex.trueWidget.isMatch

  override def allowedSegment(a: Int, i: Int, j: Int): Boolean = {
    if (trueSegmentInfer) {
      allowedSegmentTruth(a, i, j)
    } else {
      super.allowedSegment(a, i, j)
    }
  }

  def createHypergraph(H: Hypergraph[Widget]) {
    def genMatchSegment(isMatch: Boolean, a: Int, i: Int): Object = {
      if (i == N) H.endNode
      else {
        val node = (isMatch, a, i)
        if (H.addSumNode(node)) {
          genTransitionSegments(a, i, (segment: Segment) => {
            require(segment.begin == i, "Invalid begin index " + segment.begin + " != " + i + " for transition!")
            val b = segment.label
            val j = segment.end
            val segmentEmitScore = scoreTransition(a, b, i, j) + scoreEmission(b, i, j)

            def addEdgeWithMatch(otherPhrase: Seq[String]) {
              if (allowedSegment(b, i, j)) H.addEdge(node, genMatchSegment(isMatch, b, j), new Info {
                def getWeight = segmentEmitScore + {
                  if (isMatch) scoreSimilarity(b, i, j, otherPhrase)
                  else 0.0
                }

                def setPosterior(prob: Double) {
                  updateTransition(a, b, i, j, prob)
                  updateEmission(b, i, j, prob)
                  if (isMatch) updateSimilarity(b, i, j, otherPhrase, prob)
                }

                def choose(widget: MatchSegmentation) = {
                  require(widget.segmentation.append(segment), "Could not add segment: " + segment)
                  widget
                }
              })
            }

            // align with NULL segment
            addEdgeWithMatch(Seq.empty[String])

            // align with otherId's segments
            forIndex(otherSegmentation.numSegments, (k: Int) => {
              if (otherSegmentation.segment(k).label == b) {
                val otherSegment = otherSegmentation.segment(k)
                val otherPhrase = otherWords.slice(otherSegment.begin, otherSegment.end)
                addEdgeWithMatch(otherPhrase)
              }
            })
          })
        }
        node
      }
    }

    def genMatch(isMatch: Boolean): Object = {
      val node = (isMatch, 0)
      if (H.addSumNode(node)) {
        genStartSegments((segment: Segment) => {
          require(segment.begin == 0, "Invalid begin index at start!")
          val a = segment.label
          val i = segment.begin
          val j = segment.end
          val segmentEmitScore = scoreStart(a, j) + scoreEmission(a, i, j)

          def addEdgeWithMatch(otherPhrase: Seq[String]) {
            if (allowedSegment(a, i, j)) H.addEdge(node, genMatchSegment(isMatch, a, j), new Info {
              def getWeight = segmentEmitScore + {
                if (isMatch) scoreSimilarity(a, i, j, otherPhrase)
                else 0.0
              }

              def setPosterior(prob: Double) {
                updateStart(a, j, prob)
                updateEmission(a, i, j, prob)
                if (isMatch) updateSimilarity(a, i, j, otherPhrase, prob)
              }

              def choose(widget: MatchSegmentation) = {
                require(widget.segmentation.append(segment), "Could not add segment: " + segment)
                widget
              }
            })
          }

          // align with NULL segment
          addEdgeWithMatch(Seq.empty[String])

          // align with otherId's segments
          forIndex(otherSegmentation.numSegments, (k: Int) => {
            if (otherSegmentation.segment(k).label == a) {
              val otherSegment = otherSegmentation.segment(k)
              val otherPhrase = otherWords.slice(otherSegment.begin, otherSegment.end)
              addEdgeWithMatch(otherPhrase)
            }
          })
        })
      }
      node
    }

    for (isMatch <- Seq(true, false)) {
      if (allowedMatch(isMatch)) H.addEdge(H.sumStartNode(), genMatch(isMatch), new Info {
        def getWeight = scoreMatch(isMatch)

        def setPosterior(prob: Double) {
          updateMatch(isMatch, prob)
        }

        def choose(widget: MatchSegmentation) = {
          widget.isMatch = isMatch
          widget
        }
      })
    }
  }
}