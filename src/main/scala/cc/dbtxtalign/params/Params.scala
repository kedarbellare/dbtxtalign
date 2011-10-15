package cc.dbtxtalign.params

import cc.refectorie.user.kedarb.dynprog.AParams
import cc.refectorie.user.kedarb.dynprog.types.{Indexer, ParamVec}
import cc.refectorie.user.kedarb.dynprog.utils.Utils._

/**
 * @author kedarb
 * @since 5/10/11
 */

object MyParamUtils {
  def myfmt(v: Double) = "%.3f".format(v)
}

class TransitionParams(val labelIndexer: Indexer[String],
                       val starts: ParamVec,
                       val transitions: Array[ParamVec]) extends AParams {
  def foreachVec(f: (ParamVec) => Any): Unit = {
    f(starts)
    transitions.foreach(f(_))
  }

  def output(puts: (String) => Any): Unit = {
    import MyParamUtils._
    puts("")
    foreachIndex(getValues(starts), {
      (a: Int, v: Double) => if (v != 0) puts("S\t%s\t%s".format(labelIndexer(a), myfmt(v)))
    })
    foreachIndex(transitions, {
      (a: Int, pv: ParamVec) =>
        puts("")
        foreachSortedIndex(getValues(pv), {
          (b: Int, v: Double) =>
            if (v != 0) puts("T\t%s\t%s\t%s".format(labelIndexer(a), labelIndexer(b), myfmt(v)))
        })
    })
  }
}

class EmissionParams(val labelIndexer: Indexer[String],
                     val featureIndexer: Indexer[String],
                     val emissions: Array[ParamVec]) extends AParams {
  def foreachVec(f: (ParamVec) => Any): Unit = {
    emissions.foreach(f(_))
  }

  def output(puts: (String) => Any): Unit = {
    import MyParamUtils._
    foreachIndex(emissions, {
      (a: Int, pv: ParamVec) =>
        puts("")
        foreachSortedIndex(getValues(pv), {
          (i: Int, v: Double) =>
            if (v != 0) puts("E\t%s\t%s\t%s".format(labelIndexer(a), featureIndexer(i), myfmt(v)))
        })
    })
  }
}

class AlignParams(val labelIndexer: Indexer[String],
                  val alignFeatureIndexer: Indexer[String],
                  val labelAligns: Array[ParamVec]) extends AParams {
  def foreachVec(f: (ParamVec) => Any) {
    labelAligns.foreach(f(_))
  }

  def output(puts: (String) => Any) {
    import MyParamUtils._
    foreachIndex(labelAligns, {
      (a: Int, pv: ParamVec) =>
        puts("")
        foreachSortedIndex(getValues(pv), {
          (i: Int, v: Double) =>
            if (v != 0) puts("A\t%s\t%s\t%s".format(labelIndexer(a), alignFeatureIndexer(i), myfmt(v)))
        })
    })
  }
}

class SegmentParams(val transitions: TransitionParams, val emissions: EmissionParams) extends AParams {
  def output(puts: (String) => Any) {
    transitions.output(puts)
    emissions.output(puts)
  }

  def foreachVec(f: (ParamVec) => Any) {
    transitions.foreachVec(f)
    emissions.foreachVec(f)
  }
}

class Params(val transitions: TransitionParams, val emissions: EmissionParams, val aligns: AlignParams)
  extends AParams {
  def output(puts: (String) => Any) {
    transitions.output(puts)
    emissions.output(puts)
    aligns.output(puts)
  }

  def foreachVec(f: (ParamVec) => Any) {
    transitions.foreachVec(f)
    emissions.foreachVec(f)
    aligns.foreachVec(f)
  }

  def getSegmentParams: SegmentParams = new SegmentParams(transitions, emissions)

  def getAlignParams: AlignParams = aligns
}

class ConstraintParams(val constraintFeatureIndexer: Indexer[String],
                       val constraints: ParamVec) extends AParams {
  def foreachVec(f: (ParamVec) => Any) {
    f(constraints)
  }

  def output(puts: (String) => Any) {
    import MyParamUtils._
    puts("")
    foreachSortedIndex(getValues(constraints), {
      (i: Int, v: Double) =>
        if (v != 0) puts("C\t%s\t%s".format(constraintFeatureIndexer(i), myfmt(v)))
    })
  }
}

class NoopParams extends AParams {
  def foreachVec(f: (ParamVec) => Any) {}

  def output(puts: (String) => Any) {}
}