package Farseer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable

class FeatureSelection {
  private def getAllFeatureIndices(len: Int): Array[Int] = {
    val ret = new Array[Int](len)

    var i = 0
    while (i < len) {
      ret(i) = i

      i += 1
    }

    ret
  }

  def ChiSquareMulti(sc: SparkContext, data: RDD[String], indices: Array[Int] = Array[Int](), delimiter: String = ","): Array[(Int, Int, Double)] = {
    // 1. couting samples
    var isRelease = false
    if (data.getStorageLevel == StorageLevel.NONE) {isRelease = true; data.cache()}
    val nFeatures = data.first.split(delimiter).length - 1
    val _indices = if (indices.nonEmpty) indices else getAllFeatureIndices(data.first.split(delimiter).length - 1)

    // 2. transform data to <featureIndex, featureValue, label>
    val flatXY = data.filter(_.split(delimiter).length == nFeatures + 1).map{ line =>
      val features = line.trim.split(delimiter)
      val label = features(features.length - 1)
      val res = mutable.ArrayBuffer[(Int, String, String)]()
      for (idx <- _indices) {
        res += ((idx, features(idx), label))
      }
      res += ((features.length - 1, label, label))
      res += ((features.length, "COUNTER_TRICK", "COUNTER_TRICK"))

      res.toArray
    }

    // 3. compute marginal counter: c(x) & c(y)
    val cXorY = flatXY
      .flatMap(arr => arr.map{case (i, x, y) => ((i, x), 1)})
      .reduceByKey(_ + _)

    val degreeFreedom = cXorY.map{ case (((i, x), cnt)) => (i, 1)}.reduceByKey(_ + _)

    val bcXorY = sc.broadcast(cXorY.collectAsMap)
    val bDegreeFreedom = sc.broadcast(degreeFreedom.collectAsMap)

    // 4. compute chi square: sum((observe_ij - expect_ij)^2 / expect_ij)
    val chiSquare = flatXY
      .flatMap(arr => arr.map{case (i, x, y) => ((i, x, y), 1)})
      .reduceByKey(_ + _)
      .filter(_._1._1 < nFeatures)
      .map{ case (((i, x, y), cxy)) =>
        val _cXorY = bcXorY.value
        val _degreeFreedom = bDegreeFreedom.value
        val _cx = _cXorY((i, x))
        val _cy = _cXorY((nFeatures, y))
        val _cn = _cXorY((nFeatures + 1, "COUNTER_TRICK"))
        val _cxy = if (i >= nFeatures) 0.0 else cxy

        val _exy = _cx.toDouble * _cy / _cn
        val curChiSquare = math.pow(_cxy - _exy, 2) / _exy
        val df = (_degreeFreedom(i) - 1) * (_degreeFreedom(nFeatures) - 1)
        ((i, df), curChiSquare)
      }
      .reduceByKey(_ + _)
      .map{case (((i: Int, df: Int), chi: Double)) => (i, df, chi)}
      .collect()

    // 5. release cached internal RDDs
    if (isRelease) data.unpersist()
    bcXorY.unpersist()
    bDegreeFreedom.unpersist()

    chiSquare
  }

  def ChiSquareOHE(data: RDD[String], indices: Array[Int], delimiter: String = ",", label_index: Int = 0): RDD[(Int, Double)] = {
    val N = data.count()

    val chi = data.flatMap{line =>
      val feature = line.split(delimiter)
      val label = feature(label_index).toInt
      // map to (i, (A, B, C, D)) where i is column number of feature
      // set A, B, C, D according to (feature, label)
      // (1, 1) => (1, 0, 0, 0), (1, 0) => (0, 1, 0, 0)
      // (0, 1) => (0, 0, 1, 0), (0, 0) => (0, 0, 0, 0)
      indices.map { i =>
        val x = feature(i).toInt
        val y = label
        (x, y) match {
          case (1, 1) => (i, (1, 0, 0, 0))
          case (1, 0) => (i, (0, 1, 0, 0))
          case (0, 1) => (i, (0, 0, 1, 0))
          case (0, 0) => (i, (0, 0, 0, 1))
          case _ => throw new Exception(s"non supported type please use ChiSquareMulti")
        }
      }}
      .reduceByKey( (a, b) =>
        (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4)
      )
      .map{case (i, t) =>
        val A = t._1
        val B = t._2
        val C = t._3
        val D = t._4
        val curChi = (N * math.pow(A * D - C * B, 2)) / ((A + C) * (B + D) * (A + B) * (C + D))
        (i, curChi)
      }
    chi
  }

  @inline private def computeEntropy(_pxy: Double, _px: Double, _py: Double): Double = {
    /** pxy * log(pxy / (px * py)) */
    if (_pxy != 0.0 && _px != 0.0 && _py != 0.0)
      _pxy * (math.log(_pxy / (_px * _py)) / math.log(2))
    else
      0.0
  }

  def MutualInformation(sc: SparkContext, data: RDD[String], indices: Array[Int] = Array[Int](), delimiter: String = ","): Array[(Int, Double)] = {
    // 1. counting samples
    var isRelease = false
    if (data.getStorageLevel == StorageLevel.NONE) {isRelease = true; data.cache()}
    val _indices = if (indices.nonEmpty) indices else getAllFeatureIndices(data.first.split(delimiter).length - 1)
    val nFeatures = data.first.split(delimiter).length - 1

    val cXY = data.map{ line =>
      val features = line.trim.split(delimiter)
      val label = features(features.length - 1)
      val res = mutable.ArrayBuffer[(Int, String, String)]()
      for (idx <- _indices) {
        res += ((idx, features(idx), label))
      }
      res += ((features.length - 1, label, label))

      res.toArray
    }

    val countingMap = cXY.flatMap(arr => arr.map{case (i, x, y) => (i, x.split("\\|").length)}).reduceByKey(_ + _)
    val bCountingMap = sc.broadcast(countingMap.collectAsMap)

    val pXorY = cXY
      .flatMap(arr => arr.flatMap{case (i, x, y) => x.split("\\|").map(f => ((i, f.split(":")(0)), 1))})
      .reduceByKey(_ + _)
      .map{ case ((i, x), cnt) => ((i, x), cnt.toDouble / bCountingMap.value(i) )}
      .collectAsMap()

    //    pXorY.foreach(println(_))
    val bpXorY = sc.broadcast(pXorY)

    val MIM = cXY
      .flatMap(arr => arr.flatMap{case (i, x, y) => x.split("\\|").map(f => ((i, f.split(":")(0), y),1))})
      .reduceByKey(_ + _)
      .map{ case ((i, x, y), cnt) => ((i, x, y), cnt.toDouble / bCountingMap.value(i))}
      .filter(_._1._1 != nFeatures)
      .map{ case ((i, x, y), pxy) =>
        val _pXorY = bpXorY.value
        val _px = _pXorY((i, x))
        val _py = _pXorY((nFeatures, y))
        val _pxy = if (i == nFeatures) 0.0 else pxy

        (i, computeEntropy(_pxy, _px, _py))
      }
      .reduceByKey(_ + _)
      .collect()

    // 5. release cached internal RDDs
    if (isRelease) data.unpersist()
    bCountingMap.unpersist()
    bpXorY.unpersist()

    MIM
  }

  @inline private def closeTo(lhs: Double, rhs: Double, precision: Double = 1e-6): Boolean = {
    math.abs(lhs - rhs) >= precision
  }

  @inline private def computeConditionalEntropy(_pxyz: Double, _pz:Double, _pxz: Double, _pyz: Double): Double = {
    if (closeTo(_pxyz, 0.0) || closeTo(_pz, 0.0) || closeTo(_pxz, 0.0) || closeTo(_pyz, 0.0)) {
      0.0
    }
    else {
      _pxyz * (math.log((_pz * _pxyz) / (_pxz * _pyz)) / math.log(2))
    }
  }

  private def computePXY(sc: SparkContext, data: RDD[String], indices: Array[Int],
                 label_index: Int = 0, delimiter: String = ","): scala.collection.Map[(Int, String, String), Double] = {
    val cXY = data.map{ line =>
      val features = line.trim.split(delimiter)
      val label = features(label_index)
      val res = mutable.ArrayBuffer[(Int, String, String)]()
      for (idx <- indices) {
        res += ((idx, features(idx), label))
      }
      res += ((label_index, label, label))
      res.toArray
    }

    val xyCountingMap = cXY.flatMap(arr => arr.map{case (i, x, y) => (i, x.split("\\|").length)})
      .reduceByKey(_ + _)
      .collectAsMap()
    val bXYCountingMap = sc.broadcast(xyCountingMap)

    val pXorY = cXY
      .flatMap(arr => arr.flatMap{case (i, x, y) => x.split("\\|").map(f => ((i, f.split(":")(0), y), 1))})
      .reduceByKey(_ + _)
      .map{ case ((i, x, y), cnt) => ((i, x, y), cnt.toDouble / bXYCountingMap.value(i) )}
      .collectAsMap()
    pXorY
  }

  private def getAllFeatureIndices(len: Int, label_index: Int = 0): Array[(Int, Int)] = {
    val ret = mutable.ArrayBuffer[(Int, Int)]()

    for(i <- 0 until len - 1) {
      if(i != label_index) {
        for(j <- (i + 1) until len) {
          if(j != label_index) {
            ret += ((i, j))
          }
        }
      }
    }

    ret.toArray
  }

  def ConditionalMutualInfo(sc: SparkContext, data: RDD[String], label_index: Int = 0,
                                   indices_pair: Array[(Int, Int)] = Array[(Int, Int)](),
                                   delimiter: String = ","): Array[((Int,Int), Double)] = {
    if (data.getStorageLevel == StorageLevel.NONE) data.cache()
    val _indices = if (indices_pair.nonEmpty) indices_pair else getAllFeatureIndices(data.first.split(delimiter).length, label_index)
    val unique_indices = _indices.flatMap{case (i, j) => Seq[Int](i, j)}.distinct

    val pXorY = computePXY(sc, data, unique_indices, label_index, delimiter)
    val bpXorY = sc.broadcast(pXorY)

    val cXYZ = data.map{line =>
      val feature = line.trim.split(delimiter)
      val label = feature(label_index)
      val res = mutable.ArrayBuffer[(Int, String, Int, String, String)]()
      for ((i, j) <- _indices) {
        res += ((i, feature(i), j, feature(j), label))
      }
      res.toArray
    }

    val xyzCountingMap = cXYZ.flatMap(arr => arr.map{case (i, x, j, y, z) => ((i, j), x.split("\\|").length * y.split("\\|").length)})
      .reduceByKey(_ + _)
      .collectAsMap()
    val bXYZCountingMap = sc.broadcast(xyzCountingMap)

    val MIM = cXYZ.flatMap(arr => arr.flatMap{ case (i, x, j, y, z) =>
      x.split("\\|").flatMap(x_f => y.split("\\|").map(y_f => ((i, x_f.split(":")(0), j, y_f.split(":")(0), z), 1)))
    })
      .reduceByKey(_ + _)
      .map{ case ((i, x, j, y, z), cnt) =>
        val _pxyz = cnt.toDouble / bXYZCountingMap.value((i, j))
        val _bpXorY = bpXorY.value
        val _pxz = _bpXorY((i, x, z))
        val _pyz = _bpXorY((j, y, z))
        val _pz = _bpXorY((label_index, z, z))
        ((i, j), computeConditionalEntropy(_pxyz, _pz, _pxz, _pyz))
      }
      .reduceByKey(_ + _)
      .collect()
    MIM
  }

  @inline private def initArray(acc: Array[Double], len: Int): Array[Double] = {
    var i = 0
    while (i < len) {
      acc(i) = 0
      i += 1
    }
    acc
  }

  @inline private def initRawScore(rawArr: Array[(Double, Double)], len: Int): Array[(Double, Double)] = {
    var i = 0
    while (i < len) {
      rawArr(i) = (0,0)
      i += 1
    }
    rawArr
  }

  @inline private def addArray(c1: Array[Double], c2: Array[Double]): Array[Double] = {
    var i = 0
    while (i < c1.length) {
      c1(i) += c2(i)
      i += 1
    }
    c1
  }

  private def updateExpression(c: Array[(Double, Double)], v: (Array[(Double, Double)], Double)): Array[(Double, Double)] = {
    val res = mutable.ArrayBuffer[(Double, Double)]()

    var i = 0
    val features = v._1
    val label = v._2
    while (i < features.length) {
      res += ((c(i)._1 + features(i)._1, c(i)._2 + features(i)._2))
      i += 1
    }
    res += ((0.0, label + c(i)._2))

    res.toArray
  }

  private def combineExpression(c1: Array[(Double, Double)], c2: Array[(Double, Double)]): Array[(Double, Double)] = {
    val res = mutable.ArrayBuffer[(Double, Double)]()
    var i = 0
    while (i < c1.length) {
      res += ((c1(i)._1 + c2(i)._1, c1(i)._2 + c2(i)._2))
      i += 1
    }

    res.toArray
  }

  def PearsonCorrelation(sc: SparkContext, featuresAndLabel: RDD[(Array[Double], Double)], featureLen: Int): Array[(Int, Double)] = {
    val acc = new Array[Double](featureLen + 2)
    initArray(acc, featureLen + 2)

    // 1. compute mean
    val cntArray = featuresAndLabel.treeAggregate(acc)(
      seqOp = (c, v) => {
        var i = 0
        while (i < v._1.length) {
          c(i) += v._1(i)
          i += 1
        }
        c(i) += v._2
        c(i + 1) += 1
        c
      },
      combOp = (c1, c2) => {
        addArray(c1, c2)
      })

    val meanArray = cntArray.map(_ / cntArray.last).dropRight(1)

    // 2. transform: x_i - mean
    val transformedFeaturesAndLabel = featuresAndLabel.map { case ((features, label)) =>
      var i = 0
      while (i < features.length) {
        features(i) -= meanArray(i)
        i += 1
      }
      (features, label - meanArray.last)
    }

    // 3. transform (x_i - mean) => ((x_i - x_mean) * (y_i - y_mean), (x_i - mean)^2), y ignore first factor
    val expressionArray = transformedFeaturesAndLabel.map{ case ((tFeatures, tLabel)) =>
      val expressionPair = mutable.ArrayBuffer[(Double, Double)]()
      var i = 0
      while (i < tFeatures.length) {
        expressionPair += ((tFeatures(i) * tLabel, math.pow(tFeatures(i), 2)))
        i += 1
      }
      (expressionPair.toArray, math.pow(tLabel, 2))
    }

    // 4. compute: pearson return pearson array
    val pRawScore = new Array[(Double, Double)](featureLen + 1)
    val _pRawScore = initRawScore(pRawScore, featureLen+1)
    val pearsonRawScore = expressionArray.treeAggregate(_pRawScore)(
      seqOp = (c, v) => {
        updateExpression(c, v)
      },
      combOp = (c1, c2) => {
        combineExpression(c1, c2)
      })

    val pearsonScores = mutable.ArrayBuffer[(Int, Double)]()
    val yMinusYSquareSqrt = math.sqrt(pearsonRawScore.last._2)
    var i = 0
    while (i < pearsonRawScore.length - 1) {
      val xMinusXSquareSqrt = math.sqrt(pearsonRawScore(i)._2)
      val xMutiY = pearsonRawScore(i)._1
      val curPearsonScore = xMutiY / (xMinusXSquareSqrt * yMinusYSquareSqrt)
      pearsonScores += ((i, curPearsonScore))

      i += 1
    }

    pearsonScores.toArray
  }

}
