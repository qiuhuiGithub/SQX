package SparqlSpark
import scala.collection.mutable._
import SparqlSpark._
import scala.collection.concurrent._
import Array._
import scala.io.Source

object SparqlPlan {
  class Triple(val tp: String) extends java.io.Serializable {
    var spo = tp.split("\\s+")
    val s: String = spo(0)
    val p: String = spo(1)
    val o: String = spo(2)
    val str: String = tp
    var finish: Boolean = false

    override def toString(): String = {
      return str
    }
  }

  /**
   * PlanResult, which contains the plan, the rootNode, the number of variable as well as the dataProperties in the query.
   */
  class PlanResult(_plan: ArrayBuffer[ListBuffer[PlanItem]], _rootNode: String, _varNum: Int, _dataProperties: LinkedHashMap[String, MutableList[VertexProp]], _nonTreeEdge: Array[Triple]) extends java.io.Serializable {
    val plan: ArrayBuffer[ListBuffer[PlanItem]] = _plan.clone()
    val rootNode: String = _rootNode
    val numVar: Int = _varNum
    val dataProperties = _dataProperties.clone()
    val nonTreeEdge = _nonTreeEdge.clone()
  }

  /**
   * CandidateList, find the candidate list.
   */
  class Candidate(_candiPlan: PlanResult, _level: Int, _weightArray: ArrayBuffer[Double]) extends java.io.Serializable {
    val candiPlan: PlanResult = _candiPlan
    val level: Int = _level
    val weightArray: ArrayBuffer[Double] = _weightArray.clone
  }

  /**
   * PlanItem, which contains the triple, the src and the headPattern, just like
   * PLAN-- (?Z <http://spark.elte.hu#subOrganizationOf> ?Y, ?Y, Set(?X)), where ?Y is src and Set(?X) is the set which send message to ?Y
   */
  class PlanItem(_tp: Triple, _src: String, _headPattern: Set[String]) extends java.io.Serializable {
    val tp: Triple = _tp
    val src: String = _src
    var headPattern: Set[String] = _headPattern
  }

  /**
   * get DataProperties of the query, if the object is start with '"' or the predicate is type, it is DataProperties, otherwise, it's ObjectProperties.
   */
  def getDataProperties(SparqlQuery: String): (Array[Triple], LinkedHashMap[String, MutableList[VertexProp]]) = {
    var query = SparqlQuery.split(" . ")
    var TPs: Array[Triple] = query.map(tp => new Triple(tp))
    var queryArray: Array[String] = Array[String]()
    var dataProperties: LinkedHashMap[String, MutableList[VertexProp]] = LinkedHashMap[String, MutableList[VertexProp]]()
    TPs = TPs.flatMap(tp => {
      if ((tp.p == "rdf:type") || (tp.p == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") || tp.o(0) == '"') {
        var propList = dataProperties.getOrElseUpdate(tp.s, MutableList[VertexProp]())
        propList += new VertexProp(tp.p, tp.o)
        dataProperties(tp.s) = propList
        Array[Triple]()
      } else {
        Array[Triple](tp)
      }
    })

    return (TPs, dataProperties)
  }

  /**
   * calc variable
   */
  def calcVariable(level: Int, weightArray: ArrayBuffer[Double]): Double = {
    var variable = 0.0
    val mean = weightArray.reduce(_ + _) / level
    variable = 1 / level * weightArray.map(x => {
      Math.pow(x - mean, 2)
    }).reduce(_ + _)
    variable
  }

  /**
   * get the degrees of every node, and find the max as the  rootNode, after find the minimum spanning tree
   */
  def getRootNode(tps: Array[Triple]): String = {
    var array = ArrayBuffer[String]()
    tps.foreach(x => {
      array += x.s
      array += x.o
    })
    var map = new HashMap[String, Int]
    for (i <- 0 until array.length) {
      if (map.contains(array(i))) {
        var tmp = map.get(array(i))
        map.put(array(i), tmp.getOrElse(0) + 1)
      } else {
        map.put(array(i), 1)
      }
    }
    var maxStr: String = ""
    val max = map.values.max
    for (v <- map) {
      if (max == v._2)
        maxStr = v._1
    }
    return maxStr
  }

  /**
   *  find the minimum spanning tree
   */
  def findMinSpanningTree(tps: Array[Triple], scoreMap: scala.collection.Map[String, Double]): (Array[Triple], Array[Triple]) = {
    var minTriple = ArrayBuffer[Triple]()
    val node = tps.flatMap(x => List(x.s, x.o)).distinct.zipWithIndex.toMap
    val newTriple = tps.map(tp => {
      (node.getOrElse(tp.s, 0), scoreMap.getOrElse(tp.p, 100.0), node.getOrElse(tp.o, 0))
    })
    var tree = ofDim[Double](node.size, node.size)
    //init
    for (i <- 0 until node.size) {
      for (j <- 0 until node.size) {
        tree(i)(j) = Double.MaxValue
      }
    }
    for (i <- 0 until node.size) {
      for (j <- 0 until node.size) {
        newTriple.map(x => {
          if ((i == x._1 && j == x._3) || (i == x._3 && j == x._1)) {
            tree(i)(j) = x._2
            tree(j)(i) = x._2
          }
        })
      }
    }
    var lowcost = new Array[Double](node.size)
    var mst = new Array[Int](node.size)
    var min: Double = 0
    var minid: Int = 0
    var sum: Double = 0
    for (i <- 1 until node.size) {
      lowcost(i) = tree(0)(i)
      mst(i) = 0
    }
    mst(0) = 0
    for (i <- 1 until node.size) {
      min = Double.MaxValue
      minid = 0
      for (j <- 1 until node.size) {
        if (lowcost(j) < min && lowcost(j) != 0) {
          min = lowcost(j)
          minid = j
        }
      }
      //println("v" + mst(minid) + "-v" + minid + "=" + min)
      var src = ""
      var dst = ""
      for (key <- node.keySet) {
        if (node.getOrElse(key, -1) == mst(minid)) {
          src = key
        }
        if (node.getOrElse(key, -1) == minid) {
          dst = key
        }
      }
      tps.map(x => {
        if ((x.s == src && x.o == dst) || (x.s == dst && x.o == src)) {
          minTriple += x
        }
      })
      sum += min
      lowcost(minid) = 0
      for (j <- 1 until node.size) {
        if (tree(minid)(j) < lowcost(j)) {
          lowcost(j) = tree(minid)(j)
          mst(j) = minid
        }
      }
    }
    var nonTreeEdge = ArrayBuffer[Triple]()
    tps.map(tp => {
      if (!minTriple.contains(tp))
        nonTreeEdge += tp
    })
    return (minTriple.toArray, nonTreeEdge.toArray)
  }

  /**
   * Create the queryPlan, which is a BFS algorithm.
   * Choose a variable and all its neighborhood will be its children, loop while all the have been processed.
   * Build the tree top-down while process the query bottom-up.
   */
  def createPlan(SparqlQuery: String, scoreMap: scala.collection.Map[String, Double]): PlanResult = {
    var (tps, dataProperties): (Array[Triple], LinkedHashMap[String, MutableList[VertexProp]]) = getDataProperties(SparqlQuery)

    var rootNode = ""

    var vars: Set[String] = HashSet()
    var nonTreeEdge: Array[Triple] = Array[Triple]()

    if (tps.size > 0) {

      var (newtps, newnonTreeEdge) = findMinSpanningTree(tps, scoreMap)
      tps = newtps.clone
      nonTreeEdge = newnonTreeEdge.clone

    } else {
      rootNode = dataProperties.keySet.head
      dataProperties.map(v => {
        println("PLAN-- " + v._1)
        v._2.map(p => {
          println("PLAN-- " + p.prop + " " + p.obj)
        })
      })
      return new PlanResult(ArrayBuffer[ListBuffer[PlanItem]](), rootNode, dataProperties.size, dataProperties, nonTreeEdge)
    }
    tps.foreach(tp => {
      vars.add(tp.s)
      vars.add(tp.o)
    })

    var candidateArray = ArrayBuffer[Candidate]()
    for (root <- vars) {
      rootNode = root
      var v = root
      tps.map(tp => {
        tp.finish = false
      })
      var plan = ArrayBuffer[ListBuffer[PlanItem]]()
      var aliveTP = ArrayBuffer[ListBuffer[PlanItem]]()

      var q2 = new Queue[String]()
      q2.enqueue("^")
      var iteration = ListBuffer[PlanItem]()
      var aliveIter = ListBuffer[PlanItem]()
      var level: Int = 0
      var MsgPattern: LinkedHashMap[String, Set[String]] = LinkedHashMap[String, Set[String]]()
      var levelWeight: Double = 0
      var weightArray: ArrayBuffer[Double] = ArrayBuffer[Double]()
      while (v != "^") {

        if (!MsgPattern.contains(v)) {
          MsgPattern(v) = HashSet()
        }

        tps.map(tp => {

          if (!tp.finish && tp.s == v) {
            if (!q2.contains(tp.o)) q2.enqueue(tp.o)

            aliveIter += new PlanItem(new Triple("?alive " + tp.p + " ?alive"), " ", Set[String]())
            iteration += new PlanItem(new Triple(tp.str), tp.o, Set[String]())
            levelWeight += scoreMap.getOrElse(tp.p, 1.0)

            tp.finish = true
          } else if (!tp.finish && tp.o == v) {
            if (!q2.contains(tp.s)) q2.enqueue(tp.s)

            aliveIter += new PlanItem(new Triple("?alive " + tp.p + " ?alive"), " ", Set[String]())
            iteration += new PlanItem(new Triple(tp.str), tp.s, Set[String]())
            levelWeight += scoreMap.getOrElse(tp.p, 1.0)

            tp.finish = true
          } else {
            //nop
          }

        })
        if (q2.size > 1 && q2.front.equals("^")) {
          if (!iteration.isEmpty) {
            aliveTP += aliveIter
            if (aliveTP.length > 1) {
              for (i <- 0 to level - 1) {
                aliveTP(i).map(alive => iteration += alive)
              }
            }
          }
          plan += iteration
          q2.dequeue
          v = q2.dequeue
          q2.enqueue("^")
          level += 1
          weightArray += levelWeight
          aliveIter = ListBuffer[PlanItem]()
          iteration = ListBuffer[PlanItem]()
          levelWeight = 0.0
        } else if (q2.size > 1 && !q2.front.equals("^")) {
          v = q2.dequeue
        } else {
          v = "^"
        }

      }

      /**
       * The function of the code below is to add AcceptHeaders to the query, by which we can know how many messages need to send to the src in the triple.
       */
      plan = plan.reverse //bottom-up

      plan = plan.map(iter => {
        iter.map(planitem => {
          if (planitem.tp.s != "?alive") {
            var o = planitem.tp.o
            var src = planitem.src
            var des = o
            if (src == o) {
              des = planitem.tp.s
            }
            MsgPattern(des) = MsgPattern(des).union(MsgPattern(src))
            MsgPattern(des) += src
            var mp: Set[String] = Set[String]()
            MsgPattern(src).foreach(i => mp += i)
            planitem.headPattern = mp
            planitem
          } else {
            planitem.headPattern = Set[String]()
            planitem
          }
        })
      })
      candidateArray += new Candidate(new PlanResult(plan, rootNode, vars.size, dataProperties, nonTreeEdge), level, weightArray)
    }
    val minLevel = candidateArray.map(candidate => {
      candidate.level
    }).min
    val candidateResult = candidateArray.filter(candidate => {
      candidate.level == minLevel
    })
    val minVariance = candidateResult.map(candidate => {
      calcVariable(candidate.level, candidate.weightArray)
    }).min
    val finalSparqlPlan = candidateResult.filter(candidate => {
      calcVariable(candidate.level, candidate.weightArray) == minVariance
    })(0)
    finalSparqlPlan.candiPlan.plan.map(list => {
      println("PLAN-- [")
      list.map(tp => println("PLAN-- (" + tp.tp.toString() + ", " + tp.src + ", " + tp.headPattern + ")"))
      println("PLAN-- ],")
    })
    println("PLAN-- DATAPROPERTIES")
    dataProperties.map(v => {
      println("PLAN-- " + v._1)
      v._2.map(p => {
        println("PLAN-- " + p.prop + " " + p.obj)
      })
    })
    return finalSparqlPlan.candiPlan
    //return new PlanResult(plan, rootNode, vars.size, dataProperties, nonTreeEdge)
  }
}
