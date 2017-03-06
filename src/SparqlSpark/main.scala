package SparqlSpark
import SparqlSpark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.storage._
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.io.Source
import bleibinhaus.hdfsscalaexample._
import java.util.Calendar
import org.apache.log4j.{ Logger, Level }
/**
 * The main function, query the knowledge base use sparql.
 */
object Main {
  class MyHashMap[A, B](initSize: Int) extends mutable.HashMap[A, B] {
    override def initialSize: Int = initSize // 16 - by default
  }

  def show(x: Option[Long]) = x match {
    case Some(s) => s
    case None => -1L
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    println("TIME START " + Calendar.getInstance().getTime())
    var conf = new SparkConf().setAppName("SparkSparql").setMaster("local")
    val sc = new SparkContext(conf)

    var edgeArray = Array(Edge(-1L, -1L, "http://dummy/URI"))

    println("TIME READ VERTEX START " + Calendar.getInstance().getTime())

    //--------------read version N3
    //create vertex
    val tripleRDD = sc.textFile(args(0)).map(line => {
      val l = line.split("\\s+")
      (l(0), l(1), l(2))
    })
    val scoreMap = sc.textFile("hdfs://master:9000/user/qiuhui/sparkql/preWeight.txt").map(line => {
      val l = line.split("\\s+")
      (l(0), l(1).toDouble)
    })
    //            val scoreMap = sc.textFile("/users/qiuhui/preWeight.txt").map(line =>{
    //              val l = line.split("\\s+")
    //              (l(0),l(1).toDouble)
    //            })
    //            val scoreMap = scala.collection.immutable.Map.empty[String, Double]

    //create vertexes
    val nodes = sc.union(tripleRDD.flatMap(x => List(x._1, x._3))).distinct //find distinct nodes
    val vertexIds = nodes.zipWithIndex
    //vertexIds.collect.foreach(println(_))
    var NodesIdMap = vertexIds.collectAsMap()

    val vertexes = vertexIds.map(iri_id => {
      (iri_id._1, new SparqlSpark.RDFVertex(iri_id._2, iri_id._1))
    })

    //test
    //vertexes.collect.take(10).foreach(println(_))
    //Subject vertexes with properties
    val vertexSRDD: RDD[(Long, RDFVertex)] =
      tripleRDD.map(triple => (triple._1, (triple._2, triple._3)))
        .join(vertexes)
        .map(t => { //(iri, ( (p, o), rdfvertex  ))	
          val p_o = t._2._1
          val vertex = t._2._2
          if (p_o._2(0) == '"') {
            //ide kell hogy add property
            vertex.addProps(p_o._1, p_o._2);
            //type property
          } else if (p_o._1 == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") {
            vertex.addProps(p_o._1, p_o._2);
            //object property
          } else {

          }
          (vertex.id, vertex)
        }).distinct
    //println("vertexSRDD")
    //vertexSRDD.collect.foreach(println(_))
    //object vertexes
    val vertexORDD: RDD[(Long, RDFVertex)] =
      tripleRDD.map(triple => (triple._3, ()))
        .join(vertexes)
        .map(t => { //(iri, ( (), rdfvertex  ))
          val vertex = t._2._2
          (vertex.id, vertex)
        }).distinct
    //println("vertexORDD")
    //vertexORDD.collect.foreach(println(_))
    // the vertexRDD.
    val vertexRDD: RDD[(Long, RDFVertex)] = vertexORDD.union(vertexSRDD).reduceByKey((a, b) => {
      if (a.props.length > b.props.length) {
        a
      } else {
        b
      }
    })

    vertexRDD.cache

    //    val edgeLoopRDD: RDD[Edge[String]] = vertexRDD.filter(vertex => vertex._2.u(0) != '"').map(vertex => {
    //      Edge(vertex._1, vertex._1, "LOOP")
    //    })

    // edge between IRIs.
    val edgeRDD: RDD[Edge[String]] =
      tripleRDD.map(s_p_o => {
        if (s_p_o._3(0) != '"' && s_p_o._2 != "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") {
          Edge(show(NodesIdMap.get(s_p_o._1)), show(NodesIdMap.get(s_p_o._3)), s_p_o._2)
        } else {
          Edge(-1, -1, "")
        }
      }).distinct.filter(e => e.srcId != -1)
    edgeRDD.cache
    //edgeRDD.persist(StorageLevel.MEMORY_AND_DISK)
    vertexes.unpersist()
    vertexIds.unpersist()
    nodes.unpersist()

    //LUBM query1-12
    //1
    //val query = "?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#GraduateStudent> . ?X <http://spark.elte.hu#takesCourse> <http://www.Department0.University0.edu/GraduateCourse0>"
    //2
    //val query = "?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#GraduateStudent> . ?Y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#University> . ?Z <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Department> . ?X <http://spark.elte.hu#memberOf> ?Z . ?Z <http://spark.elte.hu#subOrganizationOf> ?Y . ?X <http://spark.elte.hu#undergraduateDegreeFrom> ?Y"
    //3
    //val query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Publication> . ?X <http://spark.elte.hu#publicationAuthor> <http://www.Department0.University0.edu/AssistantProfessor0>"
    //4
    //val query="?X <http://spark.elte.hu#worksFor> <http://www.Department0.University0.edu> . ?X <http://spark.elte.hu#name> ?Y1 . ?X <http://spark.elte.hu#emailAddress> ?Y2 . ?X <http://spark.elte.hu#telephone> ?Y3"
    //5
    //val query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#UndergraduateStudent> . ?X <http://spark.elte.hu#memberOf> <http://www.Department0.University0.edu>"
    //6
    //val query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#GraduateStudent>"
    //7
    //val query="?X <http://spark.elte.hu#takesCourse> ?Y . <http://www.Department0.University0.edu/AssociateProfessor0> <http://spark.elte.hu#teacherOf> ?Y"
    //8
    //val query = "?X <http://spark.elte.hu#memberOf> ?Y . ?Y <http://spark.elte.hu#subOrganizationOf> <http://www.University0.edu> . ?X <http://spark.elte.hu#emailAddress> ?Z"
    //9
    //val query = "?X <http://spark.elte.hu#advisor> ?Y . ?Y <http://spark.elte.hu#teacherOf> ?Z . ?X <http://spark.elte.hu#takesCourse> ?Z"
    //10
    //val query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#GraduateStudent> . ?X <http://spark.elte.hu#takesCourse> <http://www.Department1.University0.edu/GraduateCourse0>"
    //12
    //val query = "?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#FullProfessor> . ?Y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Department> . ?X <http://spark.elte.hu#worksFor> ?Y . ?Y <http://spark.elte.hu#subOrganizationOf> <http://www.University0.edu>"

    val query = args(1)
    //val query = "?x <like> ?y . ?z <follow> ?x"
    //println("QUERY: " + query)
    println("TIME CREATE PLAN START " + Calendar.getInstance().getTime())
    val planRes = SparqlPlan.createPlan(query, scoreMap.collectAsMap)
    val plan = planRes.plan
    val rootNode = planRes.rootNode
    val varNum: Int = planRes.numVar
    //println("varNum" + varNum)
    val nonTreeEdge = planRes.nonTreeEdge
    scoreMap.unpersist()

    println("TIME CREATE GRAPH START " + Calendar.getInstance().getTime())

    val graph = Graph(vertexRDD, edgeRDD).cache()
    println("TIME CREATE GRAPHOPS START " + Calendar.getInstance().getTime())

    val head = mutable.LinkedHashMap[String, Int]()
    var rows = mutable.ArrayBuffer[mutable.ListBuffer[String]]()
    val initMsg = mutable.LinkedHashMap.empty[String, RDFTable]
    val emptyTable = new RDFTable(head, rows)
    /**
     * msgCombiner ,combiner all the recieved message.
     */
    def msgCombiner(a: mutable.LinkedHashMap[String, RDFTable], b: mutable.LinkedHashMap[String, RDFTable]): mutable.LinkedHashMap[String, RDFTable] = {
      b.foreach(i => {
        if (a.contains(i._1)) {
          a(i._1) = a.getOrElseUpdate(i._1, emptyTable).merge(i._2)
        } else {
          a(i._1) = i._2 //.clone()
        }
      })
      //println("the a is " + a)
      return a
    }

    /**
     * send message from one vertex to another in the graph.
     */
    //EdgeTriplet[x,y]   -- x nodetype, y edgetype
    def sendMsg(edge: EdgeTriplet[RDFVertex, String]): Iterator[(VertexId, mutable.LinkedHashMap[String, RDFTable])] = {
      var iteration = edge.dstAttr.getIter()
      if (edge.srcAttr.iter > edge.dstAttr.iter) {
        iteration = edge.srcAttr.getIter()
      }
      var i: Iterator[(VertexId, mutable.LinkedHashMap[String, RDFTable])] = Iterator.empty
      var i_withoutAlive: Iterator[(VertexId, mutable.LinkedHashMap[String, RDFTable])] = Iterator.empty
      var tmp = "Iteration " + Calendar.getInstance().getTime()

      if (iteration < plan.length) {
        plan(iteration).foreach(triple => {
          
          var triplePattern = triple.tp
          
          var tablePattern = triple.headPattern
         
          if (edge.srcAttr.props.exists(vp => { vp.prop == triplePattern.p }) || edge.dstAttr.props.exists(vp => { vp.prop == triplePattern.p })) {
            if (edge.srcAttr.props.exists(vp => { vp.prop == triplePattern.p })) {
              
              if (triple.src == " ") {
                i = i ++ Iterator((edge.srcAttr.id, initMsg))
              } else {
                var m = edge.srcAttr.mergeEdgeToTable(
                  triplePattern.s, triplePattern.o,
                  triplePattern.s, triplePattern.o, edge.srcAttr.uri, edge.srcAttr.props.find(vp => { vp.prop == triplePattern.p }).get.obj, iteration, triplePattern.p)
                i = i ++ Iterator((edge.srcAttr.id, m))
                i_withoutAlive = i_withoutAlive ++ Iterator((edge.srcAttr.id, m))
                
              }
            }
            if (edge.dstAttr.props.exists(vp => { vp.prop == triplePattern.p })) {
             
              if (triple.src == " ") {
                i = i ++ Iterator((edge.dstAttr.id, initMsg))
              } else {
                var m = edge.dstAttr.mergeEdgeToTable(
                  triplePattern.s, triplePattern.o,
                  triplePattern.s, triplePattern.o, edge.dstAttr.uri, edge.dstAttr.props.find(vp => { vp.prop == triplePattern.p }).get.obj, iteration, triplePattern.p)
                i = i ++ Iterator((edge.dstAttr.id, m))
                i_withoutAlive = i_withoutAlive ++ Iterator((edge.dstAttr.id, m))
               
              }
            }
          } else if (edge.attr == triplePattern.p) {
            //ALIVE message
            if (triple.src == " ") {
              i = i ++ Iterator((edge.dstAttr.id, initMsg))
              //SEND forward
            } else if (triple.src == triplePattern.s) { //check whether the triple's direction in the queryPlan is the same as the triple's direction in the graph. If so, send forward, else send backward.        
              var keyPattern = triplePattern.s
              if (tablePattern.forall(a => edge.srcAttr.tableMap.contains(keyPattern) && edge.srcAttr.tableMap(keyPattern).head.contains(a)) &&
                edge.srcAttr.checkDataProperty(planRes.dataProperties.getOrElse(triplePattern.s, new mutable.MutableList[VertexProp]())) &&
                edge.srcAttr.checkObjectProperty(triplePattern.s) && edge.dstAttr.checkObjectProperty(triplePattern.o)) {
                var m = edge.srcAttr.mergeEdgeToTable(
                  triplePattern.o, triplePattern.s,
                  triplePattern.s, triplePattern.o, edge.srcAttr.uri, edge.dstAttr.uri, iteration, triplePattern.p)
                i = i ++ Iterator((edge.dstAttr.id, m))
                i_withoutAlive = i_withoutAlive ++ Iterator((edge.dstAttr.id, m))
              }
              //SEND backward
            } else {
              
              var keyPattern = triplePattern.o
              if (tablePattern.forall(a => edge.dstAttr.tableMap.contains(keyPattern) && edge.dstAttr.tableMap(keyPattern).head.contains(a)) && // contain all the useful info.
                edge.dstAttr.checkDataProperty(planRes.dataProperties.getOrElse(triplePattern.o, new mutable.MutableList[VertexProp]())) &&
                edge.dstAttr.checkObjectProperty(triplePattern.o) && edge.srcAttr.checkObjectProperty(triplePattern.s)) {
                var m = edge.dstAttr.mergeEdgeToTable(
                  triplePattern.s, triplePattern.o,
                  triplePattern.s, triplePattern.o, edge.srcAttr.uri, edge.dstAttr.uri, iteration, triplePattern.p)
                i = i ++ Iterator((edge.srcAttr.id, m))
                i_withoutAlive = i_withoutAlive ++ Iterator((edge.srcAttr.id, m))
              }
              i = i ++ Iterator((edge.dstAttr.id, initMsg))
            }
          } else {
            //Iterator.empty
          }
        })
      }
      return i
    }
    /**
     *  deal with the message recieved by the vertexes, mergeMsgToTable.
     */
    def vertexProgram(id: VertexId, attr: RDFVertex, msgSum: mutable.LinkedHashMap[String, RDFTable]): RDFVertex = {
      attr.mergeMsgToTable(msgSum)
      attr.iter = attr.iter + 1
      return attr.clone()
    }

    println("TIME PREGEL START " + Calendar.getInstance().getTime())
    var startTime = System.currentTimeMillis()
    var result = Pregel(graph, initMsg, Int.MaxValue, EdgeDirection.Either)(vertexProgram, sendMsg, msgCombiner)
    var withResult = true;
    if (!withResult) {
      println("WITHOUT RESULT " + Calendar.getInstance().getTime());
      System.exit(1);
    }
    println("RES " + Calendar.getInstance().getTime());
    val rootNodeDataProp = planRes.dataProperties.getOrElse(rootNode, new mutable.MutableList[VertexProp]())
   
    if (plan.size == 0) {
      var res = result.vertices.filter(v => v._2.checkDataProperty(rootNodeDataProp))
      println("RESULT1: " + res.count());
    } else {
      
      var res2 = result.vertices.filter(v => //find the final result in every vertex's tableMap
        (v._2.tableMap.contains(rootNode)
          && (v._2.tableMap(rootNode).iteration.size == plan.size) && (v._2.tableMap(rootNode).head.size == varNum)
          && (v._2.tableMap(rootNode).rows.size > 0)))
     
      res2 = res2.filter(v => (
        v._2.checkDataProperty(rootNodeDataProp)))
      //check nonTreeEdge
      var edgeArray = Array[Edge[String]]()
      if (nonTreeEdge.size > 0) {
        val propArr = nonTreeEdge.map(edge => edge.p)
        edgeArray = edgeRDD.filter(edge => propArr.contains(edge.attr)).collect
      }
      if (res2.count() > 0) {
        var rowNum = 0;
        res2.collect.foreach(v => {
          rowNum = rowNum + v._2.tableMap(rootNode).rows.size
        })
        //find final answer in the RDFTable of the rootNode, which contain all possible answers.
        var helpHead = res2.first()._2.tableMap(rootNode).head.clone()
        
        helpHead.foreach(h => print(h._1 + " "))
        println("")
        res2.foreach(v => {
          v._2.tableMap(rootNode).rows.map(row => {
            var t = "###"
            var source = ""
            var dest = ""
            var containFlag = 0
            nonTreeEdge.map(edge => {
              helpHead.foreach(varName => {
                if (varName._1 == edge.s) {
                  source = row(v._2.tableMap(rootNode).head(varName._1))
                }
                if (varName._1 == edge.o) {
                  dest = row(v._2.tableMap(rootNode).head(varName._1))
                }
              })
              if (dest(0) == '"') {
                result.vertices.filter(v => v._2.uri == source).collect.foreach(vertex => {
                  if (vertex._2.props.exists(vp => (vp.p == edge.p && vp.o == dest)))
                    containFlag += 1
                })
              } else {
                edgeArray.foreach(e => {
                  if (e == (Edge(show(NodesIdMap.get(source)), show(NodesIdMap.get(dest)), edge.p))) {
                    containFlag += 1
                  }
                })
              }
            })
            if (containFlag == nonTreeEdge.length) {
              helpHead.foreach(varName => {
                t += row(v._2.tableMap(rootNode).head(varName._1)) + " "
              })
              println(t)
            } else {
              rowNum -= 1
            }
          })
        })
        println("RESULT2:" + rowNum)
        println("-------------")
      } else {
        println("RESULT3: 0")
      }
    }
    var stopTime = System.currentTimeMillis();
    println("TIME STOP " + Calendar.getInstance().getTime())
    println("Elapsed Time: " + (stopTime - startTime))

  }
}
