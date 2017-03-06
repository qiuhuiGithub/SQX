package SparqlSpark
import _root_.org.apache.spark.graphx._
import scala.collection.mutable

object SparqlSpark {
  class Msg(val vari: String, val tab: RDFTable) extends java.io.Serializable { //message
    var table: RDFTable = tab
    var variable: String = vari
  }

  class VertexProp(val p: String, val o: String) extends java.io.Serializable { //vertex's property
    val prop: String = p
    val obj: String = o
  }
  /**
   * RDF Table, contain the head and the rows, send  between the vertexs
   */
  class RDFTable(val _head: mutable.LinkedHashMap[String, Int], val _rows: mutable.ArrayBuffer[mutable.ListBuffer[String]]) extends java.io.Serializable {
    var head: mutable.LinkedHashMap[String, Int] = _head
    var iteration: mutable.Set[Int] = mutable.Set.empty[Int]
    var rows: mutable.ArrayBuffer[mutable.ListBuffer[String]] = _rows
    //TODO: EZ ITT VAN HASZNALVA???????
    //+		var iter:Int = -1

    override def clone(): RDFTable = {
      return this
    }

    /**
     * check whether union or join two RDFTables.
     */
    def checkUnion(table2: RDFTable): Boolean = {
      var s = ((head.keySet -- table2.head.keySet) ++ (table2.head.keySet -- head.keySet)).size
      return (s == 0)
    }

    /**
     * merge two RDFTable, if the head is the same, union the rows; else join the head as well as the rows.
     */
    def merge(table2: RDFTable): RDFTable = { //merge RDFTable
      var tmp = "merge: oldrow: " + rows + "  newrow: " + table2.rows
      var union = checkUnion(table2)
      if (head.size == 0) {
        head = table2.head
        rows = table2.rows
      }

      val table1: RDFTable = this
      var newhead: mutable.LinkedHashMap[String, Int] = table1.head.clone() //ez a clone kell!
      var newrows: mutable.ArrayBuffer[mutable.ListBuffer[String]] = mutable.ArrayBuffer[mutable.ListBuffer[String]]()

      if (union) { // the head is same,union the tables
        newrows = table1.rows //.clone()
        table2.rows.map(r2 => {
          var newrow: mutable.ListBuffer[String] = mutable.ListBuffer[String]()
          table1.head.foreach(h => {
            newrow.append(r2(table2.head(h._1)))
          })
          newrows.append(newrow)
          //}
        })
      } else { // head is different, join the tables
        newrows = table1.rows.flatMap(r => table2.rows.map(r2 => {
          var l = true
          table2.head.foreach(h2 => {
            if (table1.head.contains(h2._1) && (r(table1.head.getOrElseUpdate(h2._1, -1)) != r2(h2._2))) {
              l = false
            }
          })
          if (l) {
            var newrow: mutable.ListBuffer[String] = r.clone() //ez a clone kell
            table2.head.foreach(h2 => {
              if (!table1.head.contains(h2._1)) {
                newrow.append(r2(h2._2))
                if (!newhead.contains(h2._1)) {
                  newhead(h2._1) = newhead.size
                }
              }
            })
            newrow
          } else {
            mutable.ListBuffer[String]()
          }
        }))
      }
      newrows = newrows.filter(r => r.length > 0)
      table1.rows = newrows
      table1.head = newhead

      var m = "table1 iter: ("
      table1.iteration.foreach(x => m = m + x)
      m = m + ") table2 iter: ("
      table2.iteration.foreach(x => m = m + x)
      //println(m + ") ")

      table1.iteration = table1.iteration.union(table2.iteration)
      // println("RDFTable" + table1)
      return table1 //.clone()
    }
  }

  /**
   * RDFVertex
   */
  class RDFVertex(val Vid: org.apache.spark.graphx.VertexId, val u: String) extends java.io.Serializable { // Vertex in Database Graph
    val id: org.apache.spark.graphx.VertexId = Vid
    var uri: String = u
    var iter: Int = -1
    var props: Array[VertexProp] = Array[VertexProp]()
    //var table:RDFTable = new RDFTable(mutable.LinkedHashMap.empty[String,Int], mutable.ArrayBuffer[mutable.ListBuffer[String]]())
    //var tableMap: mutable.LinkedHashMap[String, RDFTable] = mutable.LinkedHashMap.empty[String, RDFTable] //variable and its RDFTable
    var tableMap: mutable.LinkedHashMap[String, RDFTable] = mutable.LinkedHashMap.empty[String, RDFTable]
  
    override def toString(): String = {
      var str: String = id + " " + uri
      props.foreach(prop => str = str + " " + prop.prop + "##PropObj##" + prop.obj)
      return str
    }

    override def clone(): RDFVertex = {
      //			return this;
      var x = new RDFVertex(id, uri)
      x.iter = this.iter
      tableMap.foreach(v => x.tableMap(v._1) = v._2.clone())
      x.props = props.clone()
      return x
    }

    /**
     * check DataProperty of the vertex
     */
    def checkDataProperty(queryProp: mutable.MutableList[VertexProp]): Boolean = {
      //var s:String = uri+" dataprop: "+queryProp.clone().size+" "+props.size
      var b: Boolean = queryProp.forall(qp => {
        props.exists(p => {
          //s += qp.prop+" vs "+p.prop+" "+qp.obj+" vs "+p.obj
          (qp.prop == p.prop && qp.obj == p.obj)
        })
      })
      //			println(s)
      return b
    }

    /**
     * check ObjectProperty of the vertex.
     */
    def checkObjectProperty(_uriVar: String): Boolean = {
      if (_uriVar(0) != '?' && _uriVar != uri) {
        return false
      }
      return true
    }

    /**
     * add property to the vertex, which contain its prep and obj.
     */
    def addProps(p: String, o: String) {
      if (!props.exists(pro => { pro.prop == p && pro.obj == o })) {
        props = props :+ new VertexProp(p, o)
      }
    }
    def haveMsg(): Boolean = {
      false
    }

    /**
     *  Used in vertexProgram, merge all messages had been recieved.
     */
    def mergeMsgToTable(msg: mutable.LinkedHashMap[String, RDFTable]) {
      var varSet = mutable.HashSet[String]()
      var newMsg = mutable.LinkedHashMap[String, RDFTable]()
      //println(msg.size)
      msg.foreach(x => {
        if (x != null && x._1.split("\\^").size > 0) {
          varSet += x._1.split("\\^")(0)
          //println(x._1)
        }
      })
     
      varSet.foreach(vertex => {
        msg.foreach(x => {
          if (x != null && x._1.split("\\^").size > 0 && x._1.split("\\^")(0) == vertex) {
           
            if (newMsg.contains(vertex)) {
              newMsg(vertex) = newMsg(vertex).merge(x._2) // merge
            } else {
              newMsg(vertex) = x._2
            }
          }
        })
      })
      
      newMsg.foreach(m => {
        if (m != null) {
          if (tableMap.contains(m._1)) {
            tableMap(m._1) = tableMap(m._1).merge(m._2) //merge()
          } else {
            tableMap(m._1) = m._2 //.clone() 
          }
        }
        //println("MERGE MSG TABLENODE: " + m._1)
      })
      if (!newMsg.isEmpty) tableMap.filter(m => newMsg.contains(m._1))
    }

    def getIter(): Int = { return iter }

    /**
     * Used in sendMsg, send one vertex's info to its neighborhoods where the edge's prop is in the query.
     */
    def mergeEdgeToTable(vertexVar: String, mergeVar: String, var1: String, var2: String, s: String, o: String, iteration: Int, predicate: String): mutable.LinkedHashMap[String, RDFTable] = {
      
      var key = vertexVar + "^" + predicate
      var t = new RDFTable(
        mutable.LinkedHashMap[String, Int](var1 -> 0, var2 -> 1),
        mutable.ArrayBuffer[mutable.ListBuffer[String]](
          mutable.ListBuffer[String](s, o)))
    
      t.iteration.add(iteration)
      if (tableMap.contains(mergeVar)) {
        if (tableMap(mergeVar).head.size > 0) //merge the table in the tableMap that have the same key. 
          t = t.merge(tableMap(mergeVar))
      } else {
        //nop
      }
      return mutable.LinkedHashMap[String, RDFTable](key -> t)
    }

    /**
     * Unused!
     */
    //    def mergeTable(vertexVar: String, t: RDFTable) {
    //      //println("RDFVertex merge:"+uri+"  "+t.rows)
    //      //+			if (t.iter>iter) iter = t.iter
    //      //println("MERGE TABLENODE: "+vertexVar)
    //      tableMap(vertexVar) = tableMap(vertexVar).merge(t) //.clone()
    //    }
  }
}
