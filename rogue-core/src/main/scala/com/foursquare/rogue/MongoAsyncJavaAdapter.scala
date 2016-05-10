package com.foursquare.rogue

import java.util
import java.util.Collections

import com.foursquare.index.UntypedMongoIndex
import com.foursquare.rogue.MongoHelpers.MongoBuilder._
import com.foursquare.rogue.QueryHelpers._
import com.mongodb.client.result.{DeleteResult, UpdateResult}
import com.mongodb._
import com.mongodb.async.SingleResultCallback
import com.mongodb.async.client.{DistinctIterable, MongoCollection}
import com.mongodb.client.model._
import org.bson.Document
import org.bson.conversions.Bson

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Promise}


trait AsyncDBCollectionFactory[MB] {
  def getDBCollection[M <: MB](query: Query[M, _, _]): MongoCollection[Document]

  def getPrimaryDBCollection[M <: MB](query: Query[M, _, _]): MongoCollection[Document]

  def getInstanceName[M <: MB](query: Query[M, _, _]): String

  // A set of of indexes, which are ordered lists of field names
  def getIndexes[M <: MB](query: Query[M, _, _]): Option[List[UntypedMongoIndex]]

}

class PromiseCallbackBridge[T] extends SingleResultCallback[T] {
  val promise = Promise[T]()

  override def onResult(result: T, t: Throwable): Unit = {
    if (t == null) promise.success(result)
    else promise.failure(t)
  }

  def future = promise.future
}

//specialized for Long to avoid conversion quirks
class PromiseLongCallbackBridge extends SingleResultCallback[java.lang.Long] {
  val promise = Promise[Long]()
  override def onResult(result: java.lang.Long, t: Throwable): Unit = {
    if (t == null) promise.success(result)
    else promise.failure(t)
  }
  def future = promise.future
}

class PromiseLongBooleanCallbackBridge extends SingleResultCallback[java.lang.Long] {
  val promise = Promise[Boolean]()
  override def onResult(result: java.lang.Long, t: Throwable): Unit = {
    if (t == null) promise.success(result.longValue() > 0)
    else promise.failure(t)
  }
  def future = promise.future
}

class PromiseArrayListAdapter[R] extends SingleResultCallback[java.util.Collection[R]] {
  val coll = new util.ArrayList[R]()
  private[this] val p = Promise[Seq[R]]
  //coll == result - by contract
  override def onResult(result: util.Collection[R], t: Throwable): Unit = {
    if (t == null) p.success(coll)
    else p.failure(t)
  }
  def future = p.future
}

class PromiseSingleResultAdapter[R] extends SingleResultCallback[java.util.Collection[R]] {
  val coll = new util.ArrayList[R](1)
  private[this] val p = Promise[Option[R]]
  //coll == result - by contract
  override def onResult(result: util.Collection[R], t: Throwable): Unit = {
    if (t == null) p.success(coll.headOption)
    else p.failure(t)
  }
  def future = p.future
}


class MongoAsyncJavaDriverAdapter[MB](dbCollectionFactory: AsyncDBCollectionFactory[MB]) {

  def count[M <: MB](query: Query[M, _, _], readPreference: Option[ReadPreference]): Future[Long] = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause, dbCollectionFactory.getIndexes(queryClause))
    val condition: Bson = buildCondition(queryClause.condition)
    //we don't care for skip, limit in count - maybe we deviate from original, but it makes no sense anyways
    val coll = dbCollectionFactory.getDBCollection(query)
    val callback = new PromiseLongCallbackBridge()
    if(queryClause.lim.isDefined || queryClause.sk.isDefined) {
      val options = new CountOptions()
      queryClause.lim.map(options.limit(_))
      queryClause.sk.map(options.skip(_))
      coll.count(condition, options, callback)
    } else {
      coll.count(condition, callback)
    }
    callback.future
  }

  def exists[M <: MB](query: Query[M, _, _], readPreference: Option[ReadPreference]): Future[Boolean] = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause, dbCollectionFactory.getIndexes(queryClause))
    val condition: Bson = buildCondition(queryClause.condition)
    //we don't care for skip, limit in count - maybe we deviate from original, but it makes no sense anyways
    val coll = dbCollectionFactory.getDBCollection(query)
    val callback = new PromiseLongBooleanCallbackBridge()
    if(queryClause.lim.isDefined || queryClause.sk.isDefined) {
      val options = new CountOptions()
      queryClause.lim.map(options.limit(_))
      queryClause.sk.map(options.skip(_))
      coll.count(condition, options, callback)
    } else {
      coll.count(condition, callback)
    }
    callback.future
  }

  /*
  def countDistinct[M <: MB](query: Query[M, _, _],
                             key: String,
                             readPreference: Option[ReadPreference]): Long = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause, dbCollectionFactory.getIndexes(queryClause))
    val cnd = buildCondition(queryClause.condition)
    val coll = dbCollectionFactory.getDBCollection(query)
    val distIterable: DistinctIterable[Document] = coll.distinct(key, cnd, classOf[Document])
    //what to do next? - we should just iterate and count...
    // TODO: fix this so it looks like the correct mongo shell command
    val description = buildConditionString("distinct", query.collectionName, queryClause)

    runCommand(description, queryClause) {
      val coll = dbCollectionFactory.getDBCollection(query)
      coll.distinct(key, cnd, readPreference.getOrElse(coll.find().getReadPreference)).size()
    }
  }
  */

  /*
  def distinct[M <: MB, R <: Document](query: Query[M, _, _],
                           key: String,
                           readPreference: Option[ReadPreference]): Future[Seq[R]] = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause, dbCollectionFactory.getIndexes(queryClause))
    val cnd = buildCondition(queryClause.condition)
    val coll = dbCollectionFactory.getDBCollection(query)
    val pa = new PromiseArrayListAdapter[R]()
    coll.distinct[R](key, cnd, classOf[R]).into(pa.coll, pa)
    pa.future
  }
*/

  def find[M <: MB, R](query: Query[M, _, _], serializer: RogueSerializer[R]): Future[Seq[R]] = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause, dbCollectionFactory.getIndexes(queryClause))
    val cnd = buildCondition(queryClause.condition)
    val coll = dbCollectionFactory.getDBCollection(query)
    val sel = queryClause.select.map(buildSelect).getOrElse(BasicDBObjectBuilder.start.get.asInstanceOf[BasicDBObject])
    val ord = queryClause.order.map(buildOrder)
    /*
     val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause, dbCollectionFactory.getIndexes(queryClause))
    val cnd = buildCondition(queryClause.condition)

        val cursor = coll.find(cnd, sel)
        batchSize.foreach(cursor batchSize _)
        queryClause.lim.foreach(cursor.limit _)
        queryClause.sk.foreach(cursor.skip _)
        ord.foreach(cursor.sort _)
        readPreference.orElse(queryClause.readPreference).foreach(cursor.setReadPreference _)
        queryClause.maxScan.foreach(cursor addSpecial("$maxScan", _))
        queryClause.comment.foreach(cursor addSpecial("$comment", _))
        hnt.foreach(cursor hint _)


     */
    //check if serializer will work - quite possible that no, and separate mapper from Document -> R will be needed
    val adaptedSerializer = new com.mongodb.Function[Document,R]{
      override def apply(d: Document):R = serializer.fromDocument(d)
    }
    val pa = new PromiseArrayListAdapter[R]()
    //sort, hints
    val cursor = coll.find(cnd).projection(sel)
    queryClause.lim.foreach(cursor.limit _)
    queryClause.sk.foreach(cursor.skip _)
    ord.foreach(cursor.sort _)
    cursor.map(adaptedSerializer).into(pa.coll, pa)
    pa.future
  }

  def fineOne[M <: MB, R](query: Query[M, _, _], serializer: RogueSerializer[R]): Future[Option[R]] = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause, dbCollectionFactory.getIndexes(queryClause))
    val cnd = buildCondition(queryClause.condition)
    val coll = dbCollectionFactory.getDBCollection(query)
    val sel = queryClause.select.map(buildSelect).getOrElse(BasicDBObjectBuilder.start.get.asInstanceOf[BasicDBObject])
    val ord = queryClause.order.map(buildOrder)
    //check if serializer will work - quite possible that no, and separate mapper from Document -> R will be needed
    val adaptedSerializer = new com.mongodb.Function[Document,R]{
      override def apply(d: Document):R = serializer.fromDocument(d)
    }
    val oneP = new PromiseSingleResultAdapter[R]()
    val cursor = coll.find(cnd).projection(sel)
    queryClause.lim.foreach(cursor.limit _)
    queryClause.sk.foreach(cursor.skip _)
    ord.foreach(cursor.sort _)

    cursor.map(adaptedSerializer).into(oneP.coll, oneP)
    oneP.future
  }

  def foreach[M <: MB, R](query: Query[M, _, _], f: Document => Unit): Future[Unit] = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause, dbCollectionFactory.getIndexes(queryClause))
    val cnd = buildCondition(queryClause.condition)
    val coll = dbCollectionFactory.getDBCollection(query)
    val pu = Promise[Unit]
    coll.find(cnd).forEach(new Block[Document]{
      override def apply(t: Document): Unit = f(t)
    }, new SingleResultCallback[Void] {
      override def onResult(result: Void, t: Throwable): Unit = {
        if(t == null) pu.success(())
        else pu.failure(t)
      }
    })
    pu.future
  }

  def delete[M <: MB](query: Query[M, _, _],
                      writeConcern: WriteConcern): Future[Unit] = {
    val queryClause = transformer.transformQuery(query)
    validator.validateQuery(queryClause, dbCollectionFactory.getIndexes(queryClause))
    val cnd = buildCondition(queryClause.condition)
    val coll = dbCollectionFactory.getPrimaryDBCollection(query)
    val p = Promise[Unit]
    coll.deleteMany(cnd, new SingleResultCallback[DeleteResult] {
      override def onResult(result: DeleteResult, t: Throwable): Unit = {
        if(t == null) p.success(())
        else p.failure(t)
      }
    })
    p.future
  }

  def modify[M <: MB](mod: ModifyQuery[M, _],
                      upsert: Boolean,
                      multi: Boolean,
                      writeConcern: WriteConcern): Future[Unit] = {
    val modClause = transformer.transformModify(mod)
    validator.validateModify(modClause, dbCollectionFactory.getIndexes(modClause.query))
    val p = Promise[Unit]
    if (!modClause.mod.clauses.isEmpty) {
      val q = buildCondition(modClause.query.condition)
      val m = buildModify(modClause.mod)
      val coll = dbCollectionFactory.getPrimaryDBCollection(modClause.query)

      coll.updateMany(q,m, new UpdateOptions(), new SingleResultCallback[UpdateResult] {
        override def onResult(result: UpdateResult, t: Throwable): Unit = {
          if(t == null) p.success(())
          else p.failure(t)
        }
      })
    } else p.success(())
    //else clause = no modify, automatic success ... strange but true
    p.future
  }

  def findAndModify[M <: MB, R](mod: FindAndModifyQuery[M, R],
                                returnNew: Boolean,
                                upsert: Boolean,
                                remove: Boolean)
                               (f: Document => R): Future[Option[R]] = {
    val modClause = transformer.transformFindAndModify(mod)
    validator.validateFindAndModify(modClause, dbCollectionFactory.getIndexes(modClause.query))
    val p = Promise[Option[R]]
    if (!modClause.mod.clauses.isEmpty || remove) {
      val query = modClause.query
      val cnd = buildCondition(query.condition)
      val ord = query.order.map(buildOrder)
      val m = buildModify(modClause.mod)
      val coll = dbCollectionFactory.getPrimaryDBCollection(query)
      val retDoc = if(returnNew) ReturnDocument.AFTER else ReturnDocument.BEFORE

      val singleResCallback = new SingleResultCallback[Document] {
        override def onResult(result: Document, t: Throwable): Unit = {
          if(t == null) {
            if(result !=null) p.success(Option(f(result)))
            else p.success(None)
          }else p.failure(t)
        }
      }
      if(remove) {
        val opts = new FindOneAndDeleteOptions()
        ord.map(dbo => opts.sort(dbo))
        coll.findOneAndDelete(cnd, opts, singleResCallback)
      } else {
        val opts = new FindOneAndUpdateOptions().returnDocument(retDoc).upsert(upsert)
        ord.map(dbo => opts.sort(dbo))
        coll.findOneAndUpdate(cnd, m, opts, singleResCallback)
      }
    }
    else p.success(None)

    p.future
  }


}