// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.rogue

import com.foursquare.field.Field
import com.foursquare.rogue.MongoHelpers.{MongoModify, MongoSelect}
import com.mongodb.{DBObject, ReadPreference, WriteConcern}
import scala.collection.mutable.ListBuffer

trait RogueReadSerializer[R] {
  def fromDBObject(dbo: DBObject): R
}

trait RogueWriteSerializer[R] {
  def toDBObject(record: R): DBObject
}

trait RogueSerializer[From, To] extends RogueReadSerializer[To] with RogueWriteSerializer[From]

trait QueryExecutor[MB, RB] extends Rogue {
  def adapter: MongoJavaDriverAdapter[MB, RB]
  def optimizer: QueryOptimizer

  def defaultWriteConcern: WriteConcern

  protected def readSerializer[M <: MB, R](
      meta: M,
      select: Option[MongoSelect[M, R]]
  ): RogueReadSerializer[R]

  protected def writeSerializer(record: RB): RogueWriteSerializer[RB]

  def count[M <: MB, State](query: Query[M, _, State])
                           (implicit ev: ShardingOk[M, State]): Long = {
    if (optimizer.isEmptyQuery(query)) {
      0L
    } else {
      adapter.count(query)
    }
  }

  def countDistinct[M <: MB, V, State](query: Query[M, _, State])
                                      (field: M => Field[V, M])
                                      (implicit ev: ShardingOk[M, State]): Long = {
    if (optimizer.isEmptyQuery(query)) {
      0L
    } else {
      adapter.countDistinct(query, field(query.meta).name)
    }
  }

  def distinct[M <: MB, V, State](query: Query[M, _, State])
                                 (field: M => Field[V, M])
                                 (implicit ev: ShardingOk[M, State]): List[V] = {
    if (optimizer.isEmptyQuery(query)) {
      Nil
    } else {
      val rv = new ListBuffer[V]
      adapter.distinct[M, V](query, field(query.meta).name)(s => rv += s)
      rv.toList
    }
  }

  def fetch[M <: MB, R, State](query: Query[M, R, State],
                               readPreference: Option[ReadPreference] = None)
                              (implicit ev: ShardingOk[M, State]): List[R] = {
    if (optimizer.isEmptyQuery(query)) {
      Nil
    } else {
      val s = readSerializer[M, R](query.meta, query.select)
      val rv = new ListBuffer[R]
      adapter.query(query, readPreference)(dbo => rv += s.fromDBObject(dbo))
      rv.toList
    }
  }

  def fetchOne[M <: MB, R, State, S2](query: Query[M, R, State],
                                      readPreference: Option[ReadPreference] = None)
                                 (implicit ev1: AddLimit[State, S2], ev2: ShardingOk[M, S2]): Option[R] = {
    fetch(query.limit(1), readPreference).headOption
  }

  def foreach[M <: MB, R, State](query: Query[M, R, State],
                                 readPreference: Option[ReadPreference] = None)
                                (f: R => Unit)
                                (implicit ev: ShardingOk[M, State]): Unit = {
    if (optimizer.isEmptyQuery(query)) {
      ()
    } else {
      val s = readSerializer[M, R](query.meta, query.select)
      adapter.query(query, readPreference)(dbo => f(s.fromDBObject(dbo)))
    }
  }

  private def drainBuffer[A, B](
      from: ListBuffer[A],
      to: ListBuffer[B],
      f: List[A] => List[B],
      size: Int
  ): Unit = {
    // ListBuffer#length is O(1) vs ListBuffer#size is O(N) (true in 2.9.x, fixed in 2.10.x)
    if (from.length >= size) {
      to ++= f(from.toList)
      from.clear
    }
  }

  def fetchBatch[M <: MB, R, T, State](query: Query[M, R, State],
                                       batchSize: Int,
                                       readPreference: Option[ReadPreference] = None)
                                      (f: List[R] => List[T])
                                      (implicit ev: ShardingOk[M, State]): List[T] = {
    if (optimizer.isEmptyQuery(query)) {
      Nil
    } else {
      val s = readSerializer[M, R](query.meta, query.select)
      val rv = new ListBuffer[T]
      val buf = new ListBuffer[R]

      adapter.query(query, readPreference) { dbo =>
        buf += s.fromDBObject(dbo)
        drainBuffer(buf, rv, f, batchSize)
      }
      drainBuffer(buf, rv, f, 1)

      rv.toList
    }
  }

  def bulkDelete_!![M <: MB, State](query: Query[M, _, State],
                                    writeConcern: WriteConcern = defaultWriteConcern)
                                   (implicit ev1: Required[State, Unselected with Unlimited with Unskipped],
                                    ev2: ShardingOk[M, State]): Unit = {
    if (optimizer.isEmptyQuery(query)) {
      ()
    } else {
      adapter.delete(query, writeConcern)
    }
  }

  def updateOne[M <: MB, State](
      query: ModifyQuery[M, State],
      writeConcern: WriteConcern = defaultWriteConcern
  )(implicit ev: RequireShardKey[M, State]): Unit = {
    if (optimizer.isEmptyQuery(query)) {
      ()
    } else {
      adapter.modify(query, upsert = false, multi = false, writeConcern = writeConcern)
    }
  }

  def upsertOne[M <: MB, State](
      query: ModifyQuery[M, State],
      writeConcern: WriteConcern = defaultWriteConcern
  )(implicit ev: RequireShardKey[M, State]): Unit = {
    if (optimizer.isEmptyQuery(query)) {
      ()
    } else {
      adapter.modify(query, upsert = true, multi = false, writeConcern = writeConcern)
    }
  }

  def updateMulti[M <: MB, State](
      query: ModifyQuery[M, State],
      writeConcern: WriteConcern = defaultWriteConcern
  ): Unit = {
    if (optimizer.isEmptyQuery(query)) {
      ()
    } else {
      adapter.modify(query, upsert = false, multi = true, writeConcern = writeConcern)
    }
  }

  def findAndUpdateOne[M <: MB, R](
    query: FindAndModifyQuery[M, R],
    returnNew: Boolean = false,
    writeConcern: WriteConcern = defaultWriteConcern
  ): Option[R] = {
    if (optimizer.isEmptyQuery(query)) {
      None
    } else {
      val s = readSerializer[M, R](query.query.meta, query.query.select)
      adapter.findAndModify(query, returnNew, upsert=false, remove=false)(s.fromDBObject _)
    }
  }

  def findAndUpsertOne[M <: MB, R](
    query: FindAndModifyQuery[M, R],
    returnNew: Boolean = false,
    writeConcern: WriteConcern = defaultWriteConcern
  ): Option[R] = {
    if (optimizer.isEmptyQuery(query)) {
      None
    } else {
      val s = readSerializer[M, R](query.query.meta, query.query.select)
      adapter.findAndModify(query, returnNew, upsert=true, remove=false)(s.fromDBObject _)
    }
  }

  def findAndDeleteOne[M <: MB, R, State](
    query: Query[M, R, State],
    writeConcern: WriteConcern = defaultWriteConcern
  )(implicit ev: RequireShardKey[M, State]): Option[R] = {
    if (optimizer.isEmptyQuery(query)) {
      None
    } else {
      val s = readSerializer[M, R](query.meta, query.select)
      val mod = FindAndModifyQuery(query, MongoModify(Nil))
      adapter.findAndModify(mod, returnNew=false, upsert=false, remove=true)(s.fromDBObject _)
    }
  }

  def explain[M <: MB](query: Query[M, _, _]): String = {
    adapter.explain(query)
  }

  def iterate[S, M <: MB, R, State](query: Query[M, R, State],
                                    state: S,
                                    readPreference: Option[ReadPreference] = None)
                                   (handler: (S, Iter.Event[R]) => Iter.Command[S])
                                   (implicit ev: ShardingOk[M, State]): S = {
    if (optimizer.isEmptyQuery(query)) {
      handler(state, Iter.EOF).state
    } else {
      val s = readSerializer[M, R](query.meta, query.select)
      adapter.iterate(query, state, s.fromDBObject _, readPreference)(handler)
    }
  }

  def iterateBatch[S, M <: MB, R, State](query: Query[M, R, State],
                                         batchSize: Int,
                                         state: S,
                                         readPreference: Option[ReadPreference] = None)
                                        (handler: (S, Iter.Event[List[R]]) => Iter.Command[S])
                                        (implicit ev: ShardingOk[M, State]): S = {
    if (optimizer.isEmptyQuery(query)) {
      handler(state, Iter.EOF).state
    } else {
      val s = readSerializer[M, R](query.meta, query.select)
      adapter.iterateBatch(query, batchSize, state, s.fromDBObject _, readPreference)(handler)
    }
  }

  def save[RecordType <: RB](record: RecordType, writeConcern: WriteConcern = defaultWriteConcern): RecordType = {
    val s = writeSerializer(record)
    val dbo = s.toDBObject(record)
    adapter.save(record, dbo, writeConcern)
    record
  }

  def insert[RecordType <: RB](record: RecordType, writeConcern: WriteConcern = defaultWriteConcern): RecordType = {
    val s = writeSerializer(record)
    val dbo = s.toDBObject(record)
    adapter.insert(record, dbo, writeConcern)
    record
  }

  def insertAll[RecordType <: RB](records: Seq[RecordType], writeConcern: WriteConcern = defaultWriteConcern): Seq[RecordType] = {
    records.headOption.foreach(record => {
      val s = writeSerializer(record)
      val dbos = records.map(s.toDBObject)
      adapter.insertAll(record, dbos, writeConcern)
    })
    records
  }

  def remove[RecordType <: RB](record: RecordType, writeConcern: WriteConcern = defaultWriteConcern): RecordType = {
    val s = writeSerializer(record)
    val dbo = s.toDBObject(record)
    adapter.remove(record, dbo, writeConcern)
    record
  }
}
