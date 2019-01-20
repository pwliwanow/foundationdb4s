package com.github.pwliwanow.foundationdb4s.core

import scala.collection.JavaConverters._
import com.apple.foundationdb.{Database, FDB}
import com.apple.foundationdb.directory.{DirectoryLayer, DirectorySubspace}

import scala.concurrent.ExecutionContextExecutor

trait Transactor {
  val ec: ExecutionContextExecutor

  // keeps the same database opened as per:
  // https://forums.foundationdb.org/t/db-connection-opening-closing/630/2
  lazy val db: Database = {
    FDB.selectAPIVersion(apiVersion).open(clusterFilePath.orNull, ec)
  }

  /** Closes the db and releases all associated resources.
    * This must be closed at least once after db is no longer in use.
    */
  def close(): Unit = db.close()

  def apiVersion: Int

  def clusterFilePath: Option[String]

  def createOrOpen(subpath: String*): DirectorySubspace = {
    createOrOpen(new DirectoryLayer(), subpath: _*)
  }

  def createOrOpen(dir: DirectoryLayer, subpath: String*): DirectorySubspace = {
    db.runAsync(tx => dir.createOrOpen(tx, subpath.asJava)).get()
  }
}

object Transactor {
  def apply(version: Int)(implicit ece: ExecutionContextExecutor): Transactor = new Transactor {
    override val ec: ExecutionContextExecutor = ece
    override val apiVersion: Int = version
    override def clusterFilePath: Option[String] = None
  }

  def apply(version: Int, clusterFilePath: String)(
      implicit ece: ExecutionContextExecutor): Transactor = {
    val cfp = clusterFilePath
    new Transactor {
      override val ec: ExecutionContextExecutor = ece
      override val apiVersion: Int = version
      override val clusterFilePath: Option[String] = Some(cfp)
    }
  }
}
