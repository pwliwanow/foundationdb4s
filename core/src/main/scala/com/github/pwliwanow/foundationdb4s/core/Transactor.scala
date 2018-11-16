package com.github.pwliwanow.foundationdb4s.core

import scala.collection.JavaConverters._
import com.apple.foundationdb.{Database, FDB}
import com.apple.foundationdb.directory.{DirectoryLayer, DirectorySubspace}

import scala.concurrent.ExecutionContextExecutor

trait Transactor {
  val ec: ExecutionContextExecutor

  lazy val db: Database = {
    FDB.selectAPIVersion(apiVersion).open(clusterFilePath.orNull, ec)
  }

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
    sys.addShutdownHook(db.close())
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
