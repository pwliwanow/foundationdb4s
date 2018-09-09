package com.github.pwliwanow.foundationdb4s.core

import com.apple.foundationdb.directory.DirectoryLayer

import scala.collection.JavaConverters._

class TransactorSpec extends FoundationDbSpec {

  private val subdir = "test-subdir"

  override def beforeEach(): Unit = {
    super.beforeEach()
    removeSubdir()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    removeSubdir()
  }

  it should "create directory if it didn't exist before" in {
    testTransactor.createOrOpen(subdir)
    val exists =
      testTransactor.db.runAsync(tx => new DirectoryLayer().exists(tx, List(subdir).asJava)).get()
    assert(exists)
  }

  it should "open directory if it already exists" in {
    testTransactor.db.runAsync(tx => new DirectoryLayer().create(tx, List(subdir).asJava)).get()
    testTransactor.createOrOpen(subdir)
    val exists =
      testTransactor.db.runAsync(tx => new DirectoryLayer().exists(tx, List(subdir).asJava)).get()
    assert(exists)
  }

  private def removeSubdir(): Unit = {
    testTransactor.db
      .runAsync(tx => new DirectoryLayer().removeIfExists(tx, List(subdir).asJava))
      .get()
    ()
  }

}
