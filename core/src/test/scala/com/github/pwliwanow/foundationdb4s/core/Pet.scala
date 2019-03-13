package com.github.pwliwanow.foundationdb4s.core

sealed trait Pet

object Pet {
  final val CatType = "cat"
  final val DogType = "dog"

  final case class Cat(name: String) extends Pet
  final case class Dog(name: String) extends Pet
}
