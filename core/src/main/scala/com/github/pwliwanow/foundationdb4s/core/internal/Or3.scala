package com.github.pwliwanow.foundationdb4s.core.internal

private[foundationdb4s] sealed abstract class Or3[+A, +B, +C]

private[foundationdb4s] object Or3 {
  private[foundationdb4s] final case class Left[+A, +B, +C](value: A) extends Or3[A, B, C]
  private[foundationdb4s] final case class Middle[+A, +B, +C](value: B) extends Or3[A, B, C]
  private[foundationdb4s] final case class Right[+A, +B, +C](value: C) extends Or3[A, B, C]
}
