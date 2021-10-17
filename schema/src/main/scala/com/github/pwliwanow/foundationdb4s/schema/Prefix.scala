package com.github.pwliwanow.foundationdb4s.schema

import shapeless.{::, HList, HNil}

/** It's used to prove that [[Prefix]] really is a prefix of [[L]].
  *
  * Typical usage would be to have parametrized class (or trait) by some [[HList]],
  * then to require other [[HList]] as a method parameter
  * along this type class as an implicit parameter to provide additional safety during compilation.
  */
trait Prefix[L <: HList, Prefix <: HList] extends Serializable {
  def apply(x: L): Prefix
}

object Prefix extends PrefixDerivation

trait PrefixDerivation {
  implicit def hlistPrefixNil[L <: HList]: Prefix[L, HNil] =
    new Prefix[L, HNil] {
      override def apply(x: L): HNil = HNil
    }

  implicit def hlistPrefix[H, T <: HList, PH, PT <: HList](implicit
      equalsEv: H =:= PH,
      prefixEv: Prefix[T, PT]): Prefix[H :: T, PH :: PT] =
    new Prefix[H :: T, PH :: PT] {
      override def apply(x: H :: T): PH :: PT = {
        x match {
          case h :: t => equalsEv(h) :: prefixEv(t)
        }
      }
    }
}
