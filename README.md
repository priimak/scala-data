Scala Data - Tools for reading various scientific data formats
==============================================================

At the moment this library contains very primitive Scala 
code for reading 1-dimensional numpy arrays from .npy files. Only
a asmall subset of possible dataypes that can be stored in the .npy 
files is supported. Following table lists correspondance between 
supported numpy types their Scala types.

NPY Type | Scala Type
---------|-----------
u1       | Int
i1       | Byte
u2       | Int
i2       | Short
u4       | Long
i4       | Int
u8       | Long
i8       | Long
f4       | Float
i8       | Double

Due to the nature of library an explicit type conversion/declaration is 
required as is shown in the fowllowing example

```scala
import net.priimak.scala.data.npy._

val v: NPYVector[Int] = NPYVector.valueOf("voltage.npy")
```

If specified type does not match Scala type as defined in the table above then 
runtime error will be thrown at the time you access elements of the vector.

Numpy type (NPY Type) can also be retireved by calling `NPYVector.npyType()` method.

Class `NPYVector` extends from the `IndexedSeq` and therefore has access to all 
methods of Scala collection API.