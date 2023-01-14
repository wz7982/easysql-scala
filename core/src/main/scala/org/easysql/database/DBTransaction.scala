package org.easysql.database

abstract class DBTransaction[F[_]](override val db: DB)(using DBMonad[F]) extends DBOperator[F](db)