package org.easysql.database

abstract class DBConnection[F[_]](override val db: DB)(using DBMonad[F]) extends DBOperator[F](db)