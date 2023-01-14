package org.easysql.database

abstract class DBConnection[F[_]](override val db: DB)(using DbMonad[F]) extends DBOperater[F](db)