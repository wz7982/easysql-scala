package org.easysql.database

abstract class DBTransaction[F[_]](override val db: DB)(using DbMonad[F]) extends DBOperater[F](db)