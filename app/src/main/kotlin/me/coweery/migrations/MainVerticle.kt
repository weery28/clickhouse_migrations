package me.coweery.migrations

import com.fasterxml.jackson.annotation.JsonProperty
import io.reactivex.Completable
import io.reactivex.Single
import io.reactivex.functions.BiFunction
import io.vertx.core.json.JsonObject
import io.vertx.reactivex.core.AbstractVerticle
import io.vertx.reactivex.core.Vertx
import io.vertx.reactivex.ext.jdbc.JDBCClient
import ru.yandex.clickhouse.except.ClickHouseException
import java.util.Date

fun main(args: Array<String>) {
    Vertx.vertx().deployVerticle(MainVerticle::class.java.name)
}

class MainVerticle : AbstractVerticle() {

    private lateinit var client: JDBCClient
    private lateinit var config: Config
    override fun start() {

        config = vertx.fileSystem().readFileBlocking("./config.json").toJsonObject().mapTo(Config::class.java)

        client = JDBCClient.createShared(
            vertx, JsonObject()
                .put("url", config.url)
                .put("driver_class", "ru.yandex.clickhouse.ClickHouseDriver")
                .put("user", config.user)
                .put("password", config.password)
        )

        checkExistSystemTable(client).flatMapCompletable {
            if (it) {
                println("System migrations table exist.")
                Completable.complete()
            } else {
                createMigrationsTable(client)
            }
        }
            .andThen(getLastVersion(client))
            .flatMap { currentVersion ->
                loadMigrationsFiles().map {
                    it
                        .asSequence()
                        .filter { migration ->
                            migration.version > currentVersion
                        }
                        .onEach {
                            println("Migration with version ${it.version} will be applied")
                        }
                        .toList()
                }
            }
            .flatMapCompletable {
                if (checkOrder(it)) {
                    applyMigrations(it.sortedBy { it.version })
                } else {
                    println("Bad migration list")
                    Completable.complete()
                }
            }
            .subscribe({
                vertx.undeploy(deploymentID())
                println("All migrations applied. Actual state.")
                System.exit(0)
            }, {
                it.printStackTrace()
            })
    }

    private fun checkExistSystemTable(client: JDBCClient): Single<Boolean> {

        return client.rxGetConnection()
            .flatMap { conn ->
                conn.rxQuery("SELECT * FROM migrations").doFinally {
                    conn.close()
                }
            }
            .map {
                true
            }
            .onErrorResumeNext {
                if (it is ClickHouseException && (it as ClickHouseException).errorCode == 60) {
                    Single.just(false)
                } else {
                    Single.error(it)
                }
            }
    }

    private fun createMigrationsTable(client: JDBCClient): Completable {

        return client.rxGetConnection()
            .flatMapCompletable { conn ->
                conn.rxQuery(
                    "CREATE TABLE migrations(\n" +
                        "  id        UUID DEFAULT generateUUIDv4(),\n" +
                        "  version UInt64,\n" +
                        "  time DateTime\n" +
                        ")\n" +
                        "ENGINE MergeTree()\n" +
                        "ORDER BY (id);\n"
                ).ignoreElement()
                    .andThen(
                        conn.rxQuery("INSERT INTO migrations (version, time) VALUES (0, ${Date().time})")
                            .ignoreElement()
                    )
                    .doFinally { conn.close() }
            }.doOnComplete {
                println("System migrations table successfully created")
            }.doOnError {
                //it.printStackTrace()
            }
    }

    private fun getLastVersion(client: JDBCClient): Single<Long> {

        return client.rxGetConnection()
            .flatMap { conn ->
                conn.rxQuery(
                    "SELECT version FROM migrations\n" +
                        "ORDER BY time DESC \n" +
                        "LIMIT 1;"
                )
                    .map {
                        it.rows.first().getLong("version")
                    }
                    .doOnSuccess {
                        println("Current table version = $it")
                    }
            }
    }

    private fun loadMigrationsFiles(): Single<List<Migration>> {

        val regexp = Regex("V\\d+_\\S+.sql")
        return vertx.fileSystem().rxReadDir(config.path)
            .map {
                it.map {
                    it.replace(config.path + "/", "")
                }
                    .filter(regexp::matches)
                    .onEach {
                        println("Found $it migration file")
                    }

            }
            .flatMap { migrationFileNames ->
                migrationFileNames.fold(Single.just<MutableList<Migration>>(mutableListOf())) { acc, fileName ->
                    acc.zipWith(vertx.fileSystem().rxReadFile(config.path + "/" + fileName), BiFunction { t1, t2 ->
                        t1.add(Migration(fileName.split("_").first().replace("V", "").toLong(), t2.toString()))
                        return@BiFunction t1
                    })
                }
            }
    }

    private fun checkOrder(migrations: List<Migration>): Boolean {

        if (migrations.isEmpty()) return true
        migrations.sortedBy { it.version }
            .fold(migrations.minBy { it.version }!!.version - 1) { acc, migration ->
                if (migration.version - acc != 1L) {
                    return false
                } else {
                    return@fold acc + 1
                }
            }
        return true
    }

    private fun applyMigrations(migrations: List<Migration>): Completable {

        var res = Completable.complete()
        migrations.forEach {
            res = res.andThen(client.rxGetConnection()
                .flatMapCompletable { conn ->

                    val completables = it.query.split(";").filter { it.isNotBlank() && it.isNotEmpty() }
                        .mapIndexed { index, singleQuery ->
                        conn.rxQuery(singleQuery)
                            .ignoreElement()
                            .andThen(
                                conn.rxQuery("INSERT INTO migrations (version, time) VALUES (${it.version}, ${Date().time})")
                                    .ignoreElement()
                            )
                            .doOnComplete {
                                println("${it.version}.$index ----- SUCCESSFULLY APPLIED")
                            }
                            .doOnError { e ->
                                println()
                                println()
                                println("Migration with V.${it.version} has bad query")
                                println()
                                println(e.message)

                            }
                    }
                    completables.reduce { acc, completable -> acc.andThen(completable) }
                        .doFinally { conn.close() }

                })
        }
        return res
    }
}

class Migration(
    val version: Long,
    val query: String
)

class Config(
    @JsonProperty("path")
    val path: String,

    @JsonProperty("url")
    val url: String,

    @JsonProperty("user")
    val user: String,

    @JsonProperty("password")
    val password: String
)