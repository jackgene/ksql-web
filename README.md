# KSQL Web - Web UI for [KSQL](https://www.confluent.io/product/ksql/)

## Running
1. Install KSQL by following these [instructions](https://docs.confluent.io/current/ksql/docs/installation/index.html).
2. Clone this project.
3. Update the `ksql.service.base.url` value in `conf/application.conf` if necessary.
4. From the root of the project, run:
```
sbt -Dhttp.port=9000 start
```

## Building
Like most web applications, KSQL Web consists of two components:
* Server Side (written in Scala)
* Client Side (written in Elm)

Both components are built using SBT. To build the server side, run:
```
sbt compile
```

To build the client side (optional), you'll first have to
[install Elm](https://guide.elm-lang.org/install.html). Then run:
```
sbt elmMake
```

Alternatively, you may build both components,
and run the server in development mode by running:
```
sbt run
```