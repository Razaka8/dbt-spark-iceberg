# 🧊 Spark Iceberg Lakehouse — Gravitino IRC + dbt-spark

> **v1.0 — Infrastructure demo**  
> Spark Thrift Server · Apache Iceberg · Gravitino Iceberg REST Catalog · MinIO · PostgreSQL · dbt-spark  
> ✅ `materialized=view` fonctionne avec Gravitino IRC + OSS Spark 3.5.x

---


## Contexte

Ce dépôt démontre une infrastructure **lakehouse complète** basée sur Apache Iceberg,
déployée avec Docker Compose sur une VM Linux (Debian 12, IP : `192.168.70.2`).

L'objectif principal de cette **v1** est de prouver que `materialized=view` fonctionne
dans dbt-spark lorsqu'on utilise **Gravitino Iceberg REST Catalog (IRC)** comme metastore —
ce qui n'est **pas** possible avec AWS GlueCatalog + OSS Spark.

---

## Architecture

```
                         ┌──────────────────────────────────────────────────────────┐
                         │                  Docker Network : dbt-network            │
                         │                                                          │
  ┌──────────┐  Thrift   │  ┌──────────────────┐   REST /iceberg   ┌─────────────┐  │
  │ dbt-spark│ ────────► │  │ spark-thrift-dbt │ ────────────────► │ iceberg-rest│  │
  │ (host)   │ :10000    │  │ Spark 3.5.x      │   port 9001       │ Gravitino   │  │
  └──────────┘           │  │ :10000 / :4040   │                   │ IRC 1.2.0   │  │
                         │  └────────┬─────────┘                   └──────┬──────┘  │
                         │           │ S3A                                 │ JDBC   │
                         │           ▼                                     ▼        │
                         │  ┌──────────────────┐               ┌─────────────────┐  │
                         │  │ warehouse-minio  │               │ backend-psql    │  │
                         │  │ MinIO            │               │ PostgreSQL 17.2 │  │
                         │  │ :9000 / :9001    │               │ port 5432       │  │
                         │  └──────────────────┘               └─────────────────┘  │
                         └──────────────────────────────────────────────────────────┘

  spark-connect (optionnel) — port 15002 gRPC, 4041 Spark UI
```

---

## Stack technique

| Service Docker | Image | Port hôte | Rôle |
|---|---|---|---|
| `spark-thrift-dbt` | `jupyter/pyspark-notebook:x86_64-lab-4.0.7` | 10000, 4040 | Spark Thrift Server |
| `iceberg-rest` | `apache/gravitino-iceberg-rest` | **8181**→9001, 8090 | Gravitino IRC |
| `backend-psql` | `postgres:17.2-bookworm` | — | Backend JDBC du catalog |
| `warehouse-minio` | `minio/minio:latest` | 9000, 9001 | Stockage objet S3 |
| `minio-init` | `minio/mc:latest` | — | Init bucket |
| `spark-connect` | `jupyter/pyspark-notebook:x86_64-lab-4.0.7` | 15002, 4041 | Spark Connect (optionnel) |

---

## Prérequis

- Docker + Docker Compose v2
- Python 3.10+ sur la machine cliente (Windows ou Linux)
- VM Linux Debian 12 — min. 6 Go RAM — IP `192.168.70.2`
- JARs dans `./les_jars/` :
  - `iceberg-aws-bundle-1.8.x.jar`
  - `aws-java-sdk-bundle-1.12.262.jar`
  - `iceberg-aws-bundle-1.8.0.jar`
  - `hadoop-aws-3.3.4.jar`
  - `iceberg-spark-runtime-3.5_2.12-1.8.0.jar`
  - `postgresql-42.6.0.jar`
  - `gravitino-iceberg-aws-bundle-1.2.0.jar`

---

## Démarrage rapide

```bash
# 1. Cloner le dépôt
git clone https://github.com/Razaka8/dbt-spark-iceberg.git
cd dbt-spark-iceberg

# 2. Démarrer l'infrastructure
./spark-iceberg-infra/scripts/01-init-infra.sh

# 3. Vérifier que Gravitino IRC répond
curl http://192.168.70.2:8181/iceberg/v1/config

# 4. Vérifier que le Thrift Server est prêt
docker logs spark-thrift-dbt 2>&1 | grep -E "ThriftBinaryCLIService|ERROR"

# 5. Creation du namespace default
./spark-iceberg-infra/scripts/02-init-default-namespace.sh

```

---

## Configuration Spark (`config/spark_irc_gravitino/spark-defaults.conf`)

```properties
spark.hadoop.fs.defaultFS                         s3a://dbt-bucket/iceberg_warehouse

# S3 / MinIO
spark.hadoop.fs.s3a.impl                          org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider      org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
spark.hadoop.fs.s3a.access.key                    minioadmin
spark.hadoop.fs.s3a.secret.key                    minioadmin
spark.hadoop.fs.s3a.region                        us-east-1
spark.hadoop.fs.s3a.path.style.access             true
spark.hadoop.fs.s3a.endpoint                      http://192.168.70.2:9000

# Warehouse Iceberg sur S3
spark.sql.warehouse.dir                           s3a://dbt-bucket/iceberg_warehouse

# Catalog REST Gravitino
spark.sql.extensions                              org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.defaultCatalog                          rest_catalog
spark.sql.catalog.rest_catalog                    org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.rest_catalog.type               rest
spark.sql.catalog.rest_catalog.uri                http://192.168.70.2:8181/iceberg/
spark.sql.catalog.rest_catalog.s3.endpoint        http://192.168.70.2:9000
spark.sql.catalog.rest_catalog.s3.path-style-access       true
spark.sql.catalog.rest_catalog.s3.access-key-id           minioadmin
spark.sql.catalog.rest_catalog.s3.secret-access-key       minioadmin
spark.sql.catalog.rest_catalog.s3.region                  us-east-1


# Timeouts
# Timeouts réseau
spark.network.timeout                   600s
spark.files.io.connectionTimeout        600s
spark.rpc.askTimeout                    600s
spark.rpc.lookupTimeout                 600s
spark.sql.broadcastTimeout              600

# Heartbeat — évite que le driver soit considéré mort
spark.executor.heartbeatInterval        30s
spark.network.timeoutInterval           120s

# S3A timeouts
spark.hadoop.fs.s3a.connection.timeout  300000
spark.hadoop.fs.s3a.socket.timeout      300000
spark.hadoop.fs.s3a.attempts.maximum   10

# Classpath
spark.driver.extraClassPath     /home/jovyan/.ivy2/jars/*
spark.executor.extraClassPath   /home/jovyan/.ivy2/jars/*

# Performance
spark.sql.adaptive.enabled                    true
spark.sql.adaptive.coalescePartitions.enabled true
spark.sql.parquet.compression.codec           snappy
```

---

## profiles.yml (dbt)

```yaml
my_project:
  outputs:
    dev:
      host: 192.168.70.2
      method: thrift
      port: 10000
      schema: dev
      threads: 1
      type: spark
      user: spark_user # obligatoire pour dbt-fusion
    
    ci:
      host: 192.168.70.2
      method: thrift
      port: 10000
      schema: ci 
      threads: 1
      type: spark
      user: spark_user  

    prod:
      host: 192.168.70.2
      method: thrift
      port: 10000
      schema: prod 
      threads: 1
      type: spark
      user: spark_user
    
  target: dev

```

---

## Démo : `materialized=view` avec Gravitino IRC

```bash

# Installer dbt et les autres packages utiles
> pip install -r ./dbt-project/requirements.txt

> cd ./dbt-project/my_project

> dbt debug
14:24:24  Running with dbt=1.11.7
14:24:24  dbt version: 1.11.7
14:24:24  python version: 3.10.2
14:24:24  python path: .dbt_env\Scripts\python.exe
14:24:24  os info: Windows-10-10.0.26100-SP0
14:24:33  Using profiles dir at dbt-spark-iceberg\dbt-project\my_project
14:24:33  Using profiles.yml file at dbt-spark-iceberg\dbt-project\my_project\profiles.yml
14:24:33  Using dbt_project.yml file at dbt-spark-iceberg\dbt-project\my_project\dbt_project.yml
14:24:33  adapter type: spark
14:24:33  adapter version: 1.10.1
14:24:33  Configuration:
14:24:33    profiles.yml file [OK found and valid]
14:24:33    dbt_project.yml file [OK found and valid]
14:24:33  Required dependencies:
14:24:34   - git [OK found]

14:24:34  Connection:
14:24:34    host: 192.168.70.2
14:24:34    port: 10000
14:24:34    cluster: None
14:24:34    endpoint: None
14:24:34    schema: dev
14:24:34    organization: 0
14:24:34  Registered adapter: spark=1.10.1
14:25:13    Connection test: [OK connection ok]

14:25:13  All checks passed!

# Créer les tables et les views Iceberg
> dbt run 
14:25:37  Running with dbt=1.11.7
14:25:39  Registered adapter: spark=1.10.1
14:25:41  Unable to do partial parsing because a project dependency has been added
14:25:56  Found 2 models, 4 data tests, 504 macros
14:25:57
14:25:57  Concurrency: 1 threads (target='dev')
14:25:57
14:26:27  1 of 2 START sql table model dev.my_first_dbt_model ...................... [RUN]
14:27:12  1 of 2 OK created sql table model dev.my_first_dbt_model ................. [OK in 44.80s]
14:27:12  2 of 2 START sql view model dev.my_second_dbt_model ...................... [RUN]
14:27:23  2 of 2 OK created sql view model dev.my_second_dbt_model ................. [OK in 10.78s]
14:27:24
14:27:24  Finished running 1 table model, 1 view model in 0 hours 1 minutes and 27.10 seconds (87.10s).
14:27:24
14:27:24  Completed successfully
14:27:24
14:27:24  Done. PASS=2 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=2


```

```bash
# Vérifications avec beeline
> ./scripts/03-beeline.sh "show namespaces"
```
Résultat :
```
+------------------+
|namespace         |
+------------------+
|dev               |
|default           |
+------------------+
```
```bash
> ./scripts/03-beeline.sh "show tables in dev"
```
Résultat :
```
+---------+------------------+-----------+
|namespace|tableName         |isTemporary|
+---------+------------------+-----------+
|dev      |my_first_dbt_model|false      |
+---------+------------------+-----------+
```

```bash
> ./scripts/03-beeline.sh "show views in dev"
```
Résultat :
```
+---------+-------------------+-----------+
|namespace|viewName           |isTemporary|
+---------+-------------------+-----------+
|dev      |my_second_dbt_model|false      |
+---------+-------------------+-----------+
```

---

## Points importants relevés lors de l'implémentation

**1. Port Gravitino exposé sur 8181 et non 9001**  
Gravitino IRC écoute en interne sur `9001`, mais le port hôte est `8181`
pour éviter le conflit avec la console MinIO (également sur `9001`).
Le catalog Spark pointe vers `http://iceberg-rest:9001/iceberg` (réseau interne Docker).

**2. Endpoint MinIO avec IP externe dans Gravitino**  
`GRAVITINO_S3_ENDPOINT=http://192.168.70.2:9000` utilise l'IP de la VM
et non `warehouse-minio:9000`, afin que des clients externes comme DuckDB
puissent également lire les données Iceberg directement via le credential vending.

**3. JARs copiés au démarrage de Gravitino**  
L'entrypoint copie les JARs supplémentaires (driver PostgreSQL, etc.)
depuis le volume `./les_jars` vers les libs Gravitino avant le démarrage :
```yaml
entrypoint: >
  /bin/sh -c "
  cp /home/extra_jars/*.jar /root/gravitino-iceberg-rest-server/libs/ &&
  exec /root/gravitino-iceberg-rest-server/bin/start-iceberg-rest-server.sh
  "
```

**4. `materialized=view` non supporté par GlueCatalog + OSS Spark**  
Cette limitation est confirmée par AWS : les Iceberg views nécessitent
AWS Glue 5.1+ en environnement managé. Gravitino IRC contourne cette limitation
pour les déploiements on-premise.

---

## CI/CD — GitHub Actions

Le pipeline CI/CD repose sur un **self-hosted runner** installé sur la même machine que Spark.

### Vue d'ensemble

```
PR ouverte              Merge dans main     
     │                        │            
     ▼                        ▼                 
  ci.yml                   cd.yml             
  ───────                  ──────           
  linting                  slim CD           
  slim CI                  state:modified+    
  state:modified+          upload manifest    
```

### `ci.yml` — Pull Request

Déclenché sur PR vers `main`, uniquement si les fichiers dbt changent :

```
paths: models/** · macros/** · tests/** · seeds/** · dbt_project.yml · packages.yml
```

**Jobs :**

| Job | Steps | Dépendance |
|---|---|---|
| `linting` | checkout · dbt deps · dbt parse | — |
| `slim_ci` | checkout · download manifest MinIO · dbt deps · dbt build --select state:modified+ | après `linting` |

Le manifest de référence est téléchargé depuis MinIO (`dbt-artifacts/manifest.json`) pour permettre la comparaison `state:modified+` avec l'état prod.

### `cd.yml` — Merge dans main

Déclenché sur push dans `main`, mêmes path filters que CI :

**Jobs :**

| Step | Détail |
|---|---|
| checkout | — |
| download manifest MinIO | référence prod pour `--defer` |
| dbt deps | — |
| dbt build slim | `--select state:modified+ --defer --state ./artifacts-prod` |
| **upload manifest MinIO** | seulement si build + tests OK |

> ⚠️ Le manifest n'est uploadé sur MinIO **que si le build et les tests passent**. Cela garantit que la référence prod reste toujours dans un état cohérent.

### Artefacts et référence d'état

```
CD run réussi
  └── target-prod/manifest.json
        └── mc cp → MinIO/dbt-artifacts/manifest.json  ← source de vérité

CI / CD slim suivant
  └── mc cp MinIO/dbt-artifacts/manifest.json → /
        └── dbt build --state artifacts-prod/ --select state:modified+
```

Le `manifest.json` encode les checksums SHA-256 de chaque fichier SQL — dbt compare ces checksums pour identifier les modèles modifiés sans accéder aux données.

### Path filtering

| Fichier modifié | CI déclenché | CD déclenché |
|---|---|---|
| `models/**` | ✅ | ✅ |
| `macros/**` | ✅ | ✅ |
| `dbt_project.yml` | ✅ | ✅ |
| `README.md` | ❌ | ❌ |
| `docs/**` | ❌ | ❌ |
| `.github/workflows/ci.yml` | ✅ | ❌ |
| `.github/workflows/cd.yml` | ❌ | ✅ |

---

## Structure du dépôt

```
.
.github/workflows/
│    ├── cd.yml     # slim CI — PR
│    └── ci.yml     # slim CD — merge main
│
├──dbt-project/
│    └──my_project/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       └── models/
│           └── example/
│               ├── my_first_dbt_model.sql           # materialized=table
│               └── my_second_dbt_model.sql         # materialized=view ✅
|
├──spark-iceberg-infra/
│    ├── docker-compose.yml
│    ├── config/
│    │   └── spark_irc_gravitino/
│    │       └── spark-defaults.conf
│    └── les_jars/
│       ├── iceberg-aws-bundle-1.8.x.jar
│       ├── iceberg-spark-runtime-3.5_2.12-1.8.0.jar
│       ├── aws-java-sdk-bundle-1.12.x.jar
│       ├── hadoop-aws-3.3.x.jar
│       ├── gravitino-iceberg-aws-bundle-1.2.0.jar
│       └── postgresql-42.x.x.jar
│
└── README.md
```

---

## Roadmap

- [x] v1.0 — Infrastructure Spark + Gravitino IRC + MinIO + PostgreSQL + démo views
- [x] v2.0 — CI/CD
- [ ] v3.0 — Projet dbt complet (staging / intermediate / marts)
---

