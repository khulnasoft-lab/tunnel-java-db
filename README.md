# tunnel-java-db

## Overview
`tunnel-java-db` parses all indexes from [maven repository](https://repo.maven.apache.org/maven2) and stores `ArtifactID`, `GroupID`, `Version` and `sha1` for jar files to SQlite DB.

The DB is used in Tunnel to discover information about `jars` without GAV inside them.

## Update interval
Every day at 00:00

## Download the java indexes database
You can download the actual compiled database via [Tunnel](https://khulnasoft.github.io/tunnel/) or [Oras CLI](https://oras.land/cli/).

Tunnel:
```sh
TUNNEL_TEMP_DIR=$(mktemp -d)
tunnel --cache-dir $TUNNEL_TEMP_DIR image --download-java-db-only
tar -cf ./javadb.tar.gz -C $TUNNEL_TEMP_DIR/java-db metadata.json tunnel-java.db
rm -rf $TUNNEL_TEMP_DIR
```

oras >= v0.13.0:
```sh
$ oras pull ghcr.io/khulnasoft-lab/tunnel-java-db:1
```

oras < v0.13.0:
```sh
$ oras pull -a ghcr.io/khulnasoft-lab/tunnel-java-db:1
```
The database can be used for [Air-Gapped Environment](https://khulnasoft.github.io/tunnel/latest/docs/advanced/air-gap/).
