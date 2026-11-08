"""DataSlate's bounded, strict file-to-Delta Spark runtime.

The control plane MUST own both target and checkpoint and hold its single-writer
reservation until all application Pods have drained. This module is not a lock.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import os
import re
import sys
import uuid
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit, unquote

SPARK_VERSION = "4.1.3"
DELTA_VERSION = "4.3.1"
DELTA_JAR_SHA256 = "ebf35a4383f0a653c3ee3c7882c0edcac2f846be31ac61af4b96ccfaaf36c171"
RECEIPT_PREFIX = "DATASLATE_INGESTION_RECEIPT "
RECEIPT_LIMIT = 4096
CONTRACT_FILE = "_dataslate_contract.json"
IDENTITY_FILE = "_dataslate_stream_identity.json"
TYPES = {"string", "long", "double", "boolean", "date", "timestamp"}


class IngestionError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _exact(value, keys, code="invalid_spec"):
    if not isinstance(value, dict) or set(value) != set(keys):
        raise IngestionError(code)


def _integer(value, minimum, maximum):
    return type(value) is int and minimum <= value <= maximum


def canonical_uri(value: str, *, allow_local_files=False) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 2048:
        raise IngestionError("invalid_uri")
    if any(ord(c) < 33 for c in value) or any(c in value for c in "\\%*?[]{}#"):
        raise IngestionError("invalid_uri")
    u = urlsplit(value)
    if u.query or u.fragment or u.username or u.password:
        raise IngestionError("invalid_uri")
    if u.scheme in ("s3", "s3a"):
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", u.netloc):
            raise IngestionError("invalid_uri")
        scheme = "s3a"
    elif allow_local_files and u.scheme == "file" and not u.netloc:
        scheme = "file"
    else:
        raise IngestionError("unsupported_storage")
    parts = u.path.split("/")
    if not u.path.startswith("/") or not u.path.endswith("/") or len(parts) < 3:
        raise IngestionError("invalid_uri")
    if any(not re.fullmatch(r"[A-Za-z0-9_-][A-Za-z0-9_.=-]*", p) for p in parts[1:-1]):
        raise IngestionError("invalid_uri")
    return urlunsplit((scheme, u.netloc, u.path, "", ""))


def validate_spec(value: dict, *, allow_local_files=False) -> dict:
    _exact(value, {"version", "pipelineId", "runId", "pipelineRevision", "source", "destination", "checkpointUri", "maxFilesPerTrigger", "timeoutSeconds", "receiptBindingSha256"})
    if value["version"] != 1 or type(value["version"]) is not int:
        raise IngestionError("unsupported_spec_version")
    if not isinstance(value["receiptBindingSha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", value["receiptBindingSha256"]):
        raise IngestionError("invalid_receipt_binding")
    for name in ("pipelineId", "runId"):
        if not isinstance(value[name], str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", value[name]):
            raise IngestionError("invalid_spec")
    if not _integer(value["pipelineRevision"], 1, 2**53-1) or not _integer(value["maxFilesPerTrigger"], 1, 10000) or not _integer(value["timeoutSeconds"], 1, 86400):
        raise IngestionError("invalid_spec")
    _exact(value["source"], {"format", "uri", "schema"})
    _exact(value["destination"], {"uri"})
    if not isinstance(value["source"]["format"], str) or value["source"]["format"] not in ("json", "parquet"):
        raise IngestionError("unsupported_source_format")
    fields = value["source"]["schema"]
    if not isinstance(fields, list) or not 1 <= len(fields) <= 200:
        raise IngestionError("invalid_schema")
    seen = set()
    for field in fields:
        _exact(field, {"name", "type", "nullable"}, "invalid_schema")
        if not isinstance(field["name"], str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", field["name"]) or field["name"].lower() in seen or not isinstance(field["type"], str) or field["type"] not in TYPES or type(field["nullable"]) is not bool:
            raise IngestionError("invalid_schema")
        seen.add(field["name"].lower())
    # A JSON round trip detaches the accepted contract from caller mutation.
    spec = json.loads(json.dumps(value, allow_nan=False))
    locations = [canonical_uri(value["source"]["uri"], allow_local_files=allow_local_files), canonical_uri(value["destination"]["uri"], allow_local_files=allow_local_files), canonical_uri(value["checkpointUri"], allow_local_files=allow_local_files)]
    for i, a in enumerate(locations):
        for b in locations[i+1:]:
            if a.startswith(b) or b.startswith(a):
                raise IngestionError("overlapping_locations")
    spec["source"]["uri"], spec["destination"]["uri"], spec["checkpointUri"] = locations
    return spec


def contract_for(spec):
    return {"version": 1, "pipelineId": spec["pipelineId"], "source": spec["source"], "destination": spec["destination"], "checkpointUri": spec["checkpointUri"], "runtime": {"spark": SPARK_VERSION, "delta": DELTA_VERSION}, "semantics": "immutable-file-append-v1"}


def _canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise IngestionError("duplicate_json_field")
        result[key] = value
    return result


def parse_json_record(line: str, fields: list[dict]):
    """Strict NDJSON conversion; values are never included in raised errors."""
    try:
        obj = json.loads(line, object_pairs_hook=_object, parse_constant=lambda _: (_ for _ in ()).throw(IngestionError("invalid_json_number")))
    except IngestionError:
        raise
    except (ValueError, TypeError, RecursionError):
        raise IngestionError("malformed_json") from None
    if not isinstance(obj, dict) or set(obj) != {f["name"] for f in fields}:
        raise IngestionError("json_schema_drift")
    result = []
    for field in fields:
        value, kind = obj[field["name"]], field["type"]
        if value is None:
            if not field["nullable"]:
                raise IngestionError("required_value_missing")
        elif kind == "string":
            if not isinstance(value, str):
                raise IngestionError("json_type_mismatch")
        elif kind == "long":
            if type(value) is not int or not -(2**63) <= value < 2**63:
                raise IngestionError("json_type_mismatch")
        elif kind == "double":
            if type(value) not in (int, float) or not math.isfinite(value):
                raise IngestionError("json_type_mismatch")
            value = float(value)
        elif kind == "boolean":
            if type(value) is not bool:
                raise IngestionError("json_type_mismatch")
        elif kind == "date":
            try:
                if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                    raise ValueError()
                value = dt.date.fromisoformat(value)
            except ValueError:
                raise IngestionError("json_type_mismatch") from None
        elif kind == "timestamp":
            try:
                if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})", value):
                    raise ValueError()
                value = dt.datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
            except ValueError:
                raise IngestionError("json_type_mismatch") from None
        result.append(value)
    return tuple(result)


def _schema(fields):
    from pyspark.sql import types as T
    types = {"string": T.StringType, "long": T.LongType, "double": T.DoubleType, "boolean": T.BooleanType, "date": T.DateType, "timestamp": T.TimestampType}
    return T.StructType([T.StructField(f["name"], types[f["type"]](), f["nullable"]) for f in fields])


class _Storage:
    def __init__(self, spark):
        self.jvm, self.conf = spark._jvm, spark.sparkContext._jsc.hadoopConfiguration()

    def _path(self, uri):
        path = self.jvm.org.apache.hadoop.fs.Path(uri)
        return path, path.getFileSystem(self.conf)

    def exists(self, uri):
        path, fs = self._path(uri)
        return fs.exists(path)

    def children(self, uri):
        path, fs = self._path(uri)
        if not fs.exists(path):
            return []
        if not fs.getFileStatus(path).isDirectory():
            raise IngestionError("location_is_not_directory")
        return [str(s.getPath().getName()) for s in fs.listStatus(path)]

    def read_json(self, uri):
        path, fs = self._path(uri)
        if fs.getFileStatus(path).getLen() > 65536:
            raise IngestionError("invalid_storage_manifest")
        stream = fs.open(path)
        try:
            text = self.jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
            return json.loads(text, object_pairs_hook=_object)
        finally:
            stream.close()

    def write_json(self, uri, value):
        path, fs = self._path(uri)
        fs.mkdirs(path.getParent())
        stream = fs.create(path, False)
        try:
            stream.write(bytearray(_canonical_json(value).encode("utf-8")))
        finally:
            stream.close()
        if self.read_json(uri) != value:
            raise IngestionError("storage_manifest_write_uncertain")

    def verify_file(self, uri, size, modified):
        path, fs = self._path(uri)
        status = fs.getFileStatus(path)
        if not status.isFile() or status.getLen() != size or status.getModificationTime() != modified:
            raise IngestionError("source_file_changed")


def _ensure_contract(storage, spec):
    contract = contract_for(spec)
    target, checkpoint = spec["destination"]["uri"], spec["checkpointUri"]
    for location in (target, checkpoint):
        marker = location + CONTRACT_FILE
        if storage.exists(marker):
            if storage.read_json(marker) != contract:
                raise IngestionError("stored_contract_mismatch")
        elif storage.children(location):
            raise IngestionError("unowned_location")
    # A partial first setup is recoverable. Never adopt an existing Delta table
    # or silently rebuild a lost checkpoint after any table data was committed.
    if storage.exists(target + "_delta_log") and not storage.exists(checkpoint + "metadata"):
        raise IngestionError("checkpoint_missing")
    for location in (target, checkpoint):
        if not storage.exists(location + CONTRACT_FILE):
            storage.write_json(location + CONTRACT_FILE, contract)
    return hashlib.sha256(_canonical_json(contract).encode()).hexdigest()


def _ensure_query_identity(storage, spec):
    metadata = storage.read_json(spec["checkpointUri"] + "metadata")
    if set(metadata) != {"id"} or not isinstance(metadata["id"], str) or not re.fullmatch(r"[0-9a-f-]{36}", metadata["id"]):
        raise IngestionError("invalid_checkpoint_identity")
    marker = spec["destination"]["uri"] + IDENTITY_FILE
    if storage.exists(marker):
        if storage.read_json(marker) != metadata:
            raise IngestionError("checkpoint_identity_changed")
    else:
        if storage.exists(spec["destination"]["uri"] + "_delta_log"):
            raise IngestionError("checkpoint_identity_missing")
        storage.write_json(marker, metadata)


def _plain_source_file(storage, source, discovered):
    """Reject accidental raw-file scans of a Delta table (including nested ones)."""
    root, file = urlsplit(source), urlsplit(discovered)
    if root.scheme != file.scheme or root.netloc != file.netloc or not file.path.startswith(root.path):
        raise IngestionError("source_file_outside_prefix")
    parent = file.path.rsplit("/", 1)[0] + "/"
    while len(parent) >= len(root.path):
        if storage.exists(urlunsplit((file.scheme, file.netloc, parent + "_delta_log", "", ""))):
            raise IngestionError("delta_source_requires_table_reader")
        if parent == root.path:
            break
        parent = parent.rstrip("/").rsplit("/", 1)[0] + "/"


def _verify_runtime(spark):
    if spark.version != SPARK_VERSION or str(spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()) != "3.4.2":
        raise IngestionError("runtime_version_mismatch")
    cls = spark._jvm.java.lang.Class.forName("io.delta.tables.DeltaTable", True, spark._jvm.java.lang.Thread.currentThread().getContextClassLoader())
    location = str(cls.getProtectionDomain().getCodeSource().getLocation().toURI())
    parsed = urlsplit(location)
    if parsed.scheme != "file":
        raise IngestionError("delta_runtime_unverified")
    jar = Path(unquote(parsed.path))
    if not jar.is_file() or hashlib.sha256(jar.read_bytes()).hexdigest() != DELTA_JAR_SHA256:
        raise IngestionError("delta_runtime_unverified")


def _verify_transaction_retention(spark, storage, target):
    # Expiring SetTransaction records can make an old committed batch replay.
    # The dedicated ingestion runtime never opts into finite dedup retention.
    if spark.conf.get("spark.databricks.delta.properties.defaults.setTransactionRetentionDuration", None) is not None:
        raise IngestionError("transaction_retention_not_supported")
    if storage.exists(target + "_delta_log"):
        properties = spark.sql("DESCRIBE DETAIL delta.`" + target.replace("`", "``") + "`").select("properties").first()[0]
        if any(k.lower() == "delta.settransactionretentionduration" for k in properties):
            raise IngestionError("transaction_retention_not_supported")


def _parquet_frame(spark, paths, fields):
    from functools import reduce
    from pyspark.sql import functions as F
    expected = _schema(fields)
    expected_names_types = [(f.name, f.dataType) for f in expected]
    # Inspect EVERY file, including empty files. Schema merging/projection can
    # otherwise silently discard extra fields or fabricate missing null columns.
    for path in paths:
        actual = spark.read.parquet(path).schema
        if [(f.name, f.dataType) for f in actual] != expected_names_types:
            raise IngestionError("parquet_schema_drift")
    df = spark.read.schema(expected).parquet(*paths)
    required = [F.col(f["name"]).isNull() for f in fields if not f["nullable"]]
    if required and df.filter(reduce(lambda a, b: a | b, required)).limit(1).count():
        raise IngestionError("required_value_missing")
    return df


def _json_frame(spark, paths, fields):
    # RDD conversion preserves exact types instead of Spark JSON's permissive
    # numeric/string coercions. Errors fail the entire Delta microbatch commit.
    from pyspark.sql import functions as F
    text = spark.read.text(paths)
    if text.filter(~F.is_valid_utf8("value")).limit(1).count():
        raise IngestionError("invalid_utf8")
    rows = text.rdd.map(lambda row: parse_json_record(row.value, fields))
    return spark.createDataFrame(rows, schema=_schema(fields), verifySchema=True)


def _snapshot_schema_hash(schema_json, fields):
    """Attest the physical Delta schema, separately from source required values.

    Delta's pinned append writer normalizes fields to nullable. It does not
    create NOT NULL constraints from the source validation contract.
    """
    if not isinstance(schema_json, str) or len(schema_json.encode()) > 65536:
        raise IngestionError("delta_schema_unverified")
    try:
        schema = json.loads(schema_json, object_pairs_hook=_object)
        _exact(schema, {"type", "fields"}, "delta_schema_unverified")
        if schema["type"] != "struct" or not isinstance(schema["fields"], list) or len(schema["fields"]) != len(fields):
            raise IngestionError("delta_schema_unverified")
        normalized = []
        for actual, expected in zip(schema["fields"], fields):
            _exact(actual, {"name", "type", "nullable", "metadata"}, "delta_schema_unverified")
            if actual["name"] != expected["name"] or actual["type"] != expected["type"] or actual["nullable"] is not True or actual["metadata"] != {}:
                raise IngestionError("delta_schema_unverified")
            normalized.append({"name": actual["name"], "type": actual["type"], "nullable": True})
        return hashlib.sha256(_canonical_json(normalized).encode()).hexdigest()
    except (ValueError, TypeError, RecursionError):
        raise IngestionError("delta_schema_unverified") from None


def _features(option):
    names = []
    if option.isDefined():
        iterator = option.get().iterator()
        while iterator.hasNext():
            name = str(iterator.next())
            if not re.fullmatch(r"[A-Za-z][A-Za-z0-9]{0,63}", name) or len(names) >= 16 or name in names:
                raise IngestionError("delta_protocol_unverified")
            names.append(name)
    return sorted(names)


def _delta_snapshot(spark, storage, spec):
    if not storage.exists(spec["destination"]["uri"] + "_delta_log"):
        return None
    # One immutable fresh snapshot prevents mixing metadata from different
    # versions. This JVM interface is qualified against the SHA-locked Delta jar.
    snapshot = spark._jvm.org.apache.spark.sql.delta.DeltaLog.forTableWithSnapshot(
        spark._jsparkSession, spec["destination"]["uri"])._2()
    version = int(snapshot.version())
    table_id = str(snapshot.metadata().id())
    if version < 0 or version > 2**63-1 or str(uuid.UUID(table_id)) != table_id:
        raise IngestionError("delta_snapshot_unverified")
    protocol = snapshot.protocol()
    reader, writer = int(protocol.minReaderVersion()), int(protocol.minWriterVersion())
    if not _integer(reader, 1, 3) or not _integer(writer, 1, 7):
        raise IngestionError("delta_protocol_unverified")
    return {"id": table_id, "version": str(version),
            "schemaSha256": _snapshot_schema_hash(str(snapshot.metadata().schemaString()), spec["source"]["schema"]),
            "minReaderVersion": reader, "minWriterVersion": writer,
            "readerFeatures": _features(protocol.readerFeatures()), "writerFeatures": _features(protocol.writerFeatures())}


def _write_receipt(path, receipt):
    payload = _canonical_json(receipt).encode("utf-8")
    if len(payload) > RECEIPT_LIMIT:
        raise IngestionError("receipt_too_large")
    # Kubernetes mounts a regular termination-message file. Never use a rename
    # over that mount, follow a symlink, or fall back to an unbounded log tail.
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    except (OSError, ValueError, TypeError):
        raise IngestionError("receipt_write_failed") from None


def run(value: dict, *, allow_local_files=False, receipt_path="/dev/termination-log"):
    """Execute one available-now drain. Owns and stops the fresh Spark session.

    allow_local_files is for the explicit local qualification harness only;
    generated production applications must never enable it.
    """
    spec = validate_spec(value, allow_local_files=allow_local_files)
    spark = query = None
    receipt = {"version": 2, "pipelineId": spec["pipelineId"], "runId": spec["runId"], "pipelineRevision": spec["pipelineRevision"], "receiptBindingSha256": spec["receiptBindingSha256"], "state": "failed", "runtime": {"spark": SPARK_VERSION, "delta": DELTA_VERSION}, "tableExists": False, "table": None}
    stats = {"completedBatches": 0, "processedFiles": 0, "processedRows": 0, "lastBatchId": None}
    error = None
    try:
        from pyspark.sql import SparkSession, types as T, functions as F
        if SparkSession.getActiveSession() is not None:
            raise IngestionError("fresh_spark_session_required")
        builder = SparkSession.builder.appName("DataSlate ingestion " + spec["pipelineId"])
        if any(uri.startswith("s3a:") for uri in (spec["source"]["uri"], spec["destination"]["uri"], spec["checkpointUri"])):
            region = os.environ.get("AWS_REGION", "")
            role = os.environ.get("AWS_ROLE_ARN", "")
            token_file = os.environ.get("AWS_WEB_IDENTITY_TOKEN_FILE", "")
            if not re.fullmatch(r"[a-z]{2}(?:-[a-z]+)+-\d", region) or not role.startswith("arn:aws:iam::") or not token_file or not Path(token_file).is_file():
                raise IngestionError("workload_identity_required")
            # No environment static-key, shared credentials, or node-role fallback.
            builder = (builder.config("spark.hadoop.fs.s3a.aws.credentials.provider", "software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider")
                       .config("spark.hadoop.fs.s3a.endpoint.region", region)
                       .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true"))
        spark = (builder
                 .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                 .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                 .config("spark.sql.parquet.compression.codec", "snappy")
                 .config("spark.sql.parquet.mergeSchema", "false")
                 .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
                 .config("spark.sql.files.ignoreMissingFiles", "false")
                 .config("spark.sql.files.ignoreCorruptFiles", "false")
                 .config("spark.sql.caseSensitive", "true")
                 .config("spark.sql.session.timeZone", "UTC")
                 .config("spark.sql.streaming.stopTimeout", "15000")
                 .getOrCreate())
        _verify_runtime(spark)
        storage = _Storage(spark)
        contract_hash = _ensure_contract(storage, spec)
        _verify_transaction_retention(spark, storage, spec["destination"]["uri"])
        receipt["contractSha256"] = contract_hash
        # Project away content before execution: binaryFile is used only for
        # checkpointed file discovery, never to load whole files in the driver.
        metadata_schema = T.StructType([T.StructField("path", T.StringType()), T.StructField("modificationTime", T.TimestampType()), T.StructField("length", T.LongType()), T.StructField("content", T.BinaryType())])
        files = (spark.readStream.format("binaryFile").schema(metadata_schema)
                 .option("maxFilesPerTrigger", spec["maxFilesPerTrigger"])
                 .option("maxFileAge", "365000d")
                 .option("fileNameOnly", "false").option("latestFirst", "false")
                 .option("cleanSource", "off").option("recursiveFileLookup", "true")
                 .load(spec["source"]["uri"]).select("path", "length", F.unix_millis("modificationTime").alias("modifiedMillis")))

        def consume(batch, batch_id):
            discovered = batch.limit(spec["maxFilesPerTrigger"] + 1).collect()
            if len(discovered) > spec["maxFilesPerTrigger"]:
                raise IngestionError("file_batch_limit_exceeded")
            if not discovered:
                return
            paths = []
            for row in discovered:
                # All non-hidden files must have the declared format. Do not
                # silently omit a CSV or an unexpected extension in the landing zone.
                suffix = ".json" if spec["source"]["format"] == "json" else ".parquet"
                if not row.path.endswith(suffix) or len(row.path) > 4096:
                    raise IngestionError("unexpected_source_file")
                storage.verify_file(row.path, row.length, row.modifiedMillis)
                _plain_source_file(storage, spec["source"]["uri"], row.path)
                paths.append(row.path)
            _ensure_query_identity(storage, spec)
            frame = (_json_frame if spec["source"]["format"] == "json" else _parquet_frame)(spark, paths, spec["source"]["schema"])
            frame.persist()
            try:
                count = frame.count()  # Validate every row before the Delta write.
                for row in discovered:
                    storage.verify_file(row.path, row.length, row.modifiedMillis)
                (frame.write.format("delta").mode("append")
                 .option("compression", "snappy").option("mergeSchema", "false")
                 .option("txnAppId", "dataslate-" + contract_hash).option("txnVersion", batch_id)
                 .save(spec["destination"]["uri"]))
                stats["completedBatches"] += 1
                stats["processedFiles"] += len(paths)
                stats["processedRows"] += count
                stats["lastBatchId"] = batch_id
            finally:
                frame.unpersist()

        query = (files.writeStream.foreachBatch(consume).outputMode("append")
                 .option("checkpointLocation", spec["checkpointUri"])
                 .trigger(availableNow=True).start())
        terminated = query.awaitTermination(spec["timeoutSeconds"])
        if not terminated or query.isActive:
            raise IngestionError("ingestion_timeout")
        if query.exception() is not None:
            raise IngestionError("stream_failed")
        receipt["queryId"] = str(query.id)
        receipt["table"] = _delta_snapshot(spark, storage, spec)
        receipt["tableExists"] = receipt["table"] is not None
        receipt["state"] = "succeeded"
    except BaseException as exc:
        error = exc
        receipt["errorCode"] = exc.code if isinstance(exc, IngestionError) else "runtime_failed"
    finally:
        try:
            if query is not None and query.isActive:
                query.stop()
            if spark is not None:
                spark.stop()
        except BaseException as stop_error:
            receipt["state"] = "failed"
            receipt["errorCode"] = "runtime_stop_uncertain"
            error = error or stop_error
        receipt.update({key: str(value) if value is not None else None for key, value in stats.items()})
        try:
            # Capture before the controller deletes the terminal driver. The
            # immutable source/profile plus Pod UID provide provenance; this is
            # not an independently signed or storage-authenticated document.
            _write_receipt(receipt_path, receipt)
        except IngestionError as receipt_error:
            receipt["state"] = "failed"
            receipt["errorCode"] = receipt_error.code
            error = error or receipt_error
        # A success marker requires termination, Spark stop AND receipt storage.
        print(RECEIPT_PREFIX + _canonical_json(receipt), flush=True)
    if error is not None:
        raise error
    return receipt


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: dataslate_ingestion.py SPEC_JSON_FILE")
    payload = Path(sys.argv[1]).read_bytes()
    if len(payload) > 65536:
        raise SystemExit("spec too large")
    run(json.loads(payload, object_pairs_hook=_object))
