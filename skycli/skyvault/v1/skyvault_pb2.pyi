from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class WriteBatchRequest(_message.Message):
    __slots__ = ("tables",)
    TABLES_FIELD_NUMBER: _ClassVar[int]
    tables: _containers.RepeatedCompositeFieldContainer[TableWriteBatchRequest]
    def __init__(self, tables: _Optional[_Iterable[_Union[TableWriteBatchRequest, _Mapping]]] = ...) -> None: ...

class TableWriteBatchRequest(_message.Message):
    __slots__ = ("table_name", "items")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    items: _containers.RepeatedCompositeFieldContainer[WriteBatchItem]
    def __init__(self, table_name: _Optional[str] = ..., items: _Optional[_Iterable[_Union[WriteBatchItem, _Mapping]]] = ...) -> None: ...

class WriteBatchItem(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: bytes
    def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...

class WriteBatchResponse(_message.Message):
    __slots__ = ("seq_no",)
    SEQ_NO_FIELD_NUMBER: _ClassVar[int]
    seq_no: int
    def __init__(self, seq_no: _Optional[int] = ...) -> None: ...

class GetFromRunRequest(_message.Message):
    __slots__ = ("run_ids", "keys")
    RUN_IDS_FIELD_NUMBER: _ClassVar[int]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    run_ids: _containers.RepeatedScalarFieldContainer[str]
    keys: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, run_ids: _Optional[_Iterable[str]] = ..., keys: _Optional[_Iterable[str]] = ...) -> None: ...

class GetFromRunResponse(_message.Message):
    __slots__ = ("items",)
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[GetFromRunItem]
    def __init__(self, items: _Optional[_Iterable[_Union[GetFromRunItem, _Mapping]]] = ...) -> None: ...

class GetFromRunItem(_message.Message):
    __slots__ = ("key", "value", "deleted")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    DELETED_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: bytes
    deleted: _empty_pb2.Empty
    def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ..., deleted: _Optional[_Union[_empty_pb2.Empty, _Mapping]] = ...) -> None: ...

class ScanFromRunRequest(_message.Message):
    __slots__ = ("run_ids", "exclusive_start_key", "max_results")
    RUN_IDS_FIELD_NUMBER: _ClassVar[int]
    EXCLUSIVE_START_KEY_FIELD_NUMBER: _ClassVar[int]
    MAX_RESULTS_FIELD_NUMBER: _ClassVar[int]
    run_ids: _containers.RepeatedScalarFieldContainer[str]
    exclusive_start_key: str
    max_results: int
    def __init__(self, run_ids: _Optional[_Iterable[str]] = ..., exclusive_start_key: _Optional[str] = ..., max_results: _Optional[int] = ...) -> None: ...

class ScanFromRunResponse(_message.Message):
    __slots__ = ("items",)
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[GetFromRunItem]
    def __init__(self, items: _Optional[_Iterable[_Union[GetFromRunItem, _Mapping]]] = ...) -> None: ...

class GetBatchRequest(_message.Message):
    __slots__ = ("seq_no", "tables")
    SEQ_NO_FIELD_NUMBER: _ClassVar[int]
    TABLES_FIELD_NUMBER: _ClassVar[int]
    seq_no: int
    tables: _containers.RepeatedCompositeFieldContainer[TableGetBatchRequest]
    def __init__(self, seq_no: _Optional[int] = ..., tables: _Optional[_Iterable[_Union[TableGetBatchRequest, _Mapping]]] = ...) -> None: ...

class TableGetBatchRequest(_message.Message):
    __slots__ = ("table_name", "keys")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    keys: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, table_name: _Optional[str] = ..., keys: _Optional[_Iterable[str]] = ...) -> None: ...

class GetBatchResponse(_message.Message):
    __slots__ = ("tables",)
    TABLES_FIELD_NUMBER: _ClassVar[int]
    tables: _containers.RepeatedCompositeFieldContainer[TableGetBatchResponse]
    def __init__(self, tables: _Optional[_Iterable[_Union[TableGetBatchResponse, _Mapping]]] = ...) -> None: ...

class TableGetBatchResponse(_message.Message):
    __slots__ = ("table_name", "items")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    items: _containers.RepeatedCompositeFieldContainer[GetBatchItem]
    def __init__(self, table_name: _Optional[str] = ..., items: _Optional[_Iterable[_Union[GetBatchItem, _Mapping]]] = ...) -> None: ...

class GetBatchItem(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: bytes
    def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...

class ScanRequest(_message.Message):
    __slots__ = ("table_name", "exclusive_start_key", "max_results")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    EXCLUSIVE_START_KEY_FIELD_NUMBER: _ClassVar[int]
    MAX_RESULTS_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    exclusive_start_key: str
    max_results: int
    def __init__(self, table_name: _Optional[str] = ..., exclusive_start_key: _Optional[str] = ..., max_results: _Optional[int] = ...) -> None: ...

class ScanResponse(_message.Message):
    __slots__ = ("items",)
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[ScanItem]
    def __init__(self, items: _Optional[_Iterable[_Union[ScanItem, _Mapping]]] = ...) -> None: ...

class ScanItem(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: bytes
    def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...

class TableTreeCompaction(_message.Message):
    __slots__ = ("table_id", "level")
    TABLE_ID_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    table_id: int
    level: int
    def __init__(self, table_id: _Optional[int] = ..., level: _Optional[int] = ...) -> None: ...

class JobParams(_message.Message):
    __slots__ = ("wal_compaction", "table_buffer_compaction", "table_tree_compaction")
    WAL_COMPACTION_FIELD_NUMBER: _ClassVar[int]
    TABLE_BUFFER_COMPACTION_FIELD_NUMBER: _ClassVar[int]
    TABLE_TREE_COMPACTION_FIELD_NUMBER: _ClassVar[int]
    wal_compaction: _empty_pb2.Empty
    table_buffer_compaction: int
    table_tree_compaction: TableTreeCompaction
    def __init__(self, wal_compaction: _Optional[_Union[_empty_pb2.Empty, _Mapping]] = ..., table_buffer_compaction: _Optional[int] = ..., table_tree_compaction: _Optional[_Union[TableTreeCompaction, _Mapping]] = ...) -> None: ...

class JobStatus(_message.Message):
    __slots__ = ("pending", "seq_no", "failed")
    PENDING_FIELD_NUMBER: _ClassVar[int]
    SEQ_NO_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    pending: bool
    seq_no: int
    failed: bool
    def __init__(self, pending: bool = ..., seq_no: _Optional[int] = ..., failed: bool = ...) -> None: ...

class KickOffJobRequest(_message.Message):
    __slots__ = ("params",)
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    params: JobParams
    def __init__(self, params: _Optional[_Union[JobParams, _Mapping]] = ...) -> None: ...

class KickOffJobResponse(_message.Message):
    __slots__ = ("job_id",)
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    job_id: int
    def __init__(self, job_id: _Optional[int] = ...) -> None: ...

class ListJobsRequest(_message.Message):
    __slots__ = ("limit",)
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    limit: int
    def __init__(self, limit: _Optional[int] = ...) -> None: ...

class Job(_message.Message):
    __slots__ = ("id", "params", "status")
    ID_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    id: int
    params: JobParams
    status: JobStatus
    def __init__(self, id: _Optional[int] = ..., params: _Optional[_Union[JobParams, _Mapping]] = ..., status: _Optional[_Union[JobStatus, _Mapping]] = ...) -> None: ...

class ListJobsResponse(_message.Message):
    __slots__ = ("jobs",)
    JOBS_FIELD_NUMBER: _ClassVar[int]
    jobs: _containers.RepeatedCompositeFieldContainer[Job]
    def __init__(self, jobs: _Optional[_Iterable[_Union[Job, _Mapping]]] = ...) -> None: ...

class GetJobStatusRequest(_message.Message):
    __slots__ = ("job_id",)
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    job_id: int
    def __init__(self, job_id: _Optional[int] = ...) -> None: ...

class GetJobStatusResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: JobStatus
    def __init__(self, status: _Optional[_Union[JobStatus, _Mapping]] = ...) -> None: ...

class DumpSnapshotRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class DumpSnapshotResponse(_message.Message):
    __slots__ = ("snapshot",)
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    snapshot: Snapshot
    def __init__(self, snapshot: _Optional[_Union[Snapshot, _Mapping]] = ...) -> None: ...

class Snapshot(_message.Message):
    __slots__ = ("seq_no", "tables", "wal", "trees")
    class TablesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: TableConfig
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[TableConfig, _Mapping]] = ...) -> None: ...
    SEQ_NO_FIELD_NUMBER: _ClassVar[int]
    TABLES_FIELD_NUMBER: _ClassVar[int]
    WAL_FIELD_NUMBER: _ClassVar[int]
    TREES_FIELD_NUMBER: _ClassVar[int]
    seq_no: int
    tables: _containers.MessageMap[str, TableConfig]
    wal: _containers.RepeatedCompositeFieldContainer[RunMetadata]
    trees: _containers.RepeatedCompositeFieldContainer[TableTree]
    def __init__(self, seq_no: _Optional[int] = ..., tables: _Optional[_Mapping[str, TableConfig]] = ..., wal: _Optional[_Iterable[_Union[RunMetadata, _Mapping]]] = ..., trees: _Optional[_Iterable[_Union[TableTree, _Mapping]]] = ...) -> None: ...

class TableTree(_message.Message):
    __slots__ = ("table_id", "buffer", "levels")
    TABLE_ID_FIELD_NUMBER: _ClassVar[int]
    BUFFER_FIELD_NUMBER: _ClassVar[int]
    LEVELS_FIELD_NUMBER: _ClassVar[int]
    table_id: int
    buffer: _containers.RepeatedCompositeFieldContainer[RunMetadata]
    levels: _containers.RepeatedCompositeFieldContainer[TableLevel]
    def __init__(self, table_id: _Optional[int] = ..., buffer: _Optional[_Iterable[_Union[RunMetadata, _Mapping]]] = ..., levels: _Optional[_Iterable[_Union[TableLevel, _Mapping]]] = ...) -> None: ...

class TableLevel(_message.Message):
    __slots__ = ("level", "runs")
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    RUNS_FIELD_NUMBER: _ClassVar[int]
    level: int
    runs: _containers.RepeatedCompositeFieldContainer[RunMetadata]
    def __init__(self, level: _Optional[int] = ..., runs: _Optional[_Iterable[_Union[RunMetadata, _Mapping]]] = ...) -> None: ...

class DumpChangelogRequest(_message.Message):
    __slots__ = ("from_seq_no",)
    FROM_SEQ_NO_FIELD_NUMBER: _ClassVar[int]
    from_seq_no: int
    def __init__(self, from_seq_no: _Optional[int] = ...) -> None: ...

class DumpChangelogResponse(_message.Message):
    __slots__ = ("entries",)
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[ChangelogEntryWithID]
    def __init__(self, entries: _Optional[_Iterable[_Union[ChangelogEntryWithID, _Mapping]]] = ...) -> None: ...

class ChangelogEntryWithID(_message.Message):
    __slots__ = ("id", "runs_changelog_entry_v1", "table_changelog_entry_v1")
    ID_FIELD_NUMBER: _ClassVar[int]
    RUNS_CHANGELOG_ENTRY_V1_FIELD_NUMBER: _ClassVar[int]
    TABLE_CHANGELOG_ENTRY_V1_FIELD_NUMBER: _ClassVar[int]
    id: int
    runs_changelog_entry_v1: RunsChangelogEntryV1
    table_changelog_entry_v1: TableChangelogEntryV1
    def __init__(self, id: _Optional[int] = ..., runs_changelog_entry_v1: _Optional[_Union[RunsChangelogEntryV1, _Mapping]] = ..., table_changelog_entry_v1: _Optional[_Union[TableChangelogEntryV1, _Mapping]] = ...) -> None: ...

class RunsChangelogEntryV1(_message.Message):
    __slots__ = ("runs_added", "runs_removed")
    RUNS_ADDED_FIELD_NUMBER: _ClassVar[int]
    RUNS_REMOVED_FIELD_NUMBER: _ClassVar[int]
    runs_added: _containers.RepeatedScalarFieldContainer[str]
    runs_removed: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, runs_added: _Optional[_Iterable[str]] = ..., runs_removed: _Optional[_Iterable[str]] = ...) -> None: ...

class TableChangelogEntryV1(_message.Message):
    __slots__ = ("table_created", "table_dropped")
    TABLE_CREATED_FIELD_NUMBER: _ClassVar[int]
    TABLE_DROPPED_FIELD_NUMBER: _ClassVar[int]
    table_created: TableCreated
    table_dropped: TableDropped
    def __init__(self, table_created: _Optional[_Union[TableCreated, _Mapping]] = ..., table_dropped: _Optional[_Union[TableDropped, _Mapping]] = ...) -> None: ...

class TableCreated(_message.Message):
    __slots__ = ("table_id",)
    TABLE_ID_FIELD_NUMBER: _ClassVar[int]
    table_id: int
    def __init__(self, table_id: _Optional[int] = ...) -> None: ...

class TableDropped(_message.Message):
    __slots__ = ("table_id",)
    TABLE_ID_FIELD_NUMBER: _ClassVar[int]
    table_id: int
    def __init__(self, table_id: _Optional[int] = ...) -> None: ...

class RunMetadata(_message.Message):
    __slots__ = ("id", "wal_seq_no", "table_buffer", "table_tree", "stats_v1")
    ID_FIELD_NUMBER: _ClassVar[int]
    WAL_SEQ_NO_FIELD_NUMBER: _ClassVar[int]
    TABLE_BUFFER_FIELD_NUMBER: _ClassVar[int]
    TABLE_TREE_FIELD_NUMBER: _ClassVar[int]
    STATS_V1_FIELD_NUMBER: _ClassVar[int]
    id: str
    wal_seq_no: int
    table_buffer: TableBuffer
    table_tree: TableTreeLevel
    stats_v1: StatsV1
    def __init__(self, id: _Optional[str] = ..., wal_seq_no: _Optional[int] = ..., table_buffer: _Optional[_Union[TableBuffer, _Mapping]] = ..., table_tree: _Optional[_Union[TableTreeLevel, _Mapping]] = ..., stats_v1: _Optional[_Union[StatsV1, _Mapping]] = ...) -> None: ...

class TableBuffer(_message.Message):
    __slots__ = ("table_id", "seq_no")
    TABLE_ID_FIELD_NUMBER: _ClassVar[int]
    SEQ_NO_FIELD_NUMBER: _ClassVar[int]
    table_id: int
    seq_no: int
    def __init__(self, table_id: _Optional[int] = ..., seq_no: _Optional[int] = ...) -> None: ...

class TableTreeLevel(_message.Message):
    __slots__ = ("table_id", "level")
    TABLE_ID_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    table_id: int
    level: int
    def __init__(self, table_id: _Optional[int] = ..., level: _Optional[int] = ...) -> None: ...

class StatsV1(_message.Message):
    __slots__ = ("min_key", "max_key", "size_bytes", "put_count", "delete_count")
    MIN_KEY_FIELD_NUMBER: _ClassVar[int]
    MAX_KEY_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    PUT_COUNT_FIELD_NUMBER: _ClassVar[int]
    DELETE_COUNT_FIELD_NUMBER: _ClassVar[int]
    min_key: str
    max_key: str
    size_bytes: int
    put_count: int
    delete_count: int
    def __init__(self, min_key: _Optional[str] = ..., max_key: _Optional[str] = ..., size_bytes: _Optional[int] = ..., put_count: _Optional[int] = ..., delete_count: _Optional[int] = ...) -> None: ...

class PersistSnapshotRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class PersistSnapshotResponse(_message.Message):
    __slots__ = ("snapshot_id", "seq_no")
    SNAPSHOT_ID_FIELD_NUMBER: _ClassVar[int]
    SEQ_NO_FIELD_NUMBER: _ClassVar[int]
    snapshot_id: str
    seq_no: int
    def __init__(self, snapshot_id: _Optional[str] = ..., seq_no: _Optional[int] = ...) -> None: ...

class CreateTableRequest(_message.Message):
    __slots__ = ("config",)
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    config: TableConfig
    def __init__(self, config: _Optional[_Union[TableConfig, _Mapping]] = ...) -> None: ...

class CreateTableResponse(_message.Message):
    __slots__ = ("seq_no",)
    SEQ_NO_FIELD_NUMBER: _ClassVar[int]
    seq_no: int
    def __init__(self, seq_no: _Optional[int] = ...) -> None: ...

class ListTablesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListTablesResponse(_message.Message):
    __slots__ = ("tables",)
    TABLES_FIELD_NUMBER: _ClassVar[int]
    tables: _containers.RepeatedCompositeFieldContainer[TableConfig]
    def __init__(self, tables: _Optional[_Iterable[_Union[TableConfig, _Mapping]]] = ...) -> None: ...

class DropTableRequest(_message.Message):
    __slots__ = ("table_name",)
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    def __init__(self, table_name: _Optional[str] = ...) -> None: ...

class DropTableResponse(_message.Message):
    __slots__ = ("seq_no",)
    SEQ_NO_FIELD_NUMBER: _ClassVar[int]
    seq_no: int
    def __init__(self, seq_no: _Optional[int] = ...) -> None: ...

class GetTableRequest(_message.Message):
    __slots__ = ("table_name",)
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    def __init__(self, table_name: _Optional[str] = ...) -> None: ...

class GetTableResponse(_message.Message):
    __slots__ = ("table",)
    TABLE_FIELD_NUMBER: _ClassVar[int]
    table: TableConfig
    def __init__(self, table: _Optional[_Union[TableConfig, _Mapping]] = ...) -> None: ...

class TableConfig(_message.Message):
    __slots__ = ("table_id", "table_name")
    TABLE_ID_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    table_id: int
    table_name: str
    def __init__(self, table_id: _Optional[int] = ..., table_name: _Optional[str] = ...) -> None: ...
