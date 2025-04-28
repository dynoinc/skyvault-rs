from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

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
    __slots__ = ()
    def __init__(self) -> None: ...

class GetBatchRequest(_message.Message):
    __slots__ = ("tables",)
    TABLES_FIELD_NUMBER: _ClassVar[int]
    tables: _containers.RepeatedCompositeFieldContainer[TableReadBatchRequest]
    def __init__(self, tables: _Optional[_Iterable[_Union[TableReadBatchRequest, _Mapping]]] = ...) -> None: ...

class TableReadBatchRequest(_message.Message):
    __slots__ = ("table_name", "keys")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    keys: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, table_name: _Optional[str] = ..., keys: _Optional[_Iterable[str]] = ...) -> None: ...

class GetBatchResponse(_message.Message):
    __slots__ = ("tables",)
    TABLES_FIELD_NUMBER: _ClassVar[int]
    tables: _containers.RepeatedCompositeFieldContainer[TableReadBatchResponse]
    def __init__(self, tables: _Optional[_Iterable[_Union[TableReadBatchResponse, _Mapping]]] = ...) -> None: ...

class TableReadBatchResponse(_message.Message):
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

class GetFromWALRequest(_message.Message):
    __slots__ = ("tables",)
    TABLES_FIELD_NUMBER: _ClassVar[int]
    tables: _containers.RepeatedCompositeFieldContainer[TableReadBatchRequest]
    def __init__(self, tables: _Optional[_Iterable[_Union[TableReadBatchRequest, _Mapping]]] = ...) -> None: ...

class GetFromWALResponse(_message.Message):
    __slots__ = ("tables",)
    TABLES_FIELD_NUMBER: _ClassVar[int]
    tables: _containers.RepeatedCompositeFieldContainer[TableGetFromWALResponse]
    def __init__(self, tables: _Optional[_Iterable[_Union[TableGetFromWALResponse, _Mapping]]] = ...) -> None: ...

class TableGetFromWALResponse(_message.Message):
    __slots__ = ("table_name", "items")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    items: _containers.RepeatedCompositeFieldContainer[GetFromWALItem]
    def __init__(self, table_name: _Optional[str] = ..., items: _Optional[_Iterable[_Union[GetFromWALItem, _Mapping]]] = ...) -> None: ...

class GetFromWALItem(_message.Message):
    __slots__ = ("key", "value", "deleted")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    DELETED_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: bytes
    deleted: _empty_pb2.Empty
    def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ..., deleted: _Optional[_Union[_empty_pb2.Empty, _Mapping]] = ...) -> None: ...

class KickOffWALCompactionRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class KickOffWALCompactionResponse(_message.Message):
    __slots__ = ("job_id",)
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    job_id: int
    def __init__(self, job_id: _Optional[int] = ...) -> None: ...

class GetJobStatusRequest(_message.Message):
    __slots__ = ("job_id",)
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    job_id: int
    def __init__(self, job_id: _Optional[int] = ...) -> None: ...

class GetJobStatusResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: str
    def __init__(self, status: _Optional[str] = ...) -> None: ...

class ListRunsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListRunsResponse(_message.Message):
    __slots__ = ("runs",)
    RUNS_FIELD_NUMBER: _ClassVar[int]
    runs: _containers.RepeatedCompositeFieldContainer[RunMetadata]
    def __init__(self, runs: _Optional[_Iterable[_Union[RunMetadata, _Mapping]]] = ...) -> None: ...

class DumpChangelogRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class DumpChangelogResponse(_message.Message):
    __slots__ = ("entries",)
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[ChangelogEntry]
    def __init__(self, entries: _Optional[_Iterable[_Union[ChangelogEntry, _Mapping]]] = ...) -> None: ...

class ChangelogEntry(_message.Message):
    __slots__ = ("v1",)
    V1_FIELD_NUMBER: _ClassVar[int]
    v1: ChangelogEntryV1
    def __init__(self, v1: _Optional[_Union[ChangelogEntryV1, _Mapping]] = ...) -> None: ...

class ChangelogEntryV1(_message.Message):
    __slots__ = ("runs_added", "runs_removed")
    RUNS_ADDED_FIELD_NUMBER: _ClassVar[int]
    RUNS_REMOVED_FIELD_NUMBER: _ClassVar[int]
    runs_added: _containers.RepeatedScalarFieldContainer[str]
    runs_removed: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, runs_added: _Optional[_Iterable[str]] = ..., runs_removed: _Optional[_Iterable[str]] = ...) -> None: ...

class RunMetadata(_message.Message):
    __slots__ = ("id", "wal_seqno", "table_name", "stats_v1")
    ID_FIELD_NUMBER: _ClassVar[int]
    WAL_SEQNO_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    STATS_V1_FIELD_NUMBER: _ClassVar[int]
    id: str
    wal_seqno: int
    table_name: str
    stats_v1: StatsV1
    def __init__(self, id: _Optional[str] = ..., wal_seqno: _Optional[int] = ..., table_name: _Optional[str] = ..., stats_v1: _Optional[_Union[StatsV1, _Mapping]] = ...) -> None: ...

class StatsV1(_message.Message):
    __slots__ = ("min_key", "max_key", "size_bytes", "item_count")
    MIN_KEY_FIELD_NUMBER: _ClassVar[int]
    MAX_KEY_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    ITEM_COUNT_FIELD_NUMBER: _ClassVar[int]
    min_key: str
    max_key: str
    size_bytes: int
    item_count: int
    def __init__(self, min_key: _Optional[str] = ..., max_key: _Optional[str] = ..., size_bytes: _Optional[int] = ..., item_count: _Optional[int] = ...) -> None: ...
