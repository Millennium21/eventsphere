from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ReservationStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RESERVATION_STATUS_UNSPECIFIED: _ClassVar[ReservationStatus]
    PENDING: _ClassVar[ReservationStatus]
    CONFIRMED: _ClassVar[ReservationStatus]
    RELEASED: _ClassVar[ReservationStatus]
    EXPIRED: _ClassVar[ReservationStatus]
RESERVATION_STATUS_UNSPECIFIED: ReservationStatus
PENDING: ReservationStatus
CONFIRMED: ReservationStatus
RELEASED: ReservationStatus
EXPIRED: ReservationStatus

class InitializeInventoryRequest(_message.Message):
    __slots__ = ("event_id", "total_capacity")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    TOTAL_CAPACITY_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    total_capacity: int
    def __init__(self, event_id: _Optional[str] = ..., total_capacity: _Optional[int] = ...) -> None: ...

class AdjustCapacityRequest(_message.Message):
    __slots__ = ("event_id", "new_total_capacity")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    NEW_TOTAL_CAPACITY_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    new_total_capacity: int
    def __init__(self, event_id: _Optional[str] = ..., new_total_capacity: _Optional[int] = ...) -> None: ...

class ReserveSeatsRequest(_message.Message):
    __slots__ = ("event_id", "order_id", "quantity")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    order_id: str
    quantity: int
    def __init__(self, event_id: _Optional[str] = ..., order_id: _Optional[str] = ..., quantity: _Optional[int] = ...) -> None: ...

class ReservationIdRequest(_message.Message):
    __slots__ = ("reservation_id",)
    RESERVATION_ID_FIELD_NUMBER: _ClassVar[int]
    reservation_id: str
    def __init__(self, reservation_id: _Optional[str] = ...) -> None: ...

class EventIdRequest(_message.Message):
    __slots__ = ("event_id",)
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    def __init__(self, event_id: _Optional[str] = ...) -> None: ...

class ReservationResponse(_message.Message):
    __slots__ = ("success", "message", "reservation_id", "event_id", "order_id", "quantity", "status", "expires_at_unix")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    RESERVATION_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    reservation_id: str
    event_id: str
    order_id: str
    quantity: int
    status: ReservationStatus
    expires_at_unix: int
    def __init__(self, success: _Optional[bool] = ..., message: _Optional[str] = ..., reservation_id: _Optional[str] = ..., event_id: _Optional[str] = ..., order_id: _Optional[str] = ..., quantity: _Optional[int] = ..., status: _Optional[_Union[ReservationStatus, str]] = ..., expires_at_unix: _Optional[int] = ...) -> None: ...

class InventoryResponse(_message.Message):
    __slots__ = ("success", "message", "event_id", "total_capacity", "reserved_count", "confirmed_count", "available", "version")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    TOTAL_CAPACITY_FIELD_NUMBER: _ClassVar[int]
    RESERVED_COUNT_FIELD_NUMBER: _ClassVar[int]
    CONFIRMED_COUNT_FIELD_NUMBER: _ClassVar[int]
    AVAILABLE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    event_id: str
    total_capacity: int
    reserved_count: int
    confirmed_count: int
    available: int
    version: int
    def __init__(self, success: _Optional[bool] = ..., message: _Optional[str] = ..., event_id: _Optional[str] = ..., total_capacity: _Optional[int] = ..., reserved_count: _Optional[int] = ..., confirmed_count: _Optional[int] = ..., available: _Optional[int] = ..., version: _Optional[int] = ...) -> None: ...
