from __future__ import annotations

from sqlalchemy import Boolean, String
from sqlalchemy import Enum as SqlEnum
from sqlalchemy.orm import Mapped, mapped_column

from services.shared.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from services.shared.enums import UserRole

SCHEMA = "api"


class User(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    __tablename__ = "users"
    __table_args__ = {"schema": SCHEMA}
    # Without this, `updated_at`'s onupdate=func.now() value is only
    # known server-side after an UPDATE; SQLAlchemy marks it expired and
    # a later *sync* attribute read (e.g. Pydantic's model_validate)
    # can't lazily reload it in an async session, raising MissingGreenlet.
    # eager_defaults="auto" (the default) doesn't reliably cover this for
    # UPDATEs, so it's forced on explicitly.
    __mapper_args__ = {"eager_defaults": True}

    email: Mapped[str] = mapped_column(String(255), unique=True, index=True, nullable=False)
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[UserRole] = mapped_column(
        SqlEnum(
            UserRole,
            name="user_role",
            native_enum=False,
            length=20,
            validate_strings=True,
            values_callable=lambda enum_cls: [member.value for member in enum_cls],
        ),
        nullable=False,
        default=UserRole.ATTENDEE,
    )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
