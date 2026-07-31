import uuid

import pytest

from services.api.app.core.security import (
    create_access_token,
    create_refresh_token,
    decode_token,
    hash_password,
    verify_password,
)
from services.shared.errors import AuthenticationError


def test_hash_password_is_not_plaintext_and_verifies_correctly():
    hashed = hash_password("correct-horse-battery-staple")
    assert hashed != "correct-horse-battery-staple"
    assert verify_password("correct-horse-battery-staple", hashed)
    assert not verify_password("wrong-password", hashed)


def test_access_token_round_trips_and_carries_the_right_type():
    user_id = uuid.uuid4()
    token = create_access_token(user_id)
    payload = decode_token(token, expected_type="access")
    assert payload["sub"] == str(user_id)
    assert payload["type"] == "access"


def test_refresh_token_rejected_when_decoded_as_access():
    user_id = uuid.uuid4()
    refresh_token = create_refresh_token(user_id)
    with pytest.raises(AuthenticationError):
        decode_token(refresh_token, expected_type="access")


def test_garbage_token_raises_authentication_error():
    with pytest.raises(AuthenticationError):
        decode_token("not-a-real-token", expected_type="access")
