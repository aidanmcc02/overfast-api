"""Tests for authenticated tracked-player registration and expiry."""

from unittest.mock import patch

import pytest
from fastapi import status


class TestTrackedPlayerRegistration:
    def test_requires_shared_bearer_token(self, client):
        with patch(
            "app.api.routers.internal_tracked_players.settings.tracked_player_api_key",
            "service-secret",
        ):
            response = client.post("/internal/tracked-players/Player-1234")

        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_rejects_wrong_bearer_token(self, client):
        with patch(
            "app.api.routers.internal_tracked_players.settings.tracked_player_api_key",
            "service-secret",
        ):
            response = client.post(
                "/internal/tracked-players/Player-1234",
                headers={"Authorization": "Bearer wrong-secret"},
            )

        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_registers_and_renews_player(self, client, storage_db):
        with (
            patch(
                "app.api.routers.internal_tracked_players.settings.tracked_player_api_key",
                "service-secret",
            ),
            patch(
                "app.api.routers.internal_tracked_players.settings.tracked_player_tracking_ttl",
                300,
            ),
        ):
            first = client.post(
                "/internal/tracked-players/Player-1234",
                headers={"Authorization": "Bearer service-secret"},
            )
            first_expiry = storage_db._tracked_players["Player-1234"]
            second = client.post(
                "/internal/tracked-players/Player-1234",
                headers={"Authorization": "Bearer service-secret"},
            )

        assert first.status_code == status.HTTP_204_NO_CONTENT
        assert second.status_code == status.HTTP_204_NO_CONTENT
        assert storage_db._tracked_players["Player-1234"] >= first_expiry


class TestTrackedPlayerExpiration:
    @pytest.mark.asyncio
    async def test_expired_registration_is_removed(self, storage_db):
        with patch("tests.fake_storage.time.time", return_value=1000):
            await storage_db.track_player("expired-player", 120)

        with patch("tests.fake_storage.time.time", return_value=1121):
            result = await storage_db.get_tracked_player_ids()

        assert result == []
        assert storage_db._tracked_players == {}

    @pytest.mark.asyncio
    async def test_renewal_extends_expiration(self, storage_db):
        with patch("tests.fake_storage.time.time", return_value=1000):
            await storage_db.track_player("renewed-player", 120)
        with patch("tests.fake_storage.time.time", return_value=1100):
            await storage_db.track_player("renewed-player", 120)

        with patch("tests.fake_storage.time.time", return_value=1150):
            result = await storage_db.get_tracked_player_ids()

        assert result == ["renewed-player"]
