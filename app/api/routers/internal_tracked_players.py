"""Authenticated service endpoint for active player tracking."""

import secrets
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.api.dependencies import StorageDep
from app.config import settings
from app.infrastructure.logger import logger

_bearer = HTTPBearer(auto_error=False)


async def require_tracking_api_key(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> None:
    """Require the shared tracking token, failing closed when unconfigured."""
    configured_key = settings.tracked_player_api_key
    if (
        not configured_key
        or credentials is None
        or credentials.scheme.lower() != "bearer"
        or not secrets.compare_digest(credentials.credentials, configured_key)
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid tracking credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


router = APIRouter(
    prefix="/internal/tracked-players",
    include_in_schema=False,
    dependencies=[Depends(require_tracking_api_key)],
)


@router.post("/{player_id}", status_code=status.HTTP_204_NO_CONTENT)
async def track_player(
    storage: StorageDep,
    player_id: Annotated[str, Path(min_length=1, max_length=255)],
) -> None:
    """Register or renew one trusted service's active player."""
    await storage.track_player(player_id, settings.tracked_player_tracking_ttl)
    logger.info(
        "[TrackedPlayers] registration player_id={} ttl_seconds={}",
        player_id,
        settings.tracked_player_tracking_ttl,
    )
