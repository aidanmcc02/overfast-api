"""Common utilities for Blizzard HTML/JSON parsing"""

from typing import TYPE_CHECKING
from urllib.parse import quote, unquote

from selectolax.lexbor import LexborHTMLParser, LexborNode

from app.config import settings
from app.domain.exceptions import ParserBlizzardError, ParserParsingError
from app.infrastructure.logger import logger

if TYPE_CHECKING:
    import httpx2

_HTTP_504 = 504


def normalize_blizzard_id(blizzard_id: str) -> str:
    """Return the canonical URL-encoded form of a Blizzard player identifier."""
    return quote(unquote(blizzard_id), safe="")


def build_blizzard_url(path: str, segment: str) -> str:
    """Build a Blizzard request URL with exactly one layer of percent-encoding."""
    return f"{settings.blizzard_host}{path}/{normalize_blizzard_id(segment)}/"


def validate_response_status(
    response: httpx2.Response,
    valid_codes: list[int] | None = None,
) -> None:
    """Validate HTTP response status code.

    Raises:
        ParserBlizzardError: If status code is not in ``valid_codes`` (default: [200])
    """
    if valid_codes is None:
        valid_codes = [200]

    if response.status_code not in valid_codes:
        logger.error(
            "Received an error from Blizzard. HTTP {} : {}",
            response.status_code,
            response.text,
        )
        raise ParserBlizzardError(
            status_code=_HTTP_504,
            message=(
                f"Couldn't get Blizzard page (HTTP {response.status_code} error)"
                f" : {response.text}"
            ),
        )


def parse_html_root(html: str) -> LexborNode:
    """Parse HTML and return the root content node.

    Raises:
        ParserParsingError: If root node not found
    """
    parser = LexborHTMLParser(html)
    root_tag = parser.css_first("div.main-content,main")

    msg = "Could not find main content in HTML"
    if not root_tag:
        raise ParserParsingError(msg)

    return root_tag


def safe_get_text(node: LexborNode | None, default: str = "") -> str:
    """Safely get text from a node, return default if None"""
    return node.text().strip() if node else default


def safe_get_attribute(
    node: LexborNode | None,
    attribute: str,
    default: str = "",
) -> str | None:
    """Safely get attribute from a node, return default if None"""
    if not node or not node.attributes:
        return default
    return node.attributes.get(attribute, default)


def extract_blizzard_id_from_url(url: str) -> str | None:
    """Extract and normalize the Blizzard ID from a career profile URL."""
    if "/career/" not in url:
        return None

    try:
        career_segment = url.split("/career/")[1]
        blizzard_id = career_segment.rstrip("/").split("/")[0]

        if not blizzard_id:
            return None
    except IndexError, ValueError:
        logger.warning("Failed to extract Blizzard ID from URL: {}", url)
        return None

    return normalize_blizzard_id(blizzard_id)


def is_blizzard_id(player_id: str) -> bool:
    """Check if a player_id is a Blizzard ID rather than a BattleTag."""
    return "|" in unquote(player_id) and "-" not in player_id


def match_player_by_blizzard_id(
    search_results: list[dict], blizzard_id: str
) -> dict | None:
    """Match a search result after canonicalizing both identifier sources."""
    normalized_blizzard_id = normalize_blizzard_id(blizzard_id)
    for player in search_results:
        candidate = player.get("url")
        if (
            isinstance(candidate, str)
            and normalize_blizzard_id(candidate) == normalized_blizzard_id
        ):
            return player

    logger.warning(
        "No player found in search results matching Blizzard ID: {}", blizzard_id
    )
    return None
