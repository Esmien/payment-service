"""Проверка статического API-ключа в заголовке X-API-Key для защищенных эндпоинтов."""

import secrets
from typing import Annotated, Final

from fastapi import Depends, Security
from fastapi.security import APIKeyHeader
from pydantic import SecretStr

from core.config import Settings, get_settings
from core.exceptions import UnauthorizedError

API_KEY_HEADER_NAME: Final[str] = "X-API-Key"
# auto_error=False: отсутствие заголовка обрабатываем сами и отдаем доменный UnauthorizedError (401).
api_key_header_scheme = APIKeyHeader(name=API_KEY_HEADER_NAME, auto_error=False)

ApiKeyHeaderDep = Annotated[str | None, Security(api_key_header_scheme)]
SettingsDep = Annotated[Settings, Depends(get_settings)]


def _extract_api_key(*, settings: Settings) -> str:
    """Возвращает эталонный API-ключ в виде обычной строки.

    Args:
        settings: Настройки приложения.

    Returns:
        Значение ключа без обёртки SecretStr, если поле объявлено как SecretStr.
    """
    api_key = settings.api_key

    # SecretStr скрывает значение в repr и логах, поэтому для сравнения его нужно распаковать.
    if isinstance(api_key, SecretStr):
        return api_key.get_secret_value()

    return str(api_key)


async def verify_api_key(
    *,
    provided_api_key: ApiKeyHeaderDep,
    settings: SettingsDep,
) -> None:
    """Проверяет, что заголовок X-API-Key совпадает с настроенным API-ключом.

    Args:
        provided_api_key: Значение заголовка X-API-Key или None, если заголовок отсутствует.
        settings: Настройки приложения, из которых берется эталонный ключ.

    Raises:
        UnauthorizedError: Если ключ отсутствует или не совпадает с эталонным.
    """
    if provided_api_key is None:
        raise UnauthorizedError()

    expected_api_key = _extract_api_key(settings=settings)

    # secrets.compare_digest для str бросает TypeError на не-ASCII, поэтому сравниваем байты.
    # Сравнение за постоянное время защищает от атак по времени.
    if not secrets.compare_digest(provided_api_key.encode("utf-8"), expected_api_key.encode("utf-8")):
        raise UnauthorizedError()
