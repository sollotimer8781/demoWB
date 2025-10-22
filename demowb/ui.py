from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional, Union

import streamlit as st

__all__ = ["inject_css", "card"]

_CSS_SESSION_KEY = "_demowb_css_path"


def inject_css(path: Union[str, Path]) -> bool:
    """Inject custom CSS into the current Streamlit page.

    Parameters
    ----------
    path: str | Path
        Path to the CSS file that should be injected.

    Returns
    -------
    bool
        ``True`` if the CSS was successfully injected, otherwise ``False``.
    """

    try:
        css_path = Path(path).expanduser().resolve(strict=True)
    except FileNotFoundError as exc:
        st.warning(f"Не удалось найти файл стилей: {exc}")
        return False
    except Exception as exc:  # noqa: BLE001 - surface error in UI
        st.warning(f"Ошибка при загрузке стилей: {exc}")
        return False

    try:
        css_content = css_path.read_text(encoding="utf-8")
    except Exception as exc:  # noqa: BLE001 - отобразим пользователю в UI
        st.warning(f"Не удалось прочитать файл стилей: {exc}")
        return False

    if not css_content.strip():
        st.info("Файл стилей пустой — пропускаем инъекцию CSS.")
        return False

    st.markdown(f"<style>{css_content}</style>", unsafe_allow_html=True)
    st.session_state[_CSS_SESSION_KEY] = str(css_path)
    return True


@contextmanager
def card(
    *,
    title: Optional[str] = None,
    subtitle: Optional[str] = None,
    icon: Optional[str] = None,
) -> Iterator[st.delta_generator.DeltaGenerator]:
    """Render a stylised container matching the demoWB theme.

    Usage::

        with card(title="Импорт", icon="⬆") as c:
            c.write("Контент карточки")

    Parameters
    ----------
    title: str | None
        Primary title to render at the top of the card.
    subtitle: str | None
        Optional subtitle displayed below the title.
    icon: str | None
        Emoji or short text that will be shown before the title.
    """

    container = st.container()
    container.markdown("<div class='demowb-card'>", unsafe_allow_html=True)
    body = container.container()

    if title or icon:
        header_parts = []
        if icon:
            header_parts.append(f"<span class='demowb-card__icon'>{icon}</span>")
        if title:
            header_parts.append(f"<span class='demowb-card__title'>{title}</span>")
        header_html = "".join(header_parts)
        body.markdown(f"<div class='demowb-card__header'>{header_html}</div>", unsafe_allow_html=True)

    if subtitle:
        body.markdown(f"<div class='demowb-card__supporting'>{subtitle}</div>", unsafe_allow_html=True)

    try:
        yield body
    finally:
        container.markdown("</div>", unsafe_allow_html=True)
