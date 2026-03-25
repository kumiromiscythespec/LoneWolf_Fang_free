# BUILD_ID: 2026-03-25_free_logo_icon_loader_v1
from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtGui import QIcon, QPixmap


BUILD_ID = "2026-03-25_free_logo_icon_loader_v1"

_LOGO_FILENAME = "lonewolf_fang_final_brand_logo.png"
_ICON_FILENAMES = ("lwf_logo.ico", _LOGO_FILENAME)
_FALLBACK_ABSOLUTE = None


@dataclass(frozen=True)
class LogoAsset:
    source_path: Path
    pixmap: QPixmap
    pil_image: object | None = None


def _normalize_path(path: Path) -> str:
    try:
        return str(path.resolve(strict=False)).lower()
    except Exception:
        return str(path.absolute()).lower()


def logo_candidate_paths(repo_root: str | Path) -> list[Path]:
    repo_root_path = Path(repo_root or ".").resolve()
    module_logo = Path(__file__).resolve().parent.parent / "logos" / _LOGO_FILENAME
    script_dir = Path(sys.argv[0]).resolve().parent if sys.argv and sys.argv[0] else None
    exec_dir = Path(sys.executable).resolve().parent if sys.executable else None
    candidates = [
        repo_root_path / "app" / "logos" / _LOGO_FILENAME,
        module_logo,
        (script_dir / "app" / "logos" / _LOGO_FILENAME) if script_dir is not None else None,
        (exec_dir / "app" / "logos" / _LOGO_FILENAME) if exec_dir is not None else None,
        Path.cwd().resolve() / "app" / "logos" / _LOGO_FILENAME,
        _FALLBACK_ABSOLUTE,
    ]
    unique: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate is None:
            continue
        normalized = _normalize_path(candidate)
        if normalized in seen:
            continue
        seen.add(normalized)
        unique.append(candidate)
    return unique


def icon_candidate_paths(repo_root: str | Path) -> list[Path]:
    repo_root_path = Path(repo_root or ".").resolve()
    module_dir = Path(__file__).resolve().parent.parent
    script_dir = Path(sys.argv[0]).resolve().parent if sys.argv and sys.argv[0] else None
    exec_dir = Path(sys.executable).resolve().parent if sys.executable else None
    candidates: list[Path | None] = []
    for filename in _ICON_FILENAMES:
        candidates.extend(
            [
                repo_root_path / "app" / "logos" / filename,
                module_dir / "logos" / filename,
                (script_dir / "app" / "logos" / filename) if script_dir is not None else None,
                (exec_dir / "app" / "logos" / filename) if exec_dir is not None else None,
                Path.cwd().resolve() / "app" / "logos" / filename,
            ]
        )
    unique: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate is None:
            continue
        normalized = _normalize_path(candidate)
        if normalized in seen:
            continue
        seen.add(normalized)
        unique.append(candidate)
    return unique


def _load_qt_pixmap(path: Path) -> QPixmap | None:
    pixmap = QPixmap(str(path))
    if pixmap.isNull():
        return None
    return pixmap


def _load_pil_image(path: Path) -> object | None:
    try:
        from PIL import Image
    except Exception:
        return None

    try:
        with Image.open(path) as base_image:
            rgba = base_image.convert("RGBA")
            rgba.load()
    except Exception:
        return None
    return rgba


def _pixmap_from_pil_image(pil_image: object, *, width_px: int, height_px: int, device_pixel_ratio: float) -> QPixmap | None:
    try:
        from PIL import Image
        from PIL.ImageQt import ImageQt
    except Exception:
        return None

    try:
        resampling = getattr(getattr(Image, "Resampling", Image), "LANCZOS")
        resized = pil_image.resize((int(width_px), int(height_px)), resampling)
        pixmap = QPixmap.fromImage(ImageQt(resized))
        pixmap.setDevicePixelRatio(float(device_pixel_ratio))
    except Exception:
        return None
    if pixmap.isNull():
        return None
    return pixmap


def load_logo_asset(repo_root: str | Path) -> LogoAsset | None:
    for path in logo_candidate_paths(repo_root):
        if not path.is_file():
            continue
        pixmap = _load_qt_pixmap(path)
        pil_image = _load_pil_image(path)
        if pixmap is None and pil_image is not None:
            width_px = max(1, int(getattr(pil_image, "width", 1)))
            height_px = max(1, int(getattr(pil_image, "height", 1)))
            pixmap = _pixmap_from_pil_image(pil_image, width_px=width_px, height_px=height_px, device_pixel_ratio=1.0)
        if pixmap is None:
            continue
        return LogoAsset(source_path=path, pixmap=pixmap, pil_image=pil_image)
    return None


def load_logo_icon(repo_root: str | Path) -> QIcon | None:
    for path in icon_candidate_paths(repo_root):
        if not path.is_file():
            continue
        icon = QIcon(str(path))
        if not icon.isNull():
            return icon
    return None


def render_logo_pixmap(
    asset: LogoAsset | None,
    *,
    box_width: int,
    box_height: int,
    device_pixel_ratio: float = 1.0,
) -> QPixmap | None:
    if asset is None:
        return None
    if box_width <= 0 or box_height <= 0:
        return None

    src_width = max(1, int(asset.pixmap.width()))
    src_height = max(1, int(asset.pixmap.height()))
    scale = min(float(box_width) / float(src_width), float(box_height) / float(src_height))
    target_width = max(1, int(round(src_width * scale)))
    target_height = max(1, int(round(src_height * scale)))
    dpr = max(1.0, float(device_pixel_ratio or 1.0))
    target_width_px = max(1, int(round(target_width * dpr)))
    target_height_px = max(1, int(round(target_height * dpr)))

    if asset.pil_image is not None:
        pil_pixmap = _pixmap_from_pil_image(
            asset.pil_image,
            width_px=target_width_px,
            height_px=target_height_px,
            device_pixel_ratio=dpr,
        )
        if pil_pixmap is not None:
            return pil_pixmap

    scaled = asset.pixmap.scaled(
        target_width_px,
        target_height_px,
        Qt.KeepAspectRatio,
        Qt.SmoothTransformation,
    )
    scaled.setDevicePixelRatio(dpr)
    return scaled
