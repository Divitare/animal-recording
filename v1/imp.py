from __future__ import annotations

import importlib
import importlib.machinery
import importlib.util
import io
import sys
import types
from pathlib import Path

SEARCH_ERROR = 0
PY_SOURCE = 1
PY_COMPILED = 2
C_EXTENSION = 3
PKG_DIRECTORY = 5
C_BUILTIN = 6
PY_FROZEN = 7


def new_module(name: str) -> types.ModuleType:
    return types.ModuleType(name)


def reload(module):
    return importlib.reload(module)


def cache_from_source(path: str, debug_override=None) -> str:
    if debug_override is None:
        return importlib.util.cache_from_source(path)
    optimization = "" if bool(debug_override) else 1
    return importlib.util.cache_from_source(path, optimization=optimization)


def source_from_cache(path: str) -> str:
    return importlib.util.source_from_cache(path)


def get_magic():
    return importlib.util.MAGIC_NUMBER


def get_tag():
    return sys.implementation.cache_tag


def get_suffixes():
    suffixes = []
    suffixes.extend((suffix, "r", PY_SOURCE) for suffix in importlib.machinery.SOURCE_SUFFIXES)
    suffixes.extend((suffix, "rb", PY_COMPILED) for suffix in importlib.machinery.BYTECODE_SUFFIXES)
    suffixes.extend((suffix, "rb", C_EXTENSION) for suffix in importlib.machinery.EXTENSION_SUFFIXES)
    return suffixes


class NullImporter:
    def __init__(self, path):
        if path == "":
            raise ImportError("empty pathname")
        self.path = path

    def find_module(self, fullname):
        return None


def _description_from_spec(spec):
    if spec.submodule_search_locations is not None:
        return ("", "", PKG_DIRECTORY)
    origin = spec.origin or ""
    suffix = Path(origin).suffix
    if suffix in importlib.machinery.SOURCE_SUFFIXES:
        return (suffix, "r", PY_SOURCE)
    if suffix in importlib.machinery.BYTECODE_SUFFIXES:
        return (suffix, "rb", PY_COMPILED)
    if suffix in importlib.machinery.EXTENSION_SUFFIXES:
        return (suffix, "rb", C_EXTENSION)
    return ("", "", SEARCH_ERROR)


def find_module(name: str, path=None):
    search_path = path
    spec = importlib.machinery.PathFinder.find_spec(name, search_path)
    if spec is None:
        raise ImportError(f"No module named {name!r}")

    description = _description_from_spec(spec)
    pathname = spec.origin or ""

    if spec.submodule_search_locations is not None:
        return (None, pathname, description)

    loader = spec.loader
    source = None
    if loader is not None and hasattr(loader, "get_source"):
        try:
            source = loader.get_source(name)
        except Exception:
            source = None

    if source is not None:
        return (io.StringIO(source), pathname, description)

    file_handle = None
    if pathname:
        mode = description[1] or "rb"
        try:
            file_handle = open(pathname, mode)
        except OSError:
            file_handle = None
    return (file_handle, pathname, description)


def load_source(name: str, pathname: str, file=None):
    spec = importlib.util.spec_from_file_location(name, pathname)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load source module {name!r} from {pathname!r}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def load_module(name, file, pathname, description):
    if description[2] == PKG_DIRECTORY:
        spec = importlib.machinery.PathFinder.find_spec(name, [pathname])
    else:
        spec = importlib.util.spec_from_file_location(name, pathname)

    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module {name!r} from {pathname!r}")

    module = sys.modules.get(name)
    if module is None:
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module

    try:
        spec.loader.exec_module(module)
    finally:
        if file is not None and hasattr(file, "close"):
            try:
                file.close()
            except OSError:
                pass
    return module
