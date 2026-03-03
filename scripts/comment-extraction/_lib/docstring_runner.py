from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from typing import Iterable, Optional, Tuple


@dataclass(frozen=True)
class DocstringRow:
    file_id: str
    repo: str
    group: str
    release: str
    release_date: str
    directory_id: str
    snapshot_root: str
    path_rel: str

    scope: str  # "module" | "class" | "function" | "async_function"
    name: str # empty for mudule, else class/function name
    qualname: str # outer.inner

    lineno: int
    col: int
    end_lineno: int

    # raw + normalized
    raw_token: Optional[str] # best-effort string literal token (parso), else None (ast)
    docstring: str # decoded docstring content
    docstring_stripped: str # .strip()

    char_len: int
    word_len: int

    # parse backend
    parse_backend: str # "ast" or "parso"
    parse_version: Optional[str] # parso grammar version used, else None (if parso is used, it will always be 3.11)


# AST extraction (Python 3)
class _AstDocVisitor(ast.NodeVisitor):
    """
    Extract true docstrings per Python semantics:
    first statement in module/class/function body is a string literal expression.
    """
    def __init__(self):
        self.stack: list[str] = []
        self.found: list[tuple[str, str, str, int, int, int, str]] = []
        # (scope, name, qualname, lineno, end_lineno, col, docstring)

    def _qualname(self, name: str) -> str:
        if not self.stack:
            return name
        return ".".join(self.stack + [name])

    def _record_if_docstring(self, node: ast.AST, *, scope: str, name: str):
        body = getattr(node, "body", None)
        if not body:
            return

        first = body[0]
        if not isinstance(first, ast.Expr):
            return

        val = first.value
        if not (isinstance(val, ast.Constant) and isinstance(val.value, str)):
            return

        ds = val.value
        lineno = int(getattr(first, "lineno", 1) or 1)
        end_lineno = int(getattr(first, "end_lineno", lineno) or lineno)
        col = int(getattr(first, "col_offset", 0) or 0)

        qual = "" if scope == "module" else self._qualname(name)
        nm = "" if scope == "module" else name

        self.found.append((scope, nm, qual, lineno, end_lineno, col, ds))

    def visit_Module(self, node: ast.Module):
        self._record_if_docstring(node, scope="module", name="")
        self.generic_visit(node)

    def visit_ClassDef(self, node: ast.ClassDef):
        self._record_if_docstring(node, scope="class", name=node.name)
        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef):
        self._record_if_docstring(node, scope="function", name=node.name)
        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
        self._record_if_docstring(node, scope="async_function", name=node.name)
        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()


def _iter_docstrings_ast(source_text: str) -> list[tuple[str, str, str, int, int, int, str]]:
    tree = ast.parse(source_text)
    v = _AstDocVisitor()
    v.visit(tree)
    return v.found


def _safe_literal_to_str(token_text: str) -> str:
    """
    Convert a Python string literal token (e.g. r'''x\\n''') to its value.
    Uses ast.literal_eval (safe for literals). If it fails, returns token_text.
    """
    s = token_text.strip()
    # literal_eval can handle prefixes, but we leave as-is; we only strip whitespace.
    try:
        v = ast.literal_eval(s)
        return v if isinstance(v, str) else token_text
    except Exception:
        return token_text


def _parso_first_stmt_docstring(node):
    children = getattr(node, "children", None)
    if not children:
        return None

    i = 0
    while i < len(children) and getattr(children[i], "type", None) in ("newline", "indent", "dedent"):
        i += 1
    if i >= len(children):
        return None

    first_stmt = children[i]
    if getattr(first_stmt, "type", None) != "simple_stmt":
        return None

    stmt_children = [c for c in (first_stmt.children or []) if getattr(c, "type", None) != "newline"]
    if len(stmt_children) != 1:
        return None

    only = stmt_children[0]

    leaves = []
    def collect_leaves(n):
        ch = getattr(n, "children", None)
        if ch:
            for c in ch:
                collect_leaves(c)
        else:
            leaves.append(n)
    collect_leaves(only)

    token_leaves = [l for l in leaves if isinstance(getattr(l, "type", None), str)]
    token_leaves = [l for l in token_leaves if l.type not in ("newline", "endmarker")]

    if not token_leaves:
        return None

    if any(l.type != "string" for l in token_leaves):
        return None

    raw_parts = [getattr(l, "value", None) for l in token_leaves]
    if any(not isinstance(p, str) for p in raw_parts):
        return None

    decoded_parts = [_safe_literal_to_str(p) for p in raw_parts]
    decoded = "".join(decoded_parts)
    raw = "".join(raw_parts)

    first_leaf = token_leaves[0]
    last_leaf = token_leaves[-1]
    lineno, col = getattr(first_leaf, "start_pos", (1, 0))
    end_pos = getattr(last_leaf, "end_pos", (lineno, col))
    end_lineno = int(end_pos[0]) if isinstance(end_pos, tuple) else int(lineno)

    return int(lineno), int(end_lineno), int(col), decoded, raw


def _parso_walk_defs(module_node):
    """
    Yield (scope, name, qualname, suite_node) for class/func/asyncfunc.
    """
    stack: list[str] = []

    def walk(node):
        ntype = getattr(node, "type", None)

        if ntype == "classdef":
            name_leaf = node.children[1] if len(node.children) > 1 else None
            name = getattr(name_leaf, "value", "") if name_leaf else ""
            qualname = ".".join(stack + [name]) if stack else name
            suite = node.children[-1]  # suite
            yield ("class", name, qualname, suite)

            stack.append(name)
            for ch in getattr(node, "children", []) or []:
                yield from walk(ch)
            stack.pop()
            return

        if ntype == "funcdef":
            name_leaf = node.children[1] if len(node.children) > 1 else None
            name = getattr(name_leaf, "value", "") if name_leaf else ""
            qualname = ".".join(stack + [name]) if stack else name
            suite = node.children[-1]  # suite
            yield ("function", name, qualname, suite)

            stack.append(name)
            for ch in getattr(node, "children", []) or []:
                yield from walk(ch)
            stack.pop()
            return

        # async function: async_stmt -> 'async' funcdef
        if ntype == "async_stmt":
            for ch in getattr(node, "children", []) or []:
                if getattr(ch, "type", None) == "funcdef":
                    name_leaf = ch.children[1] if len(ch.children) > 1 else None
                    name = getattr(name_leaf, "value", "") if name_leaf else ""
                    qualname = ".".join(stack + [name]) if stack else name
                    suite = ch.children[-1]
                    yield ("async_function", name, qualname, suite)

                    stack.append(name)
                    for sub in getattr(ch, "children", []) or []:
                        yield from walk(sub)
                    stack.pop()
                    return

        for ch in getattr(node, "children", []) or []:
            yield from walk(ch)

    yield from walk(module_node)


def _iter_docstrings_parso(source_text: str) -> tuple[list[tuple[str, str, str, int, int, int, str, str]], str]:
    """
    Returns (found, parso_version_used)
    found is list of (scope, name, qualname, lineno, end_lineno, col, docstring, raw_token)
    """
    try:
        import parso
    except ModuleNotFoundError as e:
        raise RuntimeError("parso_not_installed") from e

    versions_to_try = ["3.11"]
    last_err: Optional[Exception] = None
    module = None
    used = None

    for v in versions_to_try:
        try:
            module = parso.parse(source_text, version=v)
            used = v
            break
        except Exception as e:
            last_err = e

    if module is None or used is None:
        print(f"Parso failed to parse with versions {versions_to_try}. Last error: {last_err}")
        raise RuntimeError(f"parso_parse_failed:{type(last_err).__name__ if last_err else 'unknown'}")

    found: list[tuple[str, str, str, int, int, int, str, str]] = []

    # module docstring
    mod = _parso_first_stmt_docstring(module)
    if mod is not None:
        lineno, end_lineno, col, ds, raw = mod
        found.append(("module", "", "", lineno, end_lineno, col, ds, raw))

    # class/function docstrings
    for scope, name, qualname, suite in _parso_walk_defs(module):
        got = _parso_first_stmt_docstring(suite)
        if got is None:
            continue
        lineno, end_lineno, col, ds, raw = got
        found.append((scope, name, qualname, lineno, end_lineno, col, ds, raw))

    return found, used


# Public generator
def iter_docstrings(
    *,
    source_text: str,
    file_id: str,
    repo: str,
    group: str,
    release: str,
    release_date: str,
    directory_id: str,
    snapshot_root: str,
    path_rel: str,
) -> Iterable[DocstringRow]:
    """
    DocstringRow entries for every true docstring in the file (module/class/function),
    using AST first (Python 3 semantics), falling back to Parso if AST fails.

    This is static analysis: it does NOT execute the code being analyzed.
    """
    # Try AST first
    try:
        found = _iter_docstrings_ast(source_text)
        for scope, name, qualname, lineno, end_lineno, col, ds in found:
            ds_stripped = ds.strip()
            char_len = len(ds_stripped)
            word_len = len(ds_stripped.split()) if ds_stripped else 0

            yield DocstringRow(
                file_id=file_id,
                repo=repo,
                group=group,
                release=release,
                release_date=release_date,
                directory_id=directory_id,
                snapshot_root=snapshot_root,
                path_rel=path_rel,
                scope=scope,
                name=name,
                qualname=qualname,
                lineno=int(lineno),
                col=int(col),
                end_lineno=int(end_lineno),
                raw_token=None,
                docstring=ds,
                docstring_stripped=ds_stripped,
                char_len=char_len,
                word_len=word_len,
                parse_backend="ast",
                parse_version=None,
            )
        return
    except Exception:
        # Fallback: Parso
        found2, ver = _iter_docstrings_parso(source_text)
        for scope, name, qualname, lineno, end_lineno, col, ds, raw in found2:
            ds_stripped = ds.strip()
            char_len = len(ds_stripped)
            word_len = len(ds_stripped.split()) if ds_stripped else 0

            yield DocstringRow(
                file_id=file_id,
                repo=repo,
                group=group,
                release=release,
                release_date=release_date,
                directory_id=directory_id,
                snapshot_root=snapshot_root,
                path_rel=path_rel,
                scope=scope,
                name=name,
                qualname=qualname,
                lineno=int(lineno),
                col=int(col),
                end_lineno=int(end_lineno),
                raw_token=raw,
                docstring=ds,
                docstring_stripped=ds_stripped,
                char_len=char_len,
                word_len=word_len,
                parse_backend="parso",
                parse_version=ver,
            )