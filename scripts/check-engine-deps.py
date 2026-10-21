#!/usr/bin/env python3
"""Keep storage engines out of TinyCortex library normal/build dependency graphs."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parent.parent
LIBRARY_KINDS = {
    "lib",
    "rlib",
    "dylib",
    "cdylib",
    "staticlib",
    "proc-macro",
}
ALLOWED_STORAGE_PACKAGES = {
    "tinystoragedrivers-api",
    "tinystoragedrivers-types",
    "tinystoragedrivers-bus",
    "tinystoragedrivers-busproxy",
}
STORAGE_IMPLEMENTATION = re.compile(r"^tinystoragedrivers(?:-|$)")
OTHER_ENGINES = {
    "rusqlite",
    "libsqlite3-sys",
    "mongodb",
    "bson",
    "aws-sdk-s3",
    "lapin",
    "sqlx",
    "redb",
    "sled",
    "rocksdb",
}
FEATURE_MODES = (
    ("default", []),
    ("no-default", ["--no-default-features"]),
    ("all", ["--all-features"]),
)


def package_names(output: str) -> list[str]:
    """Read Cargo's package-only format, tolerating Windows CRLF output."""
    names: list[str] = []
    for raw_line in output.splitlines():
        line = raw_line.strip().lstrip("│├└─ ").strip()
        if line:
            names.append(line.split()[0])
    return names


def forbidden(names: list[str]) -> list[str]:
    """Find concrete engines and TSD implementations while allowing contracts."""
    return sorted(
        {
            name
            for name in names
            if (
                STORAGE_IMPLEMENTATION.match(name)
                and name not in ALLOWED_STORAGE_PACKAGES
            )
            or name in OTHER_ENGINES
        }
    )


def workspace_libraries(metadata: object) -> dict[str, dict]:
    """Select every workspace library by its Cargo package ID; reject omissions."""
    if not isinstance(metadata, dict):
        raise RuntimeError("cargo metadata was not a JSON object")
    packages = metadata.get("packages")
    members = metadata.get("workspace_members")
    if not isinstance(packages, list) or not isinstance(members, list) or not members:
        raise RuntimeError("cargo metadata omitted packages or workspace member IDs")
    by_id: dict[str, dict] = {}
    for package in packages:
        if not isinstance(package, dict) or not isinstance(package.get("id"), str):
            raise RuntimeError("cargo metadata contains a malformed package entry")
        if package["id"] in by_id:
            raise RuntimeError(f"cargo metadata duplicated package ID: {package['id']}")
        by_id[package["id"]] = package

    libraries: dict[str, dict] = {}
    for member_id in members:
        package = by_id.get(member_id)
        if package is None:
            raise RuntimeError(f"workspace member ID was absent from packages: {member_id}")
        targets = package.get("targets")
        if not isinstance(targets, list):
            raise RuntimeError(f"workspace package has malformed targets: {member_id}")
        if any(
            isinstance(target, dict)
            and isinstance(target.get("kind"), list)
            and bool(set(target["kind"]) & LIBRARY_KINDS)
            for target in targets
        ):
            name = package.get("name")
            if not isinstance(name, str) or not name:
                raise RuntimeError(f"workspace package has no name: {member_id}")
            libraries[name] = package
    if not libraries:
        raise RuntimeError("no workspace library targets were found; refusing empty pass")
    return libraries


def checked_run(
    args: list[str], runner: Callable = subprocess.run
) -> subprocess.CompletedProcess:
    result = runner(
        ["cargo", *args], cwd=ROOT, text=True, capture_output=True, check=False
    )
    if result.returncode:
        raise RuntimeError(
            f"cargo {' '.join(args)} failed ({result.returncode}):\n{result.stderr}"
        )
    return result


def cargo_json(args: list[str], runner: Callable = subprocess.run) -> dict:
    try:
        parsed = json.loads(checked_run(args, runner).stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"cargo {' '.join(args)} returned malformed JSON: {error}") from error
    if not isinstance(parsed, dict):
        raise RuntimeError("cargo metadata output was not a JSON object")
    return parsed


def tree_for(
    package_name: str, args: list[str], runner: Callable = subprocess.run
) -> list[str]:
    names = package_names(checked_run(args, runner).stdout)
    if package_name not in names:
        raise RuntimeError(f"{package_name}: cargo tree omitted the requested root package")
    return names


def self_test() -> None:
    assert FEATURE_MODES == (
        ("default", []),
        ("no-default", ["--no-default-features"]),
        ("all", ["--all-features"]),
    )
    parsed = package_names(
        "tinycortex v0.1.3\r\n"
        "├── tinystoragedrivers-api v2.0.2\r\n"
        "└── tinystoragedrivers-types v2.0.2\r\n"
    )
    assert parsed == [
        "tinycortex",
        "tinystoragedrivers-api",
        "tinystoragedrivers-types",
    ], parsed
    assert forbidden(parsed) == []
    assert forbidden(["tinycortex", "tinystoragedrivers-document"]) == [
        "tinystoragedrivers-document"
    ]
    assert forbidden(["tinycortex", "aws-sdk-s3"]) == ["aws-sdk-s3"]
    for engine in OTHER_ENGINES:
        assert forbidden(["tinycortex", engine]) == [engine], engine
    assert forbidden(["tinycortex", "rusqlite-helper", "not-mongodb"]) == []
    assert forbidden(
        ["tinycortex", *sorted(ALLOWED_STORAGE_PACKAGES)]
    ) == []

    synthetic = {
        "packages": [
            {
                "id": f"path+tinycortex#{kind}",
                "name": f"tinycortex-{kind}",
                "targets": [{"kind": [kind]}],
            }
            for kind in sorted(LIBRARY_KINDS)
        ]
        + [
            {"id": "path+tinycortex#bin", "name": "tinycortex-cli", "targets": [{"kind": ["bin"]}]},
            {"id": "registry+outside#lib", "name": "external", "targets": [{"kind": ["lib"]}]},
        ],
        "workspace_members": [f"path+tinycortex#{kind}" for kind in sorted(LIBRARY_KINDS)]
        + ["path+tinycortex#bin"],
    }
    assert set(workspace_libraries(synthetic)) == {
        *(f"tinycortex-{kind}" for kind in LIBRARY_KINDS)
    }
    invalid_metadata = (
        [],
        {},
        {"packages": [], "workspace_members": []},
        {"packages": [], "workspace_members": ["missing"]},
        {
            "packages": [
                {"id": "bin", "name": "cli", "targets": [{"kind": ["bin"]}]}
            ],
            "workspace_members": ["bin"],
        },
    )
    for metadata in invalid_metadata:
        try:
            workspace_libraries(metadata)
        except RuntimeError:
            pass
        else:
            raise AssertionError("malformed/empty metadata was accepted")
    for output in ("", "not json"):
        try:
            json.loads(output)
        except json.JSONDecodeError:
            pass
        else:
            raise AssertionError("malformed cargo metadata JSON was accepted")
    class FakeResult:
        def __init__(self, code: int, stdout: str = "", stderr: str = ""):
            self.returncode, self.stdout, self.stderr = code, stdout, stderr

    def fake_runner(code: int, stdout: str = "", stderr: str = "") -> Callable:
        return lambda *args, **kwargs: FakeResult(code, stdout, stderr)

    try:
        cargo_json(["metadata"], fake_runner(0, "[]"))
    except RuntimeError:
        pass
    else:
        raise AssertionError("non-object cargo metadata was accepted")

    for runner, args in (
        (fake_runner(17, stderr="metadata failed"), ["metadata"]),
        (fake_runner(19, stderr="tree failed"), ["tree"]),
        (fake_runner(0, "other-package v1.0.0\r\n"), ["tree"]),
    ):
        try:
            if args == ["metadata"]:
                cargo_json(args, runner)
            else:
                tree_for("tinycortex", args, runner)
        except RuntimeError:
            pass
        else:
            raise AssertionError("failed or rootless Cargo output was accepted")
    print("engine dependency gate self-tests passed")


def main() -> int:
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        return 0
    if sys.argv[1:]:
        print("usage: check-engine-deps.py [--self-test]", file=sys.stderr)
        return 2

    self_test()
    metadata = cargo_json(["metadata", "--locked", "--format-version", "1", "--no-deps"])
    packages = workspace_libraries(metadata)
    failures: list[str] = []
    for name in sorted(packages):
        for mode, flags in FEATURE_MODES:
            args = [
                "tree",
                "--locked",
                "--format",
                "{p}",
                "--prefix",
                "none",
                "--edges",
                "normal,build",
                "-p",
                name,
                *flags,
            ]
            try:
                names = tree_for(name, args)
            except RuntimeError as error:
                failures.append(f"{name} ({mode}): {error}")
                continue
            blocked = forbidden(names)
            if blocked:
                failures.append(f"{name} ({mode}): forbidden dependencies: {', '.join(blocked)}")
                continue
            print(f"{name} ({mode}): {len(names)} packages, clean")
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print(f"checked {len(packages)} workspace libraries in default, no-default, and all features")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, json.JSONDecodeError) as error:
        print(error, file=sys.stderr)
        raise SystemExit(1)
