"""Kiểm tra cú pháp Python mà không import hoặc thực thi mã dự án."""

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
EXCLUDED = {".git", ".venv", "venv", "node_modules", "__pycache__", ".tox"}


def main() -> int:
    paths = sorted(
        path for path in ROOT.rglob("*.py")
        if not EXCLUDED.intersection(path.relative_to(ROOT).parts)
    )
    failures = []
    for path in paths:
        try:
            # compile tạo code object nhưng KHÔNG thực thi, không ghi pyc.
            compile(path.read_bytes(), str(path), "exec", dont_inherit=True)
        except (SyntaxError, UnicodeError) as exc:
            failures.append((path.relative_to(ROOT), exc))
    for path, exc in failures:
        # Chỉ tên file/vị trí/lỗi, không in dòng source có thể chứa secrets.
        print(f"FAIL {path}:{getattr(exc, 'lineno', '?')}: {getattr(exc, 'msg', type(exc).__name__)}")
    print(f"Python syntax: {len(paths) - len(failures)}/{len(paths)} PASS; {len(failures)} FAIL")
    return int(bool(failures))


if __name__ == "__main__":
    sys.exit(main())
