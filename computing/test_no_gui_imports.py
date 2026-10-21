"""Guards against GUI toolkit imports in the computing app.

Servers and slim containers usually run a Python built without Tk, so any
module-level ``tkinter`` import reachable from the URL configuration stops
Django from starting with ``ModuleNotFoundError: No module named '_tkinter'``.
"""

import ast
import os
import subprocess
import sys
from pathlib import Path

from django.conf import settings
from django.test import SimpleTestCase

COMPUTING_DIR = Path(__file__).resolve().parent

# Simulates a Python built without Tk: Python raises ImportError for any
# module whose sys.modules entry is None.
URLCONF_WITHOUT_TK = """
import sys
sys.modules["_tkinter"] = None
import django
django.setup()
import computing.urls
"""


def tkinter_imports(source):
    """Return the line numbers of every ``tkinter`` import in ``source``.

    Args:
        source: Python source code to scan.

    Returns:
        A list of line numbers, empty when nothing imports ``tkinter``.
    """
    lines = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            modules = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            modules = [node.module]
        else:
            continue
        if any(module.split(".")[0] == "tkinter" for module in modules):
            lines.append(node.lineno)
    return lines


class UrlconfWithoutTkTests(SimpleTestCase):
    def test_computing_urls_import_without_tk(self):
        env = {
            **os.environ,
            "DJANGO_SETTINGS_MODULE": os.environ.get(
                "DJANGO_SETTINGS_MODULE", "nrm_app.settings"
            ),
        }
        result = subprocess.run(
            [sys.executable, "-c", URLCONF_WITHOUT_TK],
            cwd=settings.BASE_DIR,
            env=env,
            capture_output=True,
            text=True,
            timeout=300,
        )
        self.assertEqual(result.returncode, 0, result.stderr[-2000:])


class NoTkinterImportTests(SimpleTestCase):
    def test_scanner_detects_tkinter_imports(self):
        source = "import os\nimport tkinter\nfrom tkinter.font import names\n"
        self.assertEqual(tkinter_imports(source), [2, 3])

    def test_scanner_ignores_similar_names(self):
        source = "import tkinter_helpers\nfrom .tkinter import x\n"
        self.assertEqual(tkinter_imports(source), [])

    def test_no_computing_module_imports_tkinter(self):
        offenders = []
        for path in sorted(COMPUTING_DIR.rglob("*.py")):
            try:
                lines = tkinter_imports(path.read_text(encoding="utf-8"))
            except (SyntaxError, UnicodeDecodeError):
                continue
            offenders += [f"{path.relative_to(COMPUTING_DIR)}:{n}" for n in lines]
        self.assertEqual(offenders, [], "Servers run without Tk; remove these imports")
