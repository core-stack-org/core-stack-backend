# Framework For Native Windows Runtime

Snapshot date: 2026-03-19

## Purpose

This document is a proposal to the CoRE Stack team for sustaining a native Windows runtime alongside Linux.

The goal is not "Windows at any cost". The goal is:

1. keep Linux smooth and stable
2. make native Windows a supported contributor and operator path
3. reduce friction for institutions, students, implementers, and civic-tech collaborators who work primarily on Windows
4. do this through disciplined engineering boundaries rather than scattered one-off fixes

For a Public Good product, lowering setup friction expands adoption, broadens contributor diversity, and makes cobuilding easier. Native Windows support helps that, but only if we keep the codebase intentional and regression-resistant.

## Executive Summary

The repo is already mostly portable at the Python/Django layer. The main barriers to native Windows are not business logic; they are operational assumptions:

- Linux-only installation automation
- Unix-style absolute paths and path concatenation
- direct assumptions about shell, services, and permissions
- reliance on external tools without a clear cross-OS contract

The good news is that the risk is concentrated. We do not need to rewrite the application to support Windows. We need a framework that keeps platform-specific behavior centralized and reviewable.

## What We Observed In This Codebase

### Repo shape

A quick tracked-file scan shows:

- 365 tracked Python files
- 19 tracked Markdown files

This matters because Windows compatibility has to be maintained through conventions, not by manually remembering edge cases across hundreds of modules.

### Where the portability risk actually lives

A quick repo scan found:

- 128 Linux service or package-management references in installation docs/scripts
- 55 Unix-style absolute path references across the repo
- 87 references to external binaries or OS-level tools such as `ffmpeg`, `gdal_translate`, `ogr2ogr`, Firefox, or geckodriver

These counts should not be read as "128 bugs" or "55 runtime blockers". They show the direction of the codebase:

- installation and operations are still heavily Linux-shaped
- filesystem and subprocess boundaries are the highest-risk places for cross-OS breakage
- core Django business logic is generally less risky than bootstrap, storage, and system integration code

### Qualitative patterns observed

1. The core web application is relatively portable.

- Django, DRF, model logic, serializers, and most API code are not inherently Linux-bound.
- The portability problems cluster around runtime bootstrap, file paths, installers, and external binaries.

2. Installation and operations were historically Linux-first.

- Existing installer and troubleshooting flows assume `bash`, `apt`, `systemctl`, Apache, and Linux service conventions.
- This is appropriate for Linux, but it cannot be the only supported operational path if native Windows matters.

3. Filesystem handling was a meaningful source of risk.

- Some modules used string concatenation for paths.
- Some flows assumed trailing slashes.
- Some legacy code used `split("/")` patterns that are safe for URLs or asset IDs but risky for local filesystem paths.

4. External tools are a real cross-OS surface.

- Firefox/geckodriver for PDF rendering
- FFmpeg for audio conversion
- GDAL/OGR CLI tools for raster and shapefile flows

These are valid dependencies, but they need explicit discovery and documented expectations.

5. A small tail of standalone utility scripts still contains developer-local paths.

These scripts are important because they create drag on future maintenance even if they are not on the main request path today.

## What Has Already Been Done In This Branch

The current branch already introduces the start of a safer framework:

- centralized runtime bootstrap in `nrm_app/runtime.py`
- path normalization and blank-safe defaults in `nrm_app/settings.py`
- Windows-aware entrypoint bootstrap in `manage.py`, `wsgi.py`, `asgi.py`, and `celery.py`
- Windows bootstrap scripts in `installation/bootstrap_env.py` and `installation/install_windows.ps1`
- a native Windows setup guide in `installation/WINDOWS.md`
- targeted cleanup of path-sensitive runtime flows in plans, bot interface, computing helpers, DPR PDF rendering, and GEE utilities

This is a strong base. The next step is to formalize the maintenance rules so we do not regress.

## Proposal: Cross-OS Runtime Framework

### Principle 1: Linux remains the stability baseline

Linux should remain the reference runtime for production stability and performance-sensitive workloads.

Native Windows support should be additive, not disruptive:

- no Linux regression is acceptable in the name of Windows support
- no Windows-specific shortcut should silently weaken Linux behavior
- no change should move platform-specific logic into general business logic unless unavoidable

### Principle 2: Platform-specific behavior must be centralized

If code needs to know whether it is on Windows or Linux, that logic should live in one of these places:

- runtime bootstrap helpers
- installation scripts
- subprocess discovery helpers
- documented adapter modules

It should not be spread through arbitrary feature modules unless the feature is inherently OS-dependent.

### Principle 3: Filesystem behavior must be expressed through path APIs

Path handling must always go through `pathlib`, `os.path`, or a shared helper.

Never:

- concatenate local filesystem paths with `"/"` or `"\\"`
- rely on trailing slashes
- assume `/tmp`, `/var`, `/home`, or `/usr`
- parse local file paths with `split("/")`

Acceptable:

- `Path(...) / "child"`
- `os.path.join(...)`
- helper functions that normalize repo-relative storage paths

### Principle 4: External binaries must have explicit contracts

For every external tool we depend on, we should define:

- how it is discovered
- whether `PATH` is enough
- which override env var is supported
- whether it is required or optional
- what graceful fallback looks like

Examples:

- `FIREFOX_BIN`
- `GECKODRIVER_PATH`
- `FFMPEG_BIN`
- GDAL/PROJ discovery through Conda/runtime bootstrap

### Principle 5: Installers should be parallel, not forced into one script

We should not try to make one shell script pretend to be universal.

Preferred model:

- Linux installer stays Linux-native
- Windows installer stays PowerShell-native
- shared bootstrap logic lives in Python where practical
- docs make the difference explicit instead of hiding it

### Principle 6: Degrade gracefully where Windows cannot mirror Linux exactly

Some behaviors differ by platform and should be documented rather than papered over.

Examples:

- Celery worker pool differences on Windows
- service management differences
- permission model differences for `chmod`/`chown`
- production serving differences between Apache/Linux and Windows-friendly alternatives

The rule is: be explicit, safe, and predictable.

## Engineering Guidelines To Keep Cross-OS Support Healthy

### Coding guidelines

1. Do not hardcode absolute machine-specific paths.

Bad:

```python
folder_path = "/home/ubuntu/project/data"
```

Good:

```python
folder_path = Path(settings.BASE_DIR) / "data"
```

2. Treat blank env values as a real case.

- Generated `.env` files often contain blank values.
- Code should use blank-safe defaults for path-like and OS-like settings where sensible.

3. Keep path creation and directory creation idempotent.

- `mkdir(parents=True, exist_ok=True)`
- best-effort log/temp/media directory creation

4. Wrap Unix-only permission behavior.

- `os.chmod(...)` should be best-effort where platform support differs
- avoid assuming `chown` exists outside Linux install scripts

5. Separate local filesystem paths from URL parsing.

- `split("/")` is acceptable for MIME types, URLs, or asset IDs
- it is not acceptable for local file path manipulation

6. Prefer helper functions for recurring path semantics.

Examples:

- runtime path resolution
- shapefile upload base resolution
- temp directory conventions
- binary discovery

### Review guidelines

Every PR that touches runtime, file handling, or installation should answer:

1. Does this assume Linux-only filesystem layout?
2. Does this assume `bash`, `sudo`, `systemctl`, or Apache in app logic rather than installer/docs?
3. Does this rely on a local binary without documenting discovery and fallback?
4. Does this introduce a Windows codepath outside the shared platform boundary?
5. Does this preserve Linux behavior exactly where Linux is already stable?

### Documentation guidelines

When adding or changing setup docs:

- document Linux and Windows separately when the flows are materially different
- keep common concepts shared, but keep commands platform-native
- avoid pretending the same command works everywhere if it does not
- keep contributor-facing docs honest about optional vs required dependencies

### Testing and verification guidelines

At minimum, changes in the cross-OS surface should be checked through:

- Python syntax/compile validation
- Linux install path sanity
- Windows bootstrap sanity
- one smoke test for app startup
- one smoke test for Celery worker startup

Target future CI matrix:

- Linux: app bootstrap, migrations, internal initialization test
- Windows: env bootstrap, migrations, startup smoke checks, selected runtime tests

## Suggested Adoption Model

### Phase 1: Stabilize the framework

- keep the new runtime/bootstrap helpers as the only place where OS detection lives
- finish documentation of known caveats
- remove the remaining developer-local paths from utility scripts

### Phase 2: Add guardrails

- add lint/review checks for hardcoded absolute paths
- add a small utility test suite around path resolution and runtime bootstrap
- add a Windows CI smoke job

### Phase 3: Expand confidence

- test the highest-traffic feature paths on native Windows
- document any remaining unsupported production scenarios clearly
- gather setup feedback from non-Linux contributors

## Success Metrics

We should consider this proposal successful if we can show:

- new contributors can bootstrap on Windows without WSL
- Linux install/run remains unaffected
- the number of hardcoded developer-local paths trends down over time
- Windows-related bugs are mostly localized to known boundary areas, not random app logic
- setup time and support burden for Windows users drops materially

## Risks If We Do This Poorly

- Linux regressions from over-generalized platform abstractions
- hidden complexity spread through unrelated feature modules
- docs that claim support without real verification
- a slow accumulation of "small exceptions" that make both platforms fragile

That is why the framework matters. Native Windows support is valuable, but unmanaged portability debt is expensive.

## Recommended Team Position

The team should explicitly support native Windows as a contributor/runtime path for CoRE Stack, while keeping Linux as the operational gold standard.

The right posture is:

- yes to native Windows support
- yes to careful, centralized platform boundaries
- yes to explicit operational differences
- no to scattering OS logic across the application
- no to weakening Linux smoothness in the name of broad compatibility

If we follow that posture, native Windows runtime can expand adoption and cobuilding without turning platform support into a maintenance trap.
