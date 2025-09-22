import base64
import os
import tempfile
import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.print_page_options import PrintOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.firefox import GeckoDriverManager

from utilities.logger import setup_logger

logger = setup_logger(__name__)

FIREFOX_BIN = os.environ.get("FIREFOX_BIN")  # optional explicit path
GECKODRIVER_LOG = os.environ.get("GECKODRIVER_LOG", "geckodriver.log")


def render_pdf_with_firefox(
    url: str,
    *,
    page_load_timeout: int = 120,
    ready_timeout: int = 180,
    viewport_width: int = 1600,
    viewport_height: int = 1200,
    print_landscape: bool = True,
) -> bytes:

    opts = FirefoxOptions()
    opts.add_argument("-headless")
    opts.add_argument("--no-remote")  # isolate profiles on shared hosts

    # ---- choose binary (prefer explicit env, then ESR, then regular) ----
    firefox_bin = os.environ.get("FIREFOX_BIN")
    if firefox_bin and os.path.exists(firefox_bin):
        chosen = firefox_bin
    elif os.path.exists("/usr/bin/firefox-esr"):
        chosen = "/usr/bin/firefox-esr"
    elif os.path.exists("/usr/bin/firefox"):
        chosen = "/usr/bin/firefox"
    elif os.path.exists("/usr/local/bin/firefox"):
        chosen = "/usr/local/bin/firefox"
    else:
        raise RuntimeError(
            "No Firefox binary found. Set FIREFOX_BIN or install firefox/firefox-esr."
        )
    opts.binary_location = chosen
    logger.info("PDF: using Firefox binary at %s", chosen)

    # ---- geckodriver path (cache or baked) ----
    geckopath = os.environ.get("GECKODRIVER_PATH") or GeckoDriverManager().install()

    # ---- logs ----
    try:
        # prefer project logs dir if available
        base_dir = os.path.dirname(os.path.dirname(__file__))
    except Exception:
        base_dir = os.getcwd()
    logs_dir = os.path.join(base_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    log_path = os.environ.get("GECKODRIVER_LOG", os.path.join(logs_dir, "geckodriver.log"))
    log_file = open(log_path, "a", buffering=1, encoding="utf-8", errors="replace")
    logger.info("PDF: using geckodriver at %s (logs -> %s)", geckopath, log_path)

    service = FirefoxService(executable_path=geckopath, log_output=log_file)

    # ---- sanitize environment to avoid Conda/GTK/NSS conflicts ----
    for var in (
        "LD_LIBRARY_PATH",
        "DYLD_LIBRARY_PATH",
        "GTK_PATH",
        "GTK_DATA_PREFIX",
        "MOZ_GMP_PATH",
    ):
        if os.environ.get(var):
            logger.warning("PDF: unsetting %s to avoid binary/lib conflicts", var)
            os.environ.pop(var, None)
    os.environ.setdefault("XDG_RUNTIME_DIR", "/tmp")

    # ---- optionally force a fresh, writable profile to /tmp ----
    profile_dir = tempfile.mkdtemp(prefix="ffprof_")
    opts.add_argument("-profile")
    opts.add_argument(profile_dir)
    logger.info("PDF: using temp Firefox profile at %s", profile_dir)

    driver = webdriver.Firefox(options=opts, service=service)

    try:
        # a larger viewport helps ensure tiles/charts load at print scale
        try:
            driver.set_window_rect(width=viewport_width, height=viewport_height)
        except Exception:
            pass

        driver.set_page_load_timeout(page_load_timeout)
        driver.get(url)

        # 1) DOM ready
        WebDriverWait(driver, page_load_timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

        # 2) App-specific readiness (expects your HTML to set window.__mapsReady = true
        #    OR expose window.__mapsReadyPromise that resolves when maps/WMS/charts are done)
        def _maps_ready(drv):
            try:
                return bool(drv.execute_script("return window.__mapsReady === true;"))
            except Exception:
                return False

        # Try quick check first
        if not _maps_ready(driver):
            # Fallback to awaiting a promise if provided by the page
            try:
                driver.set_script_timeout(ready_timeout)
                ok = driver.execute_async_script(
                    """
                    const cb = arguments[arguments.length - 1];
                    try {
                      if (window.__mapsReady === true) { cb(true); return; }
                      const p = window.__mapsReadyPromise;
                      if (p && typeof p.then === 'function') {
                        p.then(() => cb(true)).catch(() => cb(false));
                      } else {
                        cb(false);
                      }
                    } catch (e) { cb(false); }
                    """
                )
                if not ok:
                    # If promise path failed, use a hard wait for a positive flag within timeout
                    WebDriverWait(driver, ready_timeout).until(_maps_ready)
            except Exception:
                # Last resort: wait for a key element to exist AND give a short settle delay
                logger.warning("PDF: __mapsReady not available; using element presence fallback")
                WebDriverWait(driver, 60).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "canvas, #mainMap"))
                )
                time.sleep(0.5)

        # Give the renderer a couple of frames to flush (no arbitrary long sleep)
        try:
            driver.set_script_timeout(10)
            driver.execute_async_script(
                """
                const cb = arguments[arguments.length - 1];
                requestAnimationFrame(() => requestAnimationFrame(() => cb(true)));
                """
            )
        except Exception:
            pass

        # 3) Print to PDF
        p = PrintOptions()
        try:
            # Selenium 4 expects lowercase; if not supported, ValueError is caught
            p.orientation = "landscape" if print_landscape else "portrait"
        except Exception:
            pass
        p.scale = 1.0
        p.background = True

        pdf_b64 = driver.print_page(p)
        pdf_bytes = base64.b64decode(pdf_b64)
        logger.info("PDF: successfully rendered %d bytes", len(pdf_bytes))
        return pdf_bytes

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        try:
            log_file.close()
        except Exception:
            pass
        # cleanup of profile_dir is optional; geckodriver may still hold files briefly
        try:
            # best-effort cleanup
            for root, dirs, files in os.walk(profile_dir, topdown=False):
                for name in files:
                    try:
                        os.remove(os.path.join(root, name))
                    except Exception:
                        pass
                for name in dirs:
                    try:
                        os.rmdir(os.path.join(root, name))
                    except Exception:
                        pass
            os.rmdir(profile_dir)
        except Exception:
            pass
