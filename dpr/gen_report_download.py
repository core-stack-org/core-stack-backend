import base64
import os
import tempfile
import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from nrm_app.settings import TMP_LOCATION

from utilities.logger import setup_logger

logger = setup_logger(__name__)

# ---- Cache chromedriver path in memory ----
_chromedriver_path = None


def _get_chromedriver_path():
    """
    Get chromedriver path. Install only on first call per worker process.
    - First request: find/install & cache
    - All subsequent requests: return cached path (instant)
    - No race conditions because path is cached after first call
    """
    global _chromedriver_path

    # Already cached in this process
    if _chromedriver_path:
        return _chromedriver_path

    # Check env override
    if os.environ.get("CHROMEDRIVER_PATH"):
        _chromedriver_path = os.environ["CHROMEDRIVER_PATH"]
        logger.info("PDF: using CHROMEDRIVER_PATH from env: %s", _chromedriver_path)
        return _chromedriver_path

    # Check well-known locations
    for candidate in (
        "/var/www/.wdm/drivers/chromedriver/linux64/148.0.7778.167/chromedriver-linux64/chromedriver",
        "/usr/bin/chromedriver",
        "/usr/local/bin/chromedriver",
        "/snap/chromium/current/usr/lib/chromium-browser/chromedriver",
    ):
        if os.path.exists(candidate) and os.access(candidate, os.X_OK):
            _chromedriver_path = candidate
            logger.info("PDF: found chromedriver at: %s", _chromedriver_path)
            return _chromedriver_path

    # Install once (only happens on first request in this worker process)
    logger.info("PDF: installing chromedriver...")
    _chromedriver_path = ChromeDriverManager().install()
    logger.info("PDF: ✓ chromedriver installed at: %s", _chromedriver_path)

    return _chromedriver_path


def render_pdf_with_firefox(
        url: str,
        *,
        page_load_timeout: int = 120,
        ready_timeout: int = 180,
        viewport_width: int = 1600,
        viewport_height: int = 1200,
        print_landscape: bool = True,
) -> bytes:
    """
    Renders a URL to PDF using headless Chromium.
    Function name kept as render_pdf_with_firefox for API compatibility.
    """
    opts = ChromeOptions()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--remote-debugging-port=0")
    opts.add_argument("--disable-features=VizDisplayCompositor")
    opts.add_argument(f"--window-size={viewport_width},{viewport_height}")

    # ---- choose binary ----
    chrome_bin = os.environ.get("CHROME_BIN")
    if chrome_bin and os.path.exists(chrome_bin):
        chosen = chrome_bin
    elif os.path.exists("/usr/local/bin/chrome-wrapper"):     # ADD THIS
        chosen = "/usr/local/bin/chrome-wrapper"   
    elif os.path.exists("/usr/bin/google-chrome"):
        chosen = "/usr/bin/google-chrome"
    elif os.path.exists("/usr/bin/chromium"):
        chosen = "/usr/bin/chromium"
    elif os.path.exists("/usr/bin/chromium-browser"):
        chosen = "/usr/bin/chromium-browser"
    elif os.path.exists("/snap/bin/chromium"):
        chosen = "/snap/bin/chromium"
    elif os.path.exists("/usr/bin/google-chrome-stable"):
        chosen = "/usr/bin/google-chrome-stable"
    else:
        raise RuntimeError(
            "No Chrome/Chromium binary found. Set CHROME_BIN or install chromium/google-chrome."
        )
    opts.binary_location = chosen
    logger.info("PDF: using Chrome binary at %s", chosen)

    # ---- logs ----
    try:
        base_dir = os.path.dirname(os.path.dirname(__file__))
    except Exception:
        base_dir = os.getcwd()
    logs_dir = os.path.join(base_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    log_path = os.path.join(logs_dir, "chromedriver.log")
    log_file = open(log_path, "a", buffering=1, encoding="utf-8", errors="replace")

    # Let Selenium Manager auto-download correct chromedriver for installed Chrome
    chromedriver_path = _get_chromedriver_path()
    service = ChromeService(executable_path=chromedriver_path, log_output=log_file)
    logger.info("PDF: using chromedriver at %s", chromedriver_path)
    os.environ["NO_AT_BRIDGE"] = "1"

    # ---- temp user data dir ----
    user_data_dir = tempfile.mkdtemp(prefix="chromeprof_")
    opts.add_argument(f"--user-data-dir={user_data_dir}")
    logger.info("PDF: using temp Chrome profile at %s", user_data_dir)

    driver = webdriver.Chrome(options=opts, service=service)

    try:
        driver.set_page_load_timeout(page_load_timeout)
        driver.get(url)

        # 1) DOM ready
        WebDriverWait(driver, page_load_timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

        # 2) App-specific readiness
        def _maps_ready(drv):
            try:
                return bool(drv.execute_script("return window.__mapsReady === true;"))
            except Exception:
                return False

        if not _maps_ready(driver):
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
                    WebDriverWait(driver, ready_timeout).until(_maps_ready)
            except Exception:
                logger.warning("PDF: __mapsReady not available; using element presence fallback")
                WebDriverWait(driver, 60).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "canvas, #mainMap"))
                )
                time.sleep(0.5)

        # Give renderer a couple frames
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

        # 3) Print to PDF via Chrome DevTools Protocol
        pdf_b64 = driver.execute_cdp_cmd("Page.printToPDF", {
            "landscape": print_landscape,
            "printBackground": True,
            "scale": 1.0,
            "paperWidth": 11,   # inches (landscape A4 equivalent)
            "paperHeight": 8.5,
        })
        pdf_bytes = base64.b64decode(pdf_b64["data"])
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
        try:
            for root, dirs, files in os.walk(user_data_dir, topdown=False):
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
            os.rmdir(user_data_dir)
        except Exception:
            pass
