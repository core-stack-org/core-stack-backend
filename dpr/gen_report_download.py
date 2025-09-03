import base64, os, tempfile

from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.print_page_options import PrintOptions

from webdriver_manager.firefox import GeckoDriverManager
from utilities.logger import setup_logger

logger = setup_logger(__name__)

FIREFOX_BIN = os.environ.get("FIREFOX_BIN")  # e.g. /home/ksheetiz/miniforge3/envs/corestack-backend/bin/FirefoxApp
GECKODRIVER_LOG = os.environ.get("GECKODRIVER_LOG", "geckodriver.log")


def render_pdf_with_firefox(url: str) -> bytes:
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
        raise RuntimeError("No Firefox binary found. Set FIREFOX_BIN or install firefox-esr.")
    opts.binary_location = chosen
    logger.info("PDF: using Firefox binary at %s", chosen)

    # ---- geckodriver path (cache or baked) ----
    geckopath = os.environ.get("GECKODRIVER_PATH") or GeckoDriverManager().install()
    log_path = os.environ.get("GECKODRIVER_LOG", "geckodriver.log")
    log_file = open(log_path, "a", buffering=1)
    logger.info("PDF: using geckodriver at %s (logs -> %s)", geckopath, log_path)

    service = FirefoxService(executable_path=geckopath, log_output=log_file)

    # ---- sanitize environment to avoid Conda/GTK/NSS conflicts ----
    # Conda often sets LD_LIBRARY_PATH etc. that break system Firefox.
    for var in ("LD_LIBRARY_PATH", "DYLD_LIBRARY_PATH", "GTK_PATH", "GTK_DATA_PREFIX", "MOZ_GMP_PATH"):
        if os.environ.get(var):
            logger.warning("PDF: unsetting %s to avoid binary/lib conflicts", var)
            os.environ.pop(var, None)
    os.environ.setdefault("XDG_RUNTIME_DIR", "/tmp")  # avoids Wayland/runtime dir complaints

    # ---- optionally force a fresh, writable profile to /tmp ----
    import tempfile
    profile_dir = tempfile.mkdtemp(prefix="ffprof_")
    opts.add_argument("-profile")
    opts.add_argument(profile_dir)
    logger.info("PDF: using temp Firefox profile at %s", profile_dir)

    driver = webdriver.Firefox(options=opts, service=service)
    try:
        driver.set_page_load_timeout(120)
        driver.get(url)

        WebDriverWait(driver, 60).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "canvas, #mainMap"))
        )

        # wait for webfonts (prevents missing glyphs in PDF)
        driver.execute_script("""
            if (document.fonts && document.fonts.status !== 'loaded') {
                return document.fonts.ready;
            }
        """)

        p = PrintOptions()
        try:
            p.orientation = "landscape"   # newer Selenium expects lowercase
        except ValueError:
            # Fall back to defaults (portrait)
            pass
        p.scale = 1.0
        p.background = True
        pdf_b64 = driver.print_page(p)
        return base64.b64decode(pdf_b64)
    finally:
        try:
            driver.quit()
        finally:
            try:
                log_file.close()
            except Exception:
                pass