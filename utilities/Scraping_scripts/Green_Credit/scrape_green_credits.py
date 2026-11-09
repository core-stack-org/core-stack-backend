import time
import csv
import json
import re
import string
import requests
import zipfile
import io
import xml.etree.ElementTree as ET
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

USE_FUZZY = False
try:
    from rapidfuzz import fuzz
    HAVE_RAPIDFUZZ = True
except Exception:
    HAVE_RAPIDFUZZ = False

# ---------- Selectors ----------
TABLE = "table"
TBODY = "table tbody"
TABLE_ROWS = "table tbody > tr"
MUI_MENU_SELECTORS = [
    "body > div.MuiPopover-root div.MuiMenu-paper",
    "body > div.MuiModal-root .MuiMenu-paper",
    "body > div.MuiPopover-root .MuiPaper-root:has(ul[role='listbox'])",
    "body > div.MuiModal-root .MuiPaper-root:has(ul[role='listbox'])",
    "body > div.MuiPopover-root .MuiMenu-list",
    "body > div.MuiModal-root .MuiMenu-list",
]
MUI_MENU_ITEM = ".MuiMenuItem-root"
# Blocking announcement dialogs (e.g. "Join Training-cum-Doubt Clearing Session").
# Excludes MUI Popovers/Menus, which are also rendered as .MuiModal-root.
POPUP_MODAL_SELECTOR = "div.MuiModal-root:not(.MuiPopover-root):not(.MuiModal-hidden)"
PAGE_NEXT = "button[aria-label*='Next'], button[aria-label='Go to next page'], .MuiPaginationItem-previousNext[aria-label*='Next']"
VIEW_CELL_BUTTON_SELECTORS = ["[aria-label*='view']", ".MuiIconButton-root", "button", "img", "[role='button']"]
EMPTY_PLACEHOLDERS = [".MuiDataGrid-overlay", "p:contains('No rows')", "p:contains('No data')"]
PROJECT_API_SUBSTR = "/api/project/"

# ---------- String normalization ----------
PUNCT_TABLE = str.maketrans('', '', string.punctuation)

def normalize_text(s: str) -> str:
    if not s: return ""
    s = s.casefold().translate(PUNCT_TABLE)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def state_matches(table_state: str, selected_state: str) -> bool:
    a = normalize_text(table_state)
    b = normalize_text(selected_state)
    if a == b: return True
    if USE_FUZZY and HAVE_RAPIDFUZZ:
        try: 
            return fuzz.token_set_ratio(a, b) >= 95
        except Exception: 
            return False
    return False

# ---------- Driver ----------
def setup_driver(headless=False):
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1400,950")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(60)
    try:
        driver.execute_cdp_cmd("Network.enable", {})
    except Exception:
        pass
    print("Loading page...")
    driver.get("https://www.moefcc-gcp.in/")
    
    # Enhanced wait for table and page stability
    WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, TABLE)))
    
    # Close the announcement popup that blocks the page on load
    dismiss_popups(driver, appear_wait=6)
    
    # Scroll to the bottom to ensure the table and filters are loaded
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)  # Increased wait for dynamic content
    
    # Wait for any interactive elements to be present
    try:
        WebDriverWait(driver, 15).until(
            lambda d: len(d.find_elements(By.CSS_SELECTOR, "[role='button']")) > 0
        )
    except TimeoutException:
        pass
    
    print("✓ Page loaded\n")
    return driver

# ---------- Popup handling ----------
def _visible_popups(driver):
    try:
        return driver.find_elements(By.CSS_SELECTOR, POPUP_MODAL_SELECTOR)
    except Exception:
        return []

def dismiss_popups(driver, appear_wait=0.0, close_timeout=4.0):
    """Close blocking MUI announcement modals.

    While such a modal is open MUI sets `overflow:hidden` on <body> and
    `aria-hidden` on #__next, so Selenium reports the page's controls (like the
    'Filter by States' select) as not displayed. Sending ESC to <body> does not
    work because MUI listens for the key on the modal itself.
    """
    # Optionally wait for a late-appearing popup
    end = time.time() + appear_wait
    while time.time() < end and not _visible_popups(driver):
        time.sleep(0.25)

    if not _visible_popups(driver):
        return False

    print("  → Closing blocking popup...")
    end = time.time() + close_timeout
    while time.time() < end:
        modals = _visible_popups(driver)
        if not modals:
            print("  ✓ Popup closed")
            return True
        for modal in modals:
            try:
                btns = modal.find_elements(
                    By.XPATH,
                    ".//button[.//*[@data-testid='CloseIcon'] or "
                    "contains(translate(@aria-label,'CLOSE','close'),'close')]",
                )
                if btns:
                    driver.execute_script("arguments[0].click();", btns[0])
                else:
                    # ESC must go to the focused element inside the modal
                    ActionChains(driver).send_keys(Keys.ESCAPE).perform()
            except Exception:
                pass
        time.sleep(0.4)

    # Last resort: strip the modal from the DOM and unlock the page
    try:
        removed = driver.execute_script("""
            const ms = [...document.querySelectorAll(arguments[0])];
            ms.forEach(m => m.remove());
            document.body.style.overflow = '';
            document.body.style.paddingRight = '';
            const r = document.getElementById('__next');
            if (r) r.removeAttribute('aria-hidden');
            return ms.length;
        """, POPUP_MODAL_SELECTOR)
        print(f"  ✓ Force-removed {removed} popup(s)")
    except Exception as e:
        print(f"  ✗ Could not remove popup: {e}")
    return True

# ---------- Waits ----------
def with_stale_retries(fn, retries=5, sleep=0.06):
    last_err = None
    for _ in range(retries):
        try: 
            return fn()
        except StaleElementReferenceException as e:
            last_err = e
            time.sleep(sleep)
    if last_err: 
        raise last_err

def wait_for_rows_visible(driver, timeout=12):
    end = time.time() + timeout
    while time.time() < end:
        try:
            tbody = driver.find_element(By.CSS_SELECTOR, TBODY)
            rows = tbody.find_elements(By.CSS_SELECTOR, "tr")
            for r in rows:
                tds = r.find_elements(By.TAG_NAME, "td")
                if tds and r.is_displayed(): 
                    return True
        except Exception:
            pass
        for sel in EMPTY_PLACEHOLDERS:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                if any(e.is_displayed() for e in els): 
                    return True
            except Exception:
                pass
        time.sleep(0.15)
    return False

# ---------- Robust text ----------
def get_cell_text(cell):
    try:
        txt = cell.get_attribute("textContent")
        if txt is None: 
            txt = cell.get_attribute("innerText")
        return (txt or "").strip()
    except Exception:
        try: 
            return (cell.text or "").strip()
        except Exception: 
            return ""

# ---------- Debug function ----------
def debug_page_elements(driver):
    """Print all interactive elements for debugging"""
    print("\n=== DEBUGGING PAGE ELEMENTS ===")
    try:
        buttons = driver.find_elements(By.CSS_SELECTOR, "[role='button']")
        print(f"Found {len(buttons)} buttons with role='button'")
        for i, btn in enumerate(buttons[:15]):  # First 15
            try:
                if btn.is_displayed():
                    text = (btn.get_attribute("textContent") or "")[:50].strip()
                    aria_label = (btn.get_attribute("aria-label") or "")[:30]
                    aria_popup = btn.get_attribute("aria-haspopup") or "none"
                    classes = btn.get_attribute("class") or ""
                    print(f"{i+1}. Text: '{text}' | Popup: {aria_popup} | Label: '{aria_label}'")
                    if "MuiSelect" in classes:
                        print(f"   └─ ** FOUND MUI SELECT **")
            except Exception:
                continue
    except Exception as e:
        print(f"Error during debug: {e}")
    print("="*40 + "\n")

# ---------- Dropdown open (Enhanced) ----------
def wait_for_any_menu(driver, timeout=8):
    end = time.time() + timeout
    while time.time() < end:
        for sel in MUI_MENU_SELECTORS:
            try:
                menu = driver.find_element(By.CSS_SELECTOR, sel)
                if menu.is_displayed(): 
                    return menu
            except Exception:
                continue
        time.sleep(0.05)
    raise TimeoutException("Menu not visible")

def locate_state_trigger(driver):
    """Enhanced state trigger location with debugging"""
    
    print("  → Searching for state dropdown trigger...")
    
    # Strategy 0: the 'Filter by States' multi-select in the table header.
    # Don't gate on is_displayed() here - scroll it into view instead.
    primary_xpaths = [
        "//label[@id='demo-multiple-checkbox-label' or contains(normalize-space(.),'Filter by States')]"
        "/following-sibling::*//*[@role='button' and @aria-haspopup='listbox']",
        "//table//thead//*[contains(@class,'MuiSelect-select') and @role='button']",
    ]
    for xp in primary_xpaths:
        try:
            els = driver.find_elements(By.XPATH, xp)
            if els:
                el = els[0]
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                time.sleep(0.2)
                print("  ✓ Found 'Filter by States' select")
                return el
        except Exception:
            continue
    
    # Strategy 1: ARIA-based selectors (most specific)
    xpaths = [
        "//*[@role='button' and contains(@aria-haspopup,'listbox') and contains(translate(@aria-label,'STATE','state'),'state')]",
        "//*[self::label or self::*[contains(@class,'Label')]][contains(translate(normalize-space(.),'STATE','state'),'state')]/following-sibling::*[@role='button'][1]",
        "//*[@role='combobox' and contains(translate(@aria-label,'STATE','state'),'state')]",
        "//*[contains(@class,'MuiSelect-select') and @role='button']",
        "//*[@id='mui-component-select-state']",
        "//*[contains(@id,'state') and @role='button']",
    ]
    
    for i, xp in enumerate(xpaths):
        try:
            els = driver.find_elements(By.XPATH, xp)
            for el in els:
                if el.is_displayed():
                    print(f"  ✓ Found trigger using XPath strategy {i+1}")
                    return el
        except Exception:
            continue
    
    # Strategy 2: CSS-based fallbacks
    css_selectors = [
        "div[role='button'][aria-haspopup='listbox']",
        ".MuiSelect-select[role='button']",
        ".MuiSelect-select",
        "[aria-haspopup='listbox']",
        "div[role='combobox']",
        ".MuiInputBase-root [role='button']",
    ]
    
    for i, sel in enumerate(css_selectors):
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            for el in els:
                if el.is_displayed():
                    text = el.get_attribute("textContent") or ""
                    aria_label = el.get_attribute("aria-label") or ""
                    if "state" in text.lower() or "state" in aria_label.lower() or not text:
                        print(f"  ✓ Found trigger using CSS strategy {i+1}: '{text[:30]}'")
                        return el
        except Exception:
            continue
    
    # Strategy 3: Find all buttons and filter
    try:
        all_buttons = driver.find_elements(By.CSS_SELECTOR, "[role='button']")
        print(f"  → Scanning {len(all_buttons)} buttons on page...")
        
        for btn in all_buttons:
            if not btn.is_displayed():
                continue
            
            text = (btn.get_attribute("textContent") or "").strip().lower()
            aria_label = (btn.get_attribute("aria-label") or "").lower()
            aria_haspopup = btn.get_attribute("aria-haspopup") or ""
            
            if "listbox" in aria_haspopup or "menu" in aria_haspopup:
                if "state" in text or "state" in aria_label or "select" in text.lower():
                    print(f"  ✓ Found via button scan: {text[:30]}")
                    return btn
    except Exception as e:
        print(f"  ✗ Button scan failed: {e}")
    
    print("  ✗ Could not locate state trigger")
    return None

def open_state_dropdown(driver, max_retries=3):
    """Enhanced dropdown opening with better error handling"""
    
    for attempt in range(max_retries + 1):
        print(f"\n  Attempt {attempt + 1}/{max_retries + 1}")
        
        # Close any existing menus
        try:
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
            time.sleep(0.2)
        except Exception:
            pass
        
        # Close announcement popups that lock the page
        dismiss_popups(driver)
        
        # Try scrolling to reveal filters
        if attempt > 0:
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(0.3)
            except Exception:
                pass
        
        # Locate the trigger
        el = locate_state_trigger(driver)
        
        if not el:
            if attempt < max_retries:
                print("  → Retrying with page refresh...")
                driver.refresh()
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, TABLE))
                )
                dismiss_popups(driver, appear_wait=6)
                time.sleep(1)
                continue
            else:
                # Last resort: save screenshot for debugging
                try:
                    driver.save_screenshot("debug_state_dropdown.png")
                    print("  → Saved debug screenshot: debug_state_dropdown.png")
                    
                    with open("debug_page_source.html", "w", encoding="utf-8") as f:
                        f.write(driver.page_source)
                    print("  → Saved page source: debug_page_source.html")
                except Exception:
                    pass
                raise TimeoutException("State trigger not found after all attempts")
        
        # Try multiple click methods
        click_methods = [
            ("ActionChains", lambda: ActionChains(driver).move_to_element(el).click().perform()),
            ("Direct click", lambda: el.click()),
            ("JS click", lambda: driver.execute_script("arguments[0].click();", el)),
            ("Space key", lambda: el.send_keys(Keys.SPACE)),
            ("Enter key", lambda: el.send_keys(Keys.ENTER)),
        ]
        
        for method_name, click_fn in click_methods:
            try:
                print(f"  → Trying {method_name}...")
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                time.sleep(0.1)
                click_fn()
                time.sleep(0.3)
                
                # Check if menu appeared
                menu = wait_for_any_menu(driver, timeout=4)
                if menu:
                    print(f"  ✓ Menu opened with {method_name}")
                    return menu
            except Exception as e:
                print(f"    {method_name} failed: {str(e)[:50]}")
                continue
        
        if attempt < max_retries:
            print("  → All click methods failed, retrying...")
            time.sleep(0.5)
    
    raise TimeoutException("Could not open state dropdown after all fallbacks")

def get_menu_scroll_box(menu, driver):
    for cand in ["ul[role='listbox']", ".MuiMenu-list", ".MuiPaper-root"]:
        try:
            return menu.find_element(By.CSS_SELECTOR, cand)
        except Exception:
            continue
    return menu

def scroll_menu_until_click(menu, driver, match_text_lower, max_scrolls=160):
    scroll_box = get_menu_scroll_box(menu, driver)
    last = -1
    for _ in range(max_scrolls):
        try:
            items = menu.find_elements(By.CSS_SELECTOR, MUI_MENU_ITEM)
        except StaleElementReferenceException:
            menu = open_state_dropdown(driver)
            scroll_box = get_menu_scroll_box(menu, driver)
            items = menu.find_elements(By.CSS_SELECTOR, MUI_MENU_ITEM)
        for i in range(len(items)):
            def read_text(i=i): 
                return items[i].text.strip().lower()
            try:
                t = with_stale_retries(read_text)
                if t == match_text_lower:
                    def click_item(i=i):
                        it = menu.find_elements(By.CSS_SELECTOR, MUI_MENU_ITEM)[i]
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", it)
                        time.sleep(0.03)
                        driver.execute_script("arguments[0].click();", it)
                    with_stale_retries(click_item)
                    return True
            except StaleElementReferenceException:
                continue
        try:
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].offsetHeight;", scroll_box)
        except StaleElementReferenceException:
            menu = open_state_dropdown(driver)
            scroll_box = get_menu_scroll_box(menu, driver)
        time.sleep(0.06)
        try:
            cur = driver.execute_script("return arguments[0].scrollTop;", scroll_box)
            if cur == last: 
                break
            last = cur
        except StaleElementReferenceException:
            menu = open_state_dropdown(driver)
            scroll_box = get_menu_scroll_box(menu, driver)
    return False

def clear_state_selection(driver):
    menu = open_state_dropdown(driver)
    try:
        items = menu.find_elements(By.CSS_SELECTOR, MUI_MENU_ITEM)
    except StaleElementReferenceException:
        menu = open_state_dropdown(driver)
        items = menu.find_elements(By.CSS_SELECTOR, MUI_MENU_ITEM)
    select_all_idx = -1
    for i in range(len(items)):
        def read_text(i=i): 
            return items[i].text.strip().lower()
        try:
            t = with_stale_retries(read_text)
            if "select all" in t or t == "all":
                select_all_idx = i
                break
        except StaleElementReferenceException:
            continue
    if select_all_idx >= 0:
        def click_sa(i=select_all_idx):
            it = menu.find_elements(By.CSS_SELECTOR, MUI_MENU_ITEM)[i]
            driver.execute_script("arguments[0].click();", it)
        with_stale_retries(click_sa)
        time.sleep(0.05)
        with_stale_retries(click_sa)
    try:
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
    except Exception:
        driver.find_element(By.TAG_NAME, "body").click()
    time.sleep(0.12)

def select_state_only(driver, state_text):
    clear_state_selection(driver)
    match_text_lower = state_text.strip().lower()
    menu = open_state_dropdown(driver)
    success = False
    try:
        items = menu.find_elements(By.CSS_SELECTOR, MUI_MENU_ITEM)
    except StaleElementReferenceException:
        menu = open_state_dropdown(driver)
        items = menu.find_elements(By.CSS_SELECTOR, MUI_MENU_ITEM)
    for i in range(len(items)):
        def read_text(i=i): 
            return items[i].text.strip().lower()
        try:
            t = with_stale_retries(read_text)
            if t == match_text_lower:
                def click_item(i=i):
                    it = menu.find_elements(By.CSS_SELECTOR, MUI_MENU_ITEM)[i]
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", it)
                    time.sleep(0.03)
                    driver.execute_script("arguments[0].click();", it)
                with_stale_retries(click_item)
                success = True
                break
        except StaleElementReferenceException:
            continue
    if not success:
        success = scroll_menu_until_click(menu, driver, match_text_lower, max_scrolls=200)
    if not success:
        try: 
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
        except Exception: 
            pass
        return False
    if not wait_for_rows_visible(driver, timeout=12):
        driver.refresh()
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, TABLE)))
        dismiss_popups(driver, appear_wait=6)
        time.sleep(0.5)
        return wait_for_rows_visible(driver, timeout=10)
    return True

# ---------- Network capture ----------
def clear_perf_logs(driver):
    try: 
        _ = driver.get_log("performance")
    except Exception: 
        pass

# Substrings that identify the two API shapes we care about
PROJECT_LIST_SUBSTRINGS = ["/api/project?", "/api/project/?", "/api/projects?", "/api/projects/?"]
PROJECT_DETAIL_SUBSTR   = "/api/project/"   # matches /api/project/<uuid>

def _is_list_api(url):
    low = url.lower()
    return any(s in low for s in PROJECT_LIST_SUBSTRINGS)

def _is_detail_api(url):
    low = url.lower()
    return PROJECT_DETAIL_SUBSTR in low and not _is_list_api(url)

def _capture_body(driver, request_id, timeout=6.0):
    """Retrieve a captured response body via CDP."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
            return body.get("body", "")
        except Exception:
            time.sleep(0.15)
    return None

def wait_list_api_body(driver, timeout=12.0):
    """
    Wait for the project-list API response to arrive after a state filter is applied.
    Returns the raw JSON string (which contains {"count":N,"rows":[...]}).
    """
    end = time.time() + timeout
    rid_map = {}   # requestId -> url
    while time.time() < end:
        try:
            logs = driver.get_log("performance")
        except Exception:
            time.sleep(0.05)
            continue
        for entry in logs:
            try:
                msg = json.loads(entry["message"])["message"]
            except Exception:
                continue
            m = msg.get("method", "")
            p = msg.get("params", {})
            if m == "Network.responseReceived":
                resp = p.get("response", {})
                url  = resp.get("url", "")
                status = resp.get("status", 0)
                if _is_list_api(url) and status == 200:
                    rid_map[p.get("requestId")] = url
            elif m == "Network.loadingFinished":
                rid = p.get("requestId")
                if rid in rid_map:
                    body = _capture_body(driver, rid)
                    if body:
                        return body
        time.sleep(0.1)
    return None

def wait_project_body(driver, timeout=4.0):
    """Wait for a single /api/project/<uuid> detail response."""
    end = time.time() + timeout
    target_rid = None
    while time.time() < end:
        try:
            logs = driver.get_log("performance")
        except Exception:
            time.sleep(0.05)
            continue
        for entry in logs:
            try: 
                msg = json.loads(entry["message"])["message"]
            except Exception: 
                continue
            m = msg.get("method", "")
            p = msg.get("params", {})
            if m == "Network.responseReceived":
                resp = p.get("response", {})
                url = resp.get("url", "")
                status = resp.get("status", 0)
                if _is_detail_api(url) and status == 200:
                    target_rid = p.get("requestId")
            elif m == "Network.loadingFinished" and target_rid and p.get("requestId") == target_rid:
                body = _capture_body(driver, target_rid)
                if body:
                    return body
        time.sleep(0.1)
    return None

def fetch_project_detail_via_cdp(driver, project_id, base_url, timeout=10.0):
    """
    Use CDP Fetch.enable to directly request /api/project/<id> using the
    browser's existing session cookies, without clicking any UI button.
    Falls back to intercepting a button click if CDP Fetch isn't available.
    Returns the raw JSON string or None.
    """
    detail_url = f"{base_url.rstrip('/')}/api/project/{project_id}"
    try:
        driver.execute_cdp_cmd("Fetch.enable", {"patterns": [{"urlPattern": "*"}]})
        result = driver.execute_async_script("""
            var url = arguments[0];
            var done = arguments[1];
            fetch(url, {credentials: 'include'})
                .then(r => r.text())
                .then(t => done(t))
                .catch(e => done(null));
        """, detail_url)
        driver.execute_cdp_cmd("Fetch.disable", {})
        return result
    except Exception:
        try:
            driver.execute_cdp_cmd("Fetch.disable", {})
        except Exception:
            pass
        # Fallback: plain JS fetch using browser session
        try:
            result = driver.execute_async_script("""
                var url = arguments[0];
                var done = arguments[1];
                fetch(url, {credentials: 'include'})
                    .then(r => r.text())
                    .then(t => done(t))
                    .catch(e => done(null));
            """, detail_url)
            return result
        except Exception:
            return None

# ---------- Direct API access (by EPID) ----------
API_BASE = "https://www.moefcc-gcp.in"
_HTTP = requests.Session()
_HTTP.headers.update({"User-Agent": "Mozilla/5.0", "Accept": "application/json"})

def fetch_project_json_by_epid(epid, retries=3):
    """Return the /api/project/<projectDetailId> JSON text for a table row.

    The 'View Progress' icon no longer opens a dialog - it navigates to
    /progress-details/<EPID>, which destroys the table (stale elements). So we
    call the public APIs directly:
      /api/land-ownership/<EPID>  -> projectDetailId
      /api/project/<projectDetailId> -> {"documents": [{label: 'Land Kml File', url}]}
    """
    for attempt in range(1, retries + 1):
        try:
            r = _HTTP.get(f"{API_BASE}/api/land-ownership/{epid}", timeout=30)
            r.raise_for_status()
            project_id = (r.json() or {}).get("projectDetailId")
            if not project_id:
                print(f"  [WARN] No projectDetailId for {epid}")
                return None
            r = _HTTP.get(f"{API_BASE}/api/project/{project_id}", timeout=30)
            r.raise_for_status()
            return r.text
        except Exception as e:
            print(f"  [WARN] API fetch failed for {epid} (attempt {attempt}/{retries}): {e}")
            time.sleep(attempt)
    return None

# ---------- KML parsing ----------
def extract_kml_urls_from_project_json(project_json_text):
    try: 
        data = json.loads(project_json_text)
    except Exception: 
        return []
    docs = data.get("documents", [])
    if not isinstance(docs, list): 
        return []
    kml_urls = []   # definite KML/KMZ (by extension or explicit "kml" in path)
    candidate_urls = []  # likely KML S3 docs (GCDOC / ICFRE presigned URLs)
    for doc in docs:
        if not isinstance(doc, dict): 
            continue
        url = doc.get("url") or doc.get("documentUrl") or ""
        if not url: 
            continue
        # Strip query-string before checking extension so presigned S3 URLs
        # like  GCDOC1709272898324?X-Amz-Algorithm=...  are handled correctly
        path_part = url.split("?")[0].lower()
        if path_part.endswith(".kml") or path_part.endswith(".kmz") or "kml" in path_part:
            kml_urls.insert(0, url)
        elif ("gcdoc" in path_part) or ("icfre" in path_part) or ("icfres3" in path_part) or ("x-amz-expires" in url.lower()):
            candidate_urls.append(url)
    # Return definite KML URLs first, then S3 candidates so we always try every URL
    return kml_urls + candidate_urls

def download_text(url, timeout=30):
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code == 200: 
            return r.content
    except Exception:
        return None
    return None

def parse_kml_to_geojson(kml_text):
    try:
        if isinstance(kml_text, bytes):
            if kml_text.startswith(b"PK"):
                try:
                    with zipfile.ZipFile(io.BytesIO(kml_text)) as z:
                        kml_filename = next((name for name in z.namelist() if name.lower().endswith('.kml')), None)
                        if kml_filename:
                            kml_text = z.read(kml_filename)
                        else:
                            return None
                except Exception:
                    pass
            kml_text = kml_text.decode('utf-8', errors='ignore')
            
        # Strip default namespace so xpath like .//Polygon works, 
        # but KEEP prefixed namespaces (like xmlns:gx) to avoid ParseError
        kml_text = re.sub(r'\s+xmlns=\"[^\"]+\"', '', kml_text)
        root = ET.fromstring(kml_text)
        polygons = []
        
        def parse_ring(text):
            coords = []
            for pt in re.split(r"\s+", text.strip()):
                if not pt: 
                    continue
                parts = pt.split(",")
                if len(parts) >= 2:
                    try:
                        lon = float(parts[0])
                        lat = float(parts[1])
                        coords.append([lon, lat])
                    except Exception:
                        continue
            if len(coords) >= 3:
                if coords[0] != coords[-1]: 
                    coords.append(coords[0])
                return coords
            return None
        
        for poly in root.findall(".//Polygon"):
            outers = poly.findall(".//outerBoundaryIs/coordinates") or poly.findall(".//coordinates")
            for c in outers:
                if c is not None and c.text:
                    ring = parse_ring(c.text)
                    if ring: 
                        polygons.append([ring])
        
        for mg in root.findall(".//MultiGeometry"):
            for poly in mg.findall(".//Polygon"):
                outers = poly.findall(".//outerBoundaryIs/coordinates") or poly.findall(".//coordinates")
                for c in outers:
                    if c is not None and c.text:
                        ring = parse_ring(c.text)
                        if ring: 
                            polygons.append([ring])
        
        if polygons:
            if len(polygons) == 1:
                return {"type": "Polygon", "coordinates": polygons[0]}
            else:
                return {"type": "MultiPolygon", "coordinates": polygons}

        def parse_coords(text):
            """Parse a coordinates string into a list of [lon, lat] pairs."""
            coords = []
            for pt in re.split(r"\s+", text.strip()):
                if not pt:
                    continue
                parts = pt.split(",")
                if len(parts) >= 2:
                    try:
                        coords.append([float(parts[0]), float(parts[1])])
                    except Exception:
                        continue
            return coords

        # ── Fallback 1: LineString ──────────────────────────────────────────
        line_coords = []
        for ls in root.findall(".//LineString"):
            c = ls.find(".//coordinates")
            if c is not None and c.text:
                pts = parse_coords(c.text)
                if pts:
                    line_coords.extend(pts)
        if line_coords:
            return {"type": "LineString", "coordinates": line_coords}

        # ── Fallback 2: Point ───────────────────────────────────────────────
        for pt_el in root.findall(".//Point"):
            c = pt_el.find(".//coordinates")
            if c is not None and c.text:
                pts = parse_coords(c.text)
                if pts:
                    return {"type": "Point", "coordinates": pts[0]}

        # ── Fallback 3: bare <coordinates> anywhere ─────────────────────────
        c = root.find(".//coordinates")
        if c is not None and c.text:
            ring = parse_ring(c.text)
            if ring:
                return {"type": "Polygon", "coordinates": [ring]}
            pts = parse_coords(c.text)
            if len(pts) == 1:
                return {"type": "Point", "coordinates": pts[0]}
            if len(pts) >= 2:
                return {"type": "LineString", "coordinates": pts}

        # ── Fallback 4: external href link ──────────────────────────────────
        links = re.findall(r'href=[\'"]?(http[^\'" >]+)', kml_text)
        if links:
            return {"type": "ExternalLink", "url": links[0]}

        return None
    except Exception:
        return None

# ---------- Pagination + alignment ----------
def get_rows_fresh(driver):
    try:
        tbody = driver.find_element(By.CSS_SELECTOR, TBODY)
        return tbody.find_elements(By.CSS_SELECTOR, "tr")
    except Exception:
        return []

def count_valid_rows(driver):
    def _count():
        rows = get_rows_fresh(driver)
        cnt = 0
        for r in rows:
            try:
                if r.find_elements(By.TAG_NAME, "td"): 
                    cnt += 1
            except StaleElementReferenceException:
                continue
        return cnt
    return with_stale_retries(_count, retries=6)

def click_next(driver):
    try:
        nxt = driver.find_element(By.CSS_SELECTOR, PAGE_NEXT)
    except Exception:
        nxt = None
    if not nxt: 
        return False
    try:
        if not nxt.is_enabled():
            return False
        if nxt.get_attribute("disabled"):
            return False
        if nxt.get_attribute("aria-disabled") in ("true", "True"): 
            return False
        cls = nxt.get_attribute("class")
        if cls and "Mui-disabled" in cls:
            return False
    except Exception:
        pass
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", nxt)
        time.sleep(0.05)
        driver.execute_script("arguments[0].click();", nxt)
        return wait_for_rows_visible(driver, timeout=10)
    except Exception:
        return False

def page_rows_preview(driver, max_rows=12):
    preview = []
    try:
        rows = get_rows_fresh(driver)
        for r in rows:
            tds = r.find_elements(By.TAG_NAME, "td")
            if not tds: 
                continue
            state_text = get_cell_text(tds[2]) if len(tds) > 2 else ""
            preview.append((state_text, True))
            if len(preview) >= max_rows: 
                break
    except Exception:
        pass
    return preview

def hard_realign_to_state(driver, state):
    try:
        if not select_state_only(driver, state): 
            return False
        return wait_for_rows_visible(driver, timeout=10)
    except Exception:
        return False

def wait_page_all_rows_match_state(driver, selected_state, timeout=6.0, poll=0.25):
    end = time.time() + timeout
    while time.time() < end:
        ok = True
        rows = get_rows_fresh(driver)
        any_data = False
        for r in rows:
            try:
                tds = r.find_elements(By.TAG_NAME, "td")
            except StaleElementReferenceException:
                ok = False
                break
            if not tds: 
                continue
            any_data = True
            state_text = get_cell_text(tds[2]) if len(tds) > 2 else ""
            if not state_text or not state_matches(state_text, selected_state):
                ok = False
                break
        if any_data and ok: 
            return True
        time.sleep(poll)
    return False

VIEW_CELL_BUTTON_SELECTORS = ["[aria-label*='view']", ".MuiIconButton-root", "button", "a", "svg", "img", "[role='button']"]

# ---------- Row access ----------
def get_row_data_and_button(driver, valid_index):
    def _resolve():
        rows = get_rows_fresh(driver)
        idx = -1
        for r in rows:
            tds = r.find_elements(By.TAG_NAME, "td")
            if not tds: 
                continue
            idx += 1
            if idx == valid_index:
                # Data columns: Registration No .. Area (8). Trailing icon columns
                # ('Location', 'View Progress') are excluded so the polygon payload
                # lands at index 8, matching save_csv/export_geojson.
                cells_text = [get_cell_text(c) for c in tds[:8]]
                btn = None
                for sel in VIEW_CELL_BUTTON_SELECTORS:
                    try:
                        els = tds[-1].find_elements(By.CSS_SELECTOR, sel)
                        for el in els:
                            btn = el
                            if el.is_displayed():
                                break
                        if btn: 
                            break
                    except Exception:
                        continue
                
                if not btn:
                    try:
                        html = tds[-1].get_attribute("innerHTML")
                        print(f"  [DEBUG] Row {idx+1} missing button. Last cell HTML: {html.strip()}")
                    except Exception:
                        pass
                        
                return cells_text, btn
        return None, None
    return with_stale_retries(_resolve, retries=6)

# ---------- Per-state scraping ----------

def _resolve_kml_geometry(kml_urls):
    """Try each KML URL. Returns (geom, url) where geom may be None if parse failed but URL downloaded ok."""
    for url in kml_urls:
        kml_content = download_text(url, timeout=30)
        if not kml_content:
            continue
        geom = parse_kml_to_geojson(kml_content)
        if geom and ("coordinates" in geom or geom.get("type") == "ExternalLink"):
            return geom, url
        # Downloaded but couldn't parse geometry — still return the URL so it's saved
        return None, url
    return None, None  # every URL failed to download


def scrape_all_pages_for_current_state(driver, current_state):
    all_rows = []
    if not wait_for_rows_visible(driver, timeout=12):
        print("Warning: no visible rows after selection; skipping.")
        return all_rows

    page_idx = 1
    total_processed = 0
    total_skipped = 0

    while True:
        aligned = wait_page_all_rows_match_state(driver, current_state, timeout=6.0, poll=0.25)
        if not aligned and hard_realign_to_state(driver, current_state):
            aligned = wait_page_all_rows_match_state(driver, current_state, timeout=6.0, poll=0.25)
        if not aligned:
            preview = page_rows_preview(driver, max_rows=10)
            bad = [s for (s, _) in preview if not s or not state_matches(s, current_state)]
            if bad:
                print(f"[{current_state}] Page {page_idx}: mixed states present -> {bad}")

        total = count_valid_rows(driver)
        page_processed = 0
        page_skipped = 0

        idx = 0
        while idx < total:
            row_data, btn = get_row_data_and_button(driver, idx)
            print(f"  [DEBUG] Page {page_idx} Row {idx}: row_data={row_data}, has_btn={btn is not None}")
            idx += 1
            if not row_data:
                print(f"  [DEBUG] Skipping because row_data is empty!")
                page_skipped += 1
                continue

            # State mismatch: warn but DO NOT drop — save the row as-is
            if len(row_data) > 2 and not state_matches(row_data[2], current_state):
                print(f"  [DEBUG] State mismatch (expected={current_state}, got={row_data[2]}). Row still saved.")

            epid = row_data[1].strip() if len(row_data) > 1 else ""
            if not epid.upper().startswith("EPID"):
                print(f"  [DEBUG] Skipping because no EPID. row_data={row_data}")
                row_data.append("NO_BODY")
                all_rows.append(row_data)
                page_skipped += 1
                continue

            body_text = fetch_project_json_by_epid(epid)

            if body_text:
                kml_urls = extract_kml_urls_from_project_json(body_text)
                if kml_urls:
                    geom, used_url = _resolve_kml_geometry(kml_urls)
                    if used_url is None:
                        # Every URL failed to download (expired presigned URLs etc.)
                        print(f"  [WARN] All {len(kml_urls)} KML URL(s) failed to download. Saving with kml_url.")
                        row_data.append(json.dumps({"kml_url": kml_urls[0], "all_kml_urls": kml_urls, "geometry": None, "download_fail": True}))
                        page_skipped += 1
                    elif geom is None:
                        # Downloaded but geometry parse failed — save URL for review
                        print(f"  [WARN] KML parse failed. URL saved: {used_url}")
                        row_data.append(json.dumps({"kml_url": used_url, "all_kml_urls": kml_urls, "geometry": None, "parse_fail": True}))
                        page_processed += 1
                    elif geom.get("type") == "ExternalLink":
                        row_data.append(json.dumps({"kml_url": used_url, "geometry": None, "external_link": geom["url"]}))
                        page_processed += 1
                    else:
                        row_data.append(json.dumps({"kml_url": used_url, "geometry": geom}))
                        page_processed += 1
                else:
                    row_data.append("NO_URL")
                    page_skipped += 1
                    print(f"  [DEBUG] NO_URL. JSON body start: {body_text[:250]}")
            else:
                row_data.append("NO_BODY")
                page_skipped += 1

            all_rows.append(row_data)

        total_processed += page_processed
        total_skipped += page_skipped
        print(f"[{current_state}] Page {page_idx} summary: processed={page_processed}, skipped={page_skipped}, rows_seen={total}")

        if not click_next(driver):
            break
        page_idx += 1
        wait_for_rows_visible(driver, timeout=10)
        wait_page_all_rows_match_state(driver, current_state, timeout=6.0, poll=0.25)

    print(f"[{current_state}] Total summary: processed={total_processed}, skipped={total_skipped}")
    return all_rows

# ---------- Output ----------
def save_csv(data, filename):
    headers = ["Registration No", "EPID Id", "State", "District", "Forest Division", "Range", "Entity Name", "Area", "Polygon Data"]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)
    print(f"✓ CSV saved: {filename} ({len(data)} rows)")

def export_geojson(data, output_dir):
    Path(output_dir).mkdir(exist_ok=True)
    success = 0       # rows with real geometry
    no_geom = 0       # rows saved with geometry=null (parse_fail / external_link only)
    truly_failed = 0  # rows with no usable payload at all
    state_data = {}
    
    for idx, row in enumerate(data, 1):
        if len(row) < 9:
            truly_failed += 1
            continue
        payload = row[8]
        # Rows with absolutely no useful data — skip only these
        if (not payload) or payload in ["NO_BUTTON", "NO_URL", "NO_BODY", "ERROR"]:
            truly_failed += 1
            continue
        # Legacy PARSE_FAIL string (old format) — extract URL and still save
        if isinstance(payload, str) and payload.startswith("PARSE_FAIL:"):
            kml_url_from_fail = payload[len("PARSE_FAIL:"):]
            obj = {"kml_url": kml_url_from_fail, "geometry": None, "parse_fail": True}
        elif isinstance(payload, str) and payload.startswith("NO_KML"):
            truly_failed += 1
            continue
        else:
            try:
                obj = json.loads(payload)
            except Exception:
                truly_failed += 1
                continue

        geom = obj.get("geometry")
        ext_link = obj.get("external_link", "")
        parse_fail = obj.get("parse_fail", False)
        kml_url = obj.get("kml_url", "")
        all_kml_urls = obj.get("all_kml_urls", [])

        # Only skip if there is genuinely nothing to record
        if not geom and not ext_link and not kml_url and not all_kml_urls:
            truly_failed += 1
            continue

        state = row[2] or "Unknown"
        district = row[3] or "Unknown"
        parcel_id = row[1] or f"parcel_{idx}"
        feat = {
            "type": "Feature",
            "properties": {
                "registration_no": row[0] if len(row) > 0 else "",
                "parcel_id": parcel_id,
                "state": state,
                "district": district,
                "division": row[4] if len(row) > 4 else "",
                "range": row[5] if len(row) > 5 else "",
                "entity_name": row[6] if len(row) > 6 else "",
                "land_area": row[7] if len(row) > 7 else "",
                "kml_url": kml_url,
            },
            "geometry": geom  # may be null — GeoJSON spec allows null geometry
        }
        state_data.setdefault(state, {}).setdefault(district, []).append(feat)
        if geom:
            success += 1
        else:
            no_geom += 1
    
    for state, districts in state_data.items():
        safe_state = re.sub(r'[<>:"/\\|?*]', '_', state)
        sdir = Path(output_dir) / safe_state
        sdir.mkdir(exist_ok=True)
        for district, feats in districts.items():
            safe_dist = re.sub(r'[<>:"/\\|?*]', '_', district)
            with open(sdir / f"{safe_dist}.geojson", "w", encoding="utf-8") as f:
                json.dump({
                    "type": "FeatureCollection",
                    "features": feats,
                    "properties": {
                        "state": state,
                        "district": district,
                        "total": len(feats)
                    }
                }, f, indent=2)
    
    all_features = [feat for d in state_data.values() for feats in d.values() for feat in feats]
    with open(Path(output_dir) / "ALL_SITES.geojson", "w", encoding="utf-8") as f:
        json.dump({
            "type": "FeatureCollection",
            "features": all_features,
            "properties": {
                "total_states": len(state_data),
                "total_sites": len(all_features)
            }
        }, f, indent=2)


    # 1. Also save in the current folder as green_credit_{state}.geojson
    current_state_name = list(state_data.keys())[0] if state_data else "Unknown"
    safe_state_name = re.sub(r'[<>:"/\\|?*]', '_', current_state_name)
    root_geojson_path = f"green_credit_{safe_state_name}.geojson"
    try:
        with open(root_geojson_path, "w", encoding="utf-8") as f:
            json.dump({
                "type": "FeatureCollection",
                "features": all_features,
                "properties": {
                    "state": current_state_name,
                    "total": len(all_features)
                }
            }, f, indent=2)
        print(f"✓ Root GeoJSON saved: {root_geojson_path} ({len(all_features)} features)")
    except Exception as e:
        print(f"Failed to save root GeoJSON: {e}")

    # 2. Also save in ALL_STATE_GEOJSON/ALL_SITES_{abbr}.geojson
    STATE_ABBREVIATIONS = {
        "UTTAR PRADESH": "UP",
        "GUJARAT": "GJ",
        "JHARKHAND": "JH",
        "MADHYA PRADESH": "MP",
        "RAJASTHAN": "RJ",
        "WEST BENGAL": "WB",
        "ANDHRA PRADESH": "AP",
        "HIMACHAL PRADESH": "HP",
        "JAMMU AND KASHMIR": "JK",
        "TAMIL NADU": "TN",
        "ARUNACHAL PRADESH": "AR",
        "ANDAMAN & NICOBAR":"AN",
        "ASSAM":"AS",

    }
    state_upper = current_state_name.strip().upper()
    state_abbr = STATE_ABBREVIATIONS.get(state_upper, state_upper.replace(" ", "_"))
    
    all_state_folder = Path("ALL_STATE_GEOJSON")
    all_state_folder.mkdir(exist_ok=True)
    all_state_path = all_state_folder / f"ALL_SITES_{state_abbr}.geojson"
    try:
        with open(all_state_path, "w", encoding="utf-8") as f:
            json.dump({
                "type": "FeatureCollection",
                "features": all_features,
                "properties": {
                    "state": current_state_name,
                    "total": len(all_features)
                }
            }, f, indent=2)
        print(f"✓ ALL_STATE_GEOJSON saved: {all_state_path} ({len(all_features)} features)")
    except Exception as e:
        print(f"Failed to save ALL_STATE_GEOJSON: {e}")
    
    print("\n" + "="*70 + "\nEXPORT SUMMARY\n" + "="*70 +
          f"\n✓ With geometry:    {success}"
          f"\n~ geometry=null:    {no_geom}  (kml_url preserved in properties)"
          f"\n✗ No data at all:   {truly_failed}"
          f"\nTotal features:     {len(all_features)}"
          f"\nOutput: {output_dir}/")

# ---------- Main ----------
def main():
    driver = None
    try:
        print("="*70)
        print(" GREEN CREDIT REGISTRY - STATE-WISE SCRAPER")
        print("="*70)
        print()
        driver = setup_driver(headless=False)
        
        # Debug: Print all interactive elements
        debug_page_elements(driver)

        # We are only scraping ODISHA
        print("\n" + "="*70)
        print("SETTING STATE TO CHHATTISGARH")
        print("="*70)
        
        states = ["BIHAR","ASSAM", "CHHATTISGARH", "GOA", "GUJARAT","JHARKHAND", "MADHYA PRADESH","MAHARASHTRA","ODISHA","RAJASTHAN", "TELANGANA","UTTAR PRADESH"]
        print(f"\n✓ Processing only {len(states)} state(s)")
        print(f"States: {', '.join(states)}")

        # Process each state
        for i, state in enumerate(states, 1):
            print("\n" + "-"*60)
            print(f"[{i}/{len(states)}] Processing state: {state}")
            print("-"*60)
            
            if not select_state_only(driver, state):
                print(f"✗ Table not ready after selecting: {state}")
                safe_state = re.sub(r'[<>:"/\\|?*]', '_', state)
                save_csv([], filename=f"green_credit_{safe_state}.csv")
                continue

            rows = scrape_all_pages_for_current_state(driver, state)
            
            # The table is already filtered by the state dropdown, so we don't need to 
            # strictly enforce state_matches here, which was causing 3 valid rows to be dropped.
            filtered_rows = [r for r in rows if r and len(r) > 2]
            
            print(f"✓ State done: {state} ({len(filtered_rows)} rows)")
            
            safe_state = re.sub(r'[<>:"/\\|?*]', '_', state)
            save_csv(filtered_rows, filename=f"green_credit_{safe_state}.csv")
            export_geojson(filtered_rows, output_dir=f"geojson_{safe_state}")

        print("\n" + "="*70 + "\n✓ COMPLETE!\n" + "="*70)
    
    except KeyboardInterrupt:
        print("\nStopped by user")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if driver:
            try:
                input("\nPress Enter to close...")
            except EOFError:
                pass
            driver.quit()

if __name__ == "__main__":
    main()
