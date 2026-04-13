"""
=============================================================================
  Bundesliga-Register — Verletzungsportal
  Run:   python app.py
  Open:  http://localhost:5000

  Player names + DOB from Transfermarkt /plus/1 detailed squad view:
    https://www.transfermarkt.com/SLUG/kader/verein/ID/saison_id/2024/plus/1

  The /plus/1 view adds a full date of birth column (e.g. "Mar 27, 1986 (38)")
  to every player row, which is the stable cross-season matching key.

  Matching strategy
  -----------------
  1. Scrape club squad from Transfermarkt /plus/1.
  2. Get DOB for each Spieler-ID from REDCap PID 83 (bas_geburtsdatum).
  3. Match PID-83-DOB == TM-DOB  →  player name resolved.
  4. Fallback: shirt number from Spieler-ID suffix (e.g. FCB-17 → shirt 17).

  Two REDCap projects
  -------------------
  PID 83 Basisdaten:    bas_spieler_id, bas_geburtsdatum
  PID 84 Registerdaten: spieler_id, allg_verl_erkr (0/1/2),
                        allg_verl_datum, allg_erkr_datum,
                        exakte_diagnose, return_to_activity,
                        return_to_sport, return_to_play, status

  Setup
  -----
  1. pip install flask requests beautifulsoup4
  2. export REDCAP_TOKEN_83="D56E5AE2C570B8A7E2BD40A8DCF6755A"
     export REDCAP_TOKEN_84="E9821323902C30904C1D0ACD0EAB4B5"
     export REDCAP_SURVEY_HASH="ETH7XANTKDHDLY4L"
  3. Edit CLUB_CONFIG and PHYSICIAN_ACCOUNTS below.
  4. python app.py
=============================================================================
"""

import os, json, sqlite3, secrets, re
from datetime import datetime, timedelta
from contextlib import contextmanager
from functools import wraps

import requests
from bs4 import BeautifulSoup
from flask import (Flask, request, jsonify, render_template_string,
                   session, redirect, url_for, abort)

# ---------------------------------------------------------------------------
#  REDCap
# ---------------------------------------------------------------------------
REDCAP_BASE_URL    = "https://redcap-test.ukr.de/redcap_v14.6.0/API/"
REDCAP_TOKEN_83    = os.getenv("REDCAP_TOKEN_83",  "DEMO_TOKEN_83")
REDCAP_TOKEN_84    = os.getenv("REDCAP_TOKEN_84",  "DEMO_TOKEN_84")
REDCAP_SURVEY_HASH = os.getenv("REDCAP_SURVEY_HASH", "ETH7XANTKDHDLY4L")

# ---------------------------------------------------------------------------
#  Transfermarkt
# ---------------------------------------------------------------------------
TM_BASE    = "https://www.transfermarkt.com"
TM_SEASON  = "2024"
TM_CACHE_H = 24
TM_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Referer": "https://www.transfermarkt.com/",
}

# ---------------------------------------------------------------------------
#  Club configuration
#  tm_slug + tm_id come from the Transfermarkt URL:
#  .../SLUG/kader/verein/ID/saison_id/2024/plus/1
# ---------------------------------------------------------------------------
CLUB_CONFIG = {
    "FCB": {"name":"FC Bayern München",          "tm_slug":"fc-bayern-munchen",         "tm_id":"27"},
    "BVB": {"name":"Borussia Dortmund",           "tm_slug":"borussia-dortmund",          "tm_id":"16"},
    "B04": {"name":"Bayer 04 Leverkusen",         "tm_slug":"bayer-04-leverkusen",        "tm_id":"15"},
    "RBL": {"name":"RB Leipzig",                  "tm_slug":"rb-leipzig",                 "tm_id":"23826"},
    "SGE": {"name":"Eintracht Frankfurt",         "tm_slug":"eintracht-frankfurt",        "tm_id":"24"},
    "VFB": {"name":"VfB Stuttgart",               "tm_slug":"vfb-stuttgart",              "tm_id":"79"},
    "SVW": {"name":"Werder Bremen",               "tm_slug":"sv-werder-bremen",           "tm_id":"86"},
    "HF":  {"name":"TSG Hoffenheim",              "tm_slug":"tsg-hoffenheim",             "tm_id":"533"},
    "FCA": {"name":"FC Augsburg",                 "tm_slug":"fc-augsburg",                "tm_id":"167"},
    "BMG": {"name":"Borussia Mönchengladbach",    "tm_slug":"borussia-monchengladbach",   "tm_id":"23"},
    "SCF": {"name":"SC Freiburg",                 "tm_slug":"sport-club-freiburg",        "tm_id":"60"},
    "M05": {"name":"1. FSV Mainz 05",             "tm_slug":"1-fsv-mainz-05",             "tm_id":"62"},
    "WOB": {"name":"VfL Wolfsburg",               "tm_slug":"vfl-wolfsburg",              "tm_id":"82"},
    "BOC": {"name":"VfL Bochum",                  "tm_slug":"vfl-bochum",                 "tm_id":"80"},
    "UNI": {"name":"1. FC Union Berlin",          "tm_slug":"1-fc-union-berlin",          "tm_id":"89"},
    "HDH": {"name":"1. FC Heidenheim",            "tm_slug":"1-fc-heidenheim-1846",       "tm_id":"2036"},
    "KSV": {"name":"Holstein Kiel",               "tm_slug":"holstein-kiel",              "tm_id":"35"},
    "STP": {"name":"FC St. Pauli",                "tm_slug":"fc-st-pauli",                "tm_id":"70"},
}

# ---------------------------------------------------------------------------
#  Physician accounts: "username": ("password", "CLUB_PREFIX")
# ---------------------------------------------------------------------------
PHYSICIAN_ACCOUNTS = {
    "dr.mueller":   ("Bundesliga#2024", "FCB"),
    "dr.schneider": ("Bundesliga#2024", "BVB"),
    "dr.hoffmann":  ("Bundesliga#2024", "STP"),
}

# ---------------------------------------------------------------------------
#  App
# ---------------------------------------------------------------------------
DATABASE   = "portal.db"
SECRET_KEY = os.getenv("SECRET_KEY", secrets.token_hex(32))
app = Flask(__name__)
app.secret_key = SECRET_KEY
app.permanent_session_lifetime = timedelta(hours=8)


@contextmanager
def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_db() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS tm_players (
            tm_id          TEXT NOT NULL,
            club_prefix    TEXT NOT NULL,
            season         TEXT NOT NULL,
            full_name      TEXT NOT NULL,
            date_of_birth  TEXT,
            age            TEXT,
            nationality    TEXT,
            nationality2   TEXT,
            position       TEXT,
            shirt_number   TEXT,
            market_value   TEXT,
            tm_profile_url TEXT,
            image_url      TEXT,
            scraped_at     TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (tm_id, season)
        );
        CREATE TABLE IF NOT EXISTS spieler_map (
            spieler_id     TEXT PRIMARY KEY,
            club_prefix    TEXT NOT NULL,
            tm_id          TEXT,
            full_name      TEXT,
            date_of_birth  TEXT,
            matched_by     TEXT DEFAULT 'unmatched',
            updated_at     TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS tm_cache_meta (
            club_prefix  TEXT NOT NULL,
            season       TEXT NOT NULL,
            last_scraped TEXT,
            player_count INTEGER DEFAULT 0,
            PRIMARY KEY (club_prefix, season)
        );
        """)
    print(f"[DB] Bereit: {DATABASE}")


# ---------------------------------------------------------------------------
#  DOB parsing
# ---------------------------------------------------------------------------
def parse_tm_dob(raw):
    """
    Parse DOB string from Transfermarkt /plus/1 page.
    Handles:  'Mar 27, 1986 (38)'  and  '27.03.1986 (38)'
    Returns (iso_date, age)  e.g.  ('1986-03-27', '38')
    """
    if not raw:
        return "", ""
    age = ""
    am = re.search(r"\((\d+)\)", raw)
    if am:
        age = am.group(1)
    # German: DD.MM.YYYY
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", raw)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}", age
    # English: Mon DD, YYYY
    months = {"Jan":"01","Feb":"02","Mar":"03","Apr":"04","May":"05","Jun":"06",
               "Jul":"07","Aug":"08","Sep":"09","Oct":"10","Nov":"11","Dec":"12"}
    m2 = re.search(
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),\s+(\d{4})", raw)
    if m2:
        return (f"{m2.group(3)}-{months.get(m2.group(1),'00')}-{m2.group(2).zfill(2)}",
                age)
    return "", age


# ---------------------------------------------------------------------------
#  Transfermarkt scraping
# ---------------------------------------------------------------------------
def tm_squad_url(club_prefix):
    cfg = CLUB_CONFIG.get(club_prefix, {})
    slug  = cfg.get("tm_slug","")
    tm_id = cfg.get("tm_id","")
    if not slug or not tm_id:
        return ""
    return f"{TM_BASE}/{slug}/kader/verein/{tm_id}/saison_id/{TM_SEASON}/plus/1"


def scrape_tm_squad(club_prefix):
    url = tm_squad_url(club_prefix)
    if not url:
        print(f"[TM] Kein Config für '{club_prefix}'")
        return []
    cname = CLUB_CONFIG.get(club_prefix,{}).get("name",club_prefix)
    print(f"[TM] Scraping {cname} ...")
    try:
        resp = requests.get(url, headers=TM_HEADERS, timeout=20)
        resp.raise_for_status()
    except requests.HTTPError as e:
        code = e.response.status_code if e.response is not None else "?"
        print(f"[TM] HTTP {code} — evtl. User-Agent oder Proxy anpassen")
        return []
    except Exception as exc:
        print(f"[TM] Verbindungsfehler: {exc}")
        return []

    soup    = BeautifulSoup(resp.text, "html.parser")
    table   = soup.find("table", {"class": "items"})
    if not table:
        print(f"[TM] Keine Tabelle gefunden auf {url}")
        return []

    players = []
    for row in table.select("tbody tr"):
        cls = row.get("class", [])
        if not any(c in cls for c in ["odd","even"]):
            continue

        # Shirt number
        shirt_el = row.select_one("div.rn_nummer")
        shirt    = shirt_el.get_text(strip=True) if shirt_el else ""

        # Name + TM profile link
        name_el  = row.select_one("td.hauptlink a")
        if not name_el:
            continue
        full_name = name_el.get_text(strip=True)
        href      = name_el.get("href","")
        tm_m      = re.search(r"/spieler/(\d+)", href)
        if not tm_m:
            continue
        tm_id        = tm_m.group(1)
        tm_prof_url  = TM_BASE + href.split("?")[0]

        # Position
        pos_el   = row.select_one("td.posrela table td")
        position = pos_el.get_text(strip=True) if pos_el else ""

        # DOB + age — search all cells for the pattern
        dob_iso, age = "", ""
        for td in row.find_all("td"):
            txt = td.get_text(strip=True)
            dob_iso, age = parse_tm_dob(txt)
            if dob_iso:
                break

        # Nationalities
        flags = row.select("img.flaggenrahmen")
        nat   = flags[0].get("title","") if flags else ""
        nat2  = flags[1].get("title","") if len(flags)>1 else ""

        # Market value
        mv_el = row.select_one("td.rechts.hauptlink")
        mv    = mv_el.get_text(strip=True) if mv_el else ""

        # Player image
        img_el    = row.select_one("img.bilderrahmen-fixed")
        image_url = ""
        if img_el:
            image_url = img_el.get("data-src") or img_el.get("src","")

        players.append({
            "tm_id": tm_id, "club_prefix": club_prefix, "season": TM_SEASON,
            "full_name": full_name, "date_of_birth": dob_iso, "age": age,
            "nationality": nat, "nationality2": nat2, "position": position,
            "shirt_number": shirt, "market_value": mv,
            "tm_profile_url": tm_prof_url, "image_url": image_url,
        })

    print(f"[TM] {len(players)} Spieler gefunden für {cname}")
    return players


def refresh_tm_cache(club_prefix, force=False):
    """Refresh TM squad cache if stale. Returns True if scraped."""
    with get_db() as db:
        meta = db.execute(
            "SELECT last_scraped FROM tm_cache_meta WHERE club_prefix=? AND season=?",
            (club_prefix, TM_SEASON)
        ).fetchone()
    if not force and meta and meta["last_scraped"]:
        if datetime.utcnow() - datetime.fromisoformat(meta["last_scraped"]) \
                < timedelta(hours=TM_CACHE_H):
            return False

    players = scrape_tm_squad(club_prefix)
    if not players:
        return False

    with get_db() as db:
        for p in players:
            db.execute(
                "INSERT INTO tm_players "
                "(tm_id,club_prefix,season,full_name,date_of_birth,age,"
                " nationality,nationality2,position,shirt_number,market_value,"
                " tm_profile_url,image_url,scraped_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(tm_id,season) DO UPDATE SET "
                " full_name=excluded.full_name,"
                " date_of_birth=excluded.date_of_birth,"
                " age=excluded.age, position=excluded.position,"
                " shirt_number=excluded.shirt_number,"
                " market_value=excluded.market_value,"
                " image_url=excluded.image_url,"
                " scraped_at=excluded.scraped_at",
                (p["tm_id"],p["club_prefix"],p["season"],p["full_name"],
                 p["date_of_birth"],p["age"],p["nationality"],p["nationality2"],
                 p["position"],p["shirt_number"],p["market_value"],
                 p["tm_profile_url"],p["image_url"],
                 datetime.utcnow().isoformat())
            )
        db.execute(
            "INSERT INTO tm_cache_meta (club_prefix,season,last_scraped,player_count) "
            "VALUES (?,?,?,?) "
            "ON CONFLICT(club_prefix,season) DO UPDATE SET "
            " last_scraped=excluded.last_scraped, player_count=excluded.player_count",
            (club_prefix, TM_SEASON, datetime.utcnow().isoformat(), len(players))
        )
    _match_by_dob(club_prefix)
    _match_by_shirt(club_prefix)
    return True


def _match_by_dob(club_prefix):
    with get_db() as db:
        rows = db.execute(
            "SELECT spieler_id, date_of_birth FROM spieler_map "
            "WHERE club_prefix=? AND date_of_birth IS NOT NULL "
            "  AND date_of_birth!='' AND matched_by IN ('unmatched','shirt')",
            (club_prefix,)
        ).fetchall()
        for row in rows:
            tm = db.execute(
                "SELECT * FROM tm_players "
                "WHERE club_prefix=? AND season=? AND date_of_birth=?",
                (club_prefix, TM_SEASON, row["date_of_birth"])
            ).fetchone()
            if tm:
                db.execute(
                    "UPDATE spieler_map SET tm_id=?,full_name=?,matched_by='dob',"
                    "updated_at=datetime('now') WHERE spieler_id=?",
                    (tm["tm_id"], tm["full_name"], row["spieler_id"])
                )


def _match_by_shirt(club_prefix):
    with get_db() as db:
        rows = db.execute(
            "SELECT spieler_id FROM spieler_map "
            "WHERE club_prefix=? AND matched_by='unmatched'",
            (club_prefix,)
        ).fetchall()
        for row in rows:
            sid   = row["spieler_id"]
            shirt = sid.split("-")[-1].lstrip("0") if "-" in sid else ""
            if not shirt:
                continue
            tm = db.execute(
                "SELECT * FROM tm_players "
                "WHERE club_prefix=? AND season=? AND shirt_number=?",
                (club_prefix, TM_SEASON, shirt)
            ).fetchone()
            if tm:
                db.execute(
                    "UPDATE spieler_map SET tm_id=?,full_name=?,matched_by='shirt',"
                    "updated_at=datetime('now') WHERE spieler_id=?",
                    (tm["tm_id"], tm["full_name"], sid)
                )


def register_spieler(spieler_id, dob=""):
    prefix = spieler_id.split("-")[0] if "-" in spieler_id else spieler_id

    # Normalise DOB if in German format
    if dob and "." in dob:
        parts = dob.split(".")
        if len(parts) == 3:
            dob = f"{parts[2]}-{parts[1]}-{parts[0]}"

    with get_db() as db:
        existing = db.execute(
            "SELECT * FROM spieler_map WHERE spieler_id=?", (spieler_id,)
        ).fetchone()
        existing = dict(existing) if existing else None

    if not existing:
        with get_db() as db:
            db.execute(
                "INSERT INTO spieler_map "
                "(spieler_id,club_prefix,date_of_birth,matched_by) "
                "VALUES (?,?,?,'unmatched') ON CONFLICT(spieler_id) DO NOTHING",
                (spieler_id, prefix, dob or None)
            )
    elif dob and not (existing.get("date_of_birth") or ""):
        with get_db() as db:
            db.execute(
                "UPDATE spieler_map SET date_of_birth=? WHERE spieler_id=?",
                (dob, spieler_id)
            )

    # Ensure TM cache loaded
    refresh_tm_cache(prefix)

    # Immediate match attempt
    with get_db() as db:
        sm = db.execute(
            "SELECT * FROM spieler_map WHERE spieler_id=?", (spieler_id,)
        ).fetchone()
        if sm and sm["matched_by"] == "unmatched":
            dob_use = sm["date_of_birth"] or dob
            if dob_use:
                tm = db.execute(
                    "SELECT * FROM tm_players "
                    "WHERE club_prefix=? AND season=? AND date_of_birth=?",
                    (prefix, TM_SEASON, dob_use)
                ).fetchone()
                if tm:
                    db.execute(
                        "UPDATE spieler_map SET tm_id=?,full_name=?,matched_by='dob',"
                        "updated_at=datetime('now') WHERE spieler_id=?",
                        (tm["tm_id"], tm["full_name"], spieler_id)
                    )
                    return
            shirt = spieler_id.split("-")[-1].lstrip("0") if "-" in spieler_id else ""
            if shirt:
                tm = db.execute(
                    "SELECT * FROM tm_players "
                    "WHERE club_prefix=? AND season=? AND shirt_number=?",
                    (prefix, TM_SEASON, shirt)
                ).fetchone()
                if tm:
                    db.execute(
                        "UPDATE spieler_map SET tm_id=?,full_name=?,matched_by='shirt',"
                        "updated_at=datetime('now') WHERE spieler_id=?",
                        (tm["tm_id"], tm["full_name"], spieler_id)
                    )


def get_player_info(spieler_id):
    with get_db() as db:
        sm = db.execute(
            "SELECT * FROM spieler_map WHERE spieler_id=?", (spieler_id,)
        ).fetchone()
        tm = db.execute(
            "SELECT * FROM tm_players WHERE tm_id=? AND season=?",
            (sm["tm_id"], TM_SEASON)
        ).fetchone() if sm and sm["tm_id"] else None
    result = {
        "spieler_id":    spieler_id,
        "full_name":     (sm["full_name"] if sm else None) or spieler_id,
        "matched_by":    sm["matched_by"] if sm else "unmatched",
        "date_of_birth": sm["date_of_birth"] if sm else "",
        "tm_id":         sm["tm_id"] if sm else None,
    }
    if tm:
        for k in ("position","age","nationality","nationality2",
                  "shirt_number","market_value","tm_profile_url","image_url"):
            result[k] = tm[k]
    return result


# ---------------------------------------------------------------------------
#  REDCap helpers
# ---------------------------------------------------------------------------
def rc_post(token, **kwargs):
    try:
        data = {"token": token, "content": "record", "format": "json", "type": "flat"}
        data.update(kwargs)
        resp = requests.post(REDCAP_BASE_URL, timeout=20, data=data)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        print(f"[RC] Fehler: {exc}")
        return []


def rc83_get_players(club_prefix):
    if REDCAP_TOKEN_83 == "DEMO_TOKEN_83":
        demo = [
            ("BC-1817","1993-03-15"),("BC-2285","1997-08-22"),
            ("BC-2924","2000-11-05"),("BC-1231","1995-06-14"),
            ("BC-5168","1999-02-28"),("BC-3344","2001-09-03"),
            ("BC-4412","1996-12-11"),("BC-6677","1998-04-17"),
            ("BC-7890","2003-07-30"),("BC-1023","1992-01-08"),
            ("BC-4567","2002-05-25"),("BC-8901","1994-10-19"),
            ("BC-2345","2004-03-06"),("BC-6789","1991-08-29"),
            ("BC-3456","1997-11-22"),("BC-9012","2000-06-01"),
        ]
        return [{"bas_spieler_id": s, "bas_geburtsdatum": d} for s, d in demo]
    return rc_post(REDCAP_TOKEN_83, filterLogic=f'[bas_spieler_id] like "{club_prefix}-%"')


def rc84_get_records(club_prefix):
    if REDCAP_TOKEN_84 == "DEMO_TOKEN_84":
        p = club_prefix
        return [
            {"record_id":"7","spieler_id":f"{p}-1817","allg_verl_erkr":"1",
             "allg_verl_datum":"2024-09-16","allg_erkr_datum":"",
             "exakte_diagnose":"Innenbandriss Knie","verletzungskategorie":"Knieverletzung",
             "return_to_activity":"2024-10-01","return_to_sport":"2024-10-15",
             "return_to_play":"2024-10-20","status":"Complete",
             "saison":"2024/25","halbserie":"Hinrunde"},
            {"record_id":"219","spieler_id":f"{p}-1817","allg_verl_erkr":"1",
             "allg_verl_datum":"2024-12-03","allg_erkr_datum":"",
             "exakte_diagnose":"Muskelfaserriss Hamstrings",
             "verletzungskategorie":"Muskuläre Verletzung",
             "return_to_activity":"","return_to_sport":"","return_to_play":"",
             "status":"Incomplete","saison":"2024/25","halbserie":"Hinrunde"},
            {"record_id":"224","spieler_id":f"{p}-2285","allg_verl_erkr":"2",
             "allg_verl_datum":"","allg_erkr_datum":"2024-11-20",
             "exakte_diagnose":"Grippaler Infekt","verletzungskategorie":"",
             "return_to_activity":"2024-11-24","return_to_sport":"",
             "return_to_play":"2024-11-25","status":"Complete",
             "saison":"2024/25","halbserie":"Hinrunde"},
            {"record_id":"301","spieler_id":f"{p}-2924","allg_verl_erkr":"1",
             "allg_verl_datum":"2025-01-10","allg_erkr_datum":"",
             "exakte_diagnose":"Distorsion OSG rechts",
             "verletzungskategorie":"Verletzung des Fußes oder des OSG",
             "return_to_activity":"","return_to_sport":"","return_to_play":"",
             "status":"Incomplete","saison":"2024/25","halbserie":"Rückrunde"},
        ]
    return rc_post(REDCAP_TOKEN_84, filterLogic=f'[spieler_id] like "{club_prefix}-%"')


def format_date_de(iso):
    if not iso:
        return "–"
    try:
        p = iso.split("-")
        if len(p)==3: return f"{p[2]}.{p[1]}.{p[0]}"
    except Exception:
        pass
    return iso


def event_date(rec):
    typ = str(rec.get("allg_verl_erkr","0"))
    return rec.get("allg_verl_datum","") if typ=="1" \
        else rec.get("allg_erkr_datum","") if typ=="2" else ""


def event_label(rec):
    return {"1":"Verletzung","2":"Erkrankung"}.get(
        str(rec.get("allg_verl_erkr","0")), "Keine")


def survey_url(record_id):
    return (f"https://redcap-test.ukr.de/redcap_v14.6.0/surveys/"
            f"?s={REDCAP_SURVEY_HASH}&record={record_id}")


def new_entry_url(spieler_id):
    return (f"https://redcap-test.ukr.de/redcap_v14.6.0/surveys/"
            f"?s={REDCAP_SURVEY_HASH}&record=new"
            f"&field=spieler_id&value={spieler_id}")


# ---------------------------------------------------------------------------
#  Auth
# ---------------------------------------------------------------------------
def check_creds(u, p):
    acc = PHYSICIAN_ACCOUNTS.get(u)
    return bool(acc and secrets.compare_digest(acc[0], p))

def get_club(username):
    acc = PHYSICIAN_ACCOUNTS.get(username)
    return acc[1] if acc else ""

def login_required(f):
    @wraps(f)
    def d(*a,**kw):
        if "username" not in session: return redirect(url_for("login_page"))
        return f(*a,**kw)
    return d

def api_auth(f):
    @wraps(f)
    def d(*a,**kw):
        if "username" not in session:
            return jsonify(ok=False,error="Nicht angemeldet"),401
        return f(*a,**kw)
    return d

def me():    return session.get("username","")
def myclb(): return get_club(me())


# ---------------------------------------------------------------------------
#  Login HTML
# ---------------------------------------------------------------------------
LOGIN_HTML = """<!DOCTYPE html>
<html lang="de"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Bundesliga-Register</title>
<style>
:root{--bg:#fff;--bg2:#f4f4f2;--border:#d0d0cc;--text:#1a1a18;
  --red:#e3000f;--red-bg:#fff2f2;--red-bd:#ffcccc;--r:3px}
@media(prefers-color-scheme:dark){:root{--bg:#1a1a18;--bg2:#222220;
  --border:#3a3a38;--text:#e8e8e4;--red-bg:#2a0808;--red-bd:#7a2020}}
*{box-sizing:border-box;margin:0;padding:0;font-family:'Segoe UI',system-ui,sans-serif}
body{background:var(--bg2);display:flex;align-items:center;
  justify-content:center;min-height:100vh;color:var(--text)}
.card{background:var(--bg);border:1px solid var(--border);border-radius:6px;
  padding:40px 44px;width:100%;max-width:400px}
.logo{text-align:center;margin-bottom:26px}
.stripe{background:#e3000f;color:#fff;font-size:10px;font-weight:700;
  letter-spacing:.08em;padding:3px 10px;border-radius:var(--r);
  display:inline-block;text-transform:uppercase;margin-bottom:10px}
.title{font-size:18px;font-weight:700}
.sub{font-size:11px;color:#999;margin-top:3px}
.field{display:flex;flex-direction:column;gap:5px;margin-bottom:15px}
.field label{font-size:11px;font-weight:700;color:#888;
  text-transform:uppercase;letter-spacing:.05em}
.field input{padding:9px 12px;border:1px solid var(--border);border-radius:var(--r);
  background:var(--bg);color:var(--text);font-size:14px}
.field input:focus{outline:none;border-color:#e3000f}
.submit{width:100%;padding:10px;border-radius:var(--r);border:none;
  background:#e3000f;color:#fff;font-size:14px;font-weight:600;
  cursor:pointer;margin-top:4px}
.submit:hover{opacity:.88}
.err{background:var(--red-bg);color:var(--red);border:1px solid var(--red-bd);
  border-radius:var(--r);padding:9px 12px;font-size:13px;margin-bottom:14px}
.foot{font-size:11px;color:#aaa;text-align:center;margin-top:14px}
</style></head>
<body><div class="card">
  <div class="logo">
    <div class="stripe">Bundesliga-Register</div>
    <div class="title">Verletzungsportal</div>
    <div class="sub">für ausfall-relevante Verletzungen und Erkrankungen</div>
  </div>
  {% if error %}<div class="err">{{ error }}</div>{% endif %}
  <form method="POST" action="/login">
    <div class="field"><label>Benutzername</label>
      <input name="username" type="text" placeholder="z.B. dr.mueller"
             value="{{ username or '' }}" autocomplete="username" required autofocus>
    </div>
    <div class="field"><label>Passwort</label>
      <input name="password" type="password" autocomplete="current-password" required>
    </div>
    <button class="submit">Anmelden</button>
  </form>
  <div class="foot">Bei Problemen wenden Sie sich an den Administrator.</div>
</div></body></html>"""


def load_html():
    with open(os.path.join(os.path.dirname(__file__), "portal.html")) as f:
        return f.read()


# ---------------------------------------------------------------------------
#  Auth routes
# ---------------------------------------------------------------------------
@app.route("/login", methods=["GET","POST"])
def login_page():
    if request.method == "GET":
        if "username" in session: return redirect(url_for("index"))
        return render_template_string(LOGIN_HTML, error=None, username=None)
    u = request.form.get("username","").strip().lower()
    p = request.form.get("password","")
    if check_creds(u, p):
        session.permanent = True
        session["username"] = u
        return redirect(url_for("index"))
    return render_template_string(LOGIN_HTML,
        error="Benutzername oder Passwort falsch.", username=u)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login_page"))


# ---------------------------------------------------------------------------
#  Portal
# ---------------------------------------------------------------------------
@app.route("/")
@login_required
def index():
    club  = myclb()
    cname = CLUB_CONFIG.get(club,{}).get("name",club)
    return render_template_string(load_html(),
        current_user=me(), club_prefix=club, club_name=cname)


@app.route("/api/tree")
@api_auth
def api_tree():
    club = myclb()
    # PID 83 — player list + DOB
    base    = rc83_get_players(club)
    sid_dob = {}
    for r in base:
        sid = r.get("bas_spieler_id","")
        dob = r.get("bas_geburtsdatum","")
        if sid: sid_dob[sid] = dob

    # Refresh TM cache
    refresh_tm_cache(club)

    # Register all players from PID 83 (with DOB for matching)
    for sid, dob in sid_dob.items():
        register_spieler(sid, dob)

    # PID 84 — injury records
    reg     = rc84_get_records(club)
    rec_cnt = {}
    rec_hlb = {}
    for r in reg:
        sid = r.get("spieler_id","")
        if not sid: continue
        if str(r.get("allg_verl_erkr","0")) in ("1","2"):
            rec_cnt[sid] = rec_cnt.get(sid,0) + 1
        rec_hlb.setdefault(sid, set()).add(r.get("halbserie","Hinrunde"))
        if sid not in sid_dob:
            register_spieler(sid)

    all_sids = set(sid_dob.keys()) | set(rec_cnt.keys())
    players  = []
    for sid in sorted(all_sids):
        info = get_player_info(sid)
        players.append({
            "spieler_id":   sid,
            "full_name":    info["full_name"],
            "matched_by":   info["matched_by"],
            "record_count": rec_cnt.get(sid,0),
            "tm_id":        info.get("tm_id"),
        })
    players.sort(key=lambda x: x["full_name"])

    for p in players:
        rec_hlb.setdefault(p["spieler_id"],set()).add("Hinrunde")

    halbserien = []
    for halb in ["Hinrunde","Rückrunde"]:
        subset = [p for p in players if halb in rec_hlb.get(p["spieler_id"],set())]
        if subset:
            halbserien.append({"name":halb,"players":subset})

    return jsonify(
        club_prefix=club, club_name=CLUB_CONFIG.get(club,{}).get("name",club),
        season_display="2024/25", total_players=len(all_sids),
        total_records=len(reg), halbserien=halbserien,
    )


@app.route("/api/player/<path:spieler_id>")
@api_auth
def api_player(spieler_id):
    club = myclb()
    if not spieler_id.startswith(club+"-"): abort(403)
    info = get_player_info(spieler_id)
    all_recs = rc84_get_records(club)
    p_recs   = sorted(
        [r for r in all_recs if r.get("spieler_id","")==spieler_id],
        key=lambda r: event_date(r) or ""
    )
    for r in p_recs:
        r["event_date_de"]   = format_date_de(event_date(r))
        r["event_label"]     = event_label(r)
        r["rta_de"]          = format_date_de(r.get("return_to_activity",""))
        r["rts_de"]          = format_date_de(r.get("return_to_sport",""))
        r["rtp_de"]          = format_date_de(r.get("return_to_play",""))
        r["status_complete"] = "complete" in (r.get("status","").lower())
        r["redcap_url"]      = survey_url(r.get("record_id",""))
    return jsonify(
        spieler_id=spieler_id, full_name=info["full_name"],
        matched_by=info["matched_by"],
        date_of_birth=format_date_de(info.get("date_of_birth","")),
        age=info.get("age",""), shirt_number=info.get("shirt_number",""),
        position=info.get("position",""),
        nationality=info.get("nationality",""), nationality2=info.get("nationality2",""),
        market_value=info.get("market_value",""),
        tm_id=info.get("tm_id"), tm_profile_url=info.get("tm_profile_url",""),
        image_url=info.get("image_url",""),
        records=p_recs, new_entry_url=new_entry_url(spieler_id),
    )


@app.route("/api/assign", methods=["POST"])
@api_auth
def api_assign():
    data      = request.get_json()
    spieler_id = data.get("spieler_id","")
    full_name  = data.get("full_name","").strip()
    dob        = data.get("dob","").strip()
    if not spieler_id or not full_name:
        return jsonify(ok=False, error="spieler_id und full_name erforderlich")
    if not spieler_id.startswith(myclb()+"-"):
        return jsonify(ok=False, error="Keine Berechtigung"), 403
    prefix = spieler_id.split("-")[0]
    with get_db() as db:
        db.execute(
            "INSERT INTO spieler_map "
            "(spieler_id,club_prefix,full_name,date_of_birth,matched_by) "
            "VALUES (?,?,?,?,'admin') "
            "ON CONFLICT(spieler_id) DO UPDATE SET "
            " full_name=excluded.full_name,"
            " date_of_birth=COALESCE(excluded.date_of_birth,spieler_map.date_of_birth),"
            " matched_by='admin', updated_at=datetime('now')",
            (spieler_id, prefix, full_name, dob or None)
        )
    return jsonify(ok=True)


@app.route("/api/refresh-tm")
@api_auth
def api_refresh_tm():
    ok = refresh_tm_cache(myclb(), force=True)
    return jsonify(ok=ok, club=myclb(), url=tm_squad_url(myclb()))


if __name__ == "__main__":
    init_db()
    d83 = REDCAP_TOKEN_83 == "DEMO_TOKEN_83"
    d84 = REDCAP_TOKEN_84 == "DEMO_TOKEN_84"
    print("\n" + "="*66)
    print("  Bundesliga-Register — Verletzungsportal")
    print("  Browser:      http://localhost:5000")
    print(f"  PID 83:       {'DEMO' if d83 else 'LIVE'}")
    print(f"  PID 84:       {'DEMO' if d84 else 'LIVE'}")
    print(f"  Transfermarkt: saison_id/{TM_SEASON}/plus/1  (DOB + age view)")
    print(f"  Datenbank:    {DATABASE}")
    print()
    print("  Umgebungsvariablen:")
    print("    export REDCAP_TOKEN_83='...'")
    print("    export REDCAP_TOKEN_84='...'")
    print("    export REDCAP_SURVEY_HASH='...'")
    print()
    print("  Arzt-Zugänge:")
    for u,(pw,club) in PHYSICIAN_ACCOUNTS.items():
        print(f"    {u:22} → {CLUB_CONFIG.get(club,{}).get('name',club)}")
    print()
    print("  Beispiel-URLs (Transfermarkt /plus/1):")
    for pfx in list(CLUB_CONFIG)[:3]:
        print(f"    {pfx}: {tm_squad_url(pfx)}")
    print("="*66+"\n")
    app.run(debug=True, host="0.0.0.0", port=5000)
