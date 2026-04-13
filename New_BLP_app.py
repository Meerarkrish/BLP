from dotenv import load_dotenv
load_dotenv()
import os, sqlite3, secrets, re, time
from datetime import datetime, timedelta
from contextlib import contextmanager
from functools import wraps

import requests
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify, session, redirect, url_for, abort

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

REDCAP_BASE = "https://redcap-prod.ukr.de/redcap_v14.6.0/API/"
TOKEN83 = os.getenv("REDCAP_TOKEN_83")
TOKEN84 = os.getenv("REDCAP_TOKEN_84")
SURVEY = os.getenv("REDCAP_SURVEY_HASH")

TM_SEASON = "2024"
TM_CACHE_HOURS = 24

DATABASE = "prod.db"

if not TOKEN83 or not TOKEN84 or not SURVEY:
    raise RuntimeError("❌ Missing REDCap environment variables")

# ---------------------------------------------------------------------------
# APP
# ---------------------------------------------------------------------------

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", secrets.token_hex(32))
app.permanent_session_lifetime = timedelta(hours=8)

# ---------------------------------------------------------------------------
# CLUB CONFIG
# ---------------------------------------------------------------------------

CLUBS = {
    "FCB": {"name": "FC Bayern München", "slug": "fc-bayern-munchen", "id": "27"}
}

USERS = {
    "dr.mueller": ("Bundesliga#2024", "FCB")
}

# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

@contextmanager
def db():
    c = sqlite3.connect(DATABASE)
    c.row_factory = sqlite3.Row
    yield c
    c.commit()
    c.close()

def init_db():
    with db() as d:
        d.executescript("""
        CREATE TABLE IF NOT EXISTS tm_players (
            tm_id TEXT PRIMARY KEY,
            club TEXT,
            name TEXT,
            dob TEXT,
            shirt TEXT,
            scraped_at TEXT
        );

        CREATE TABLE IF NOT EXISTS spieler_map (
            spieler_id TEXT PRIMARY KEY,
            club TEXT,
            tm_id TEXT,
            name TEXT,
            dob TEXT,
            matched_by TEXT
        );

        CREATE TABLE IF NOT EXISTS cache (
            club TEXT PRIMARY KEY,
            last_update TEXT
        );
        """)

# ---------------------------------------------------------------------------
# TRANSFERMARKT
# ---------------------------------------------------------------------------

def tm_url(club):
    c = CLUBS[club]
    return f"https://www.transfermarkt.com/{c['slug']}/kader/verein/{c['id']}/saison_id/{TM_SEASON}/plus/1"

def parse_dob(txt):
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", txt)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return ""

def scrape_tm(club):
    url = tm_url(club)
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(r.text, "html.parser")

    players = []

    for row in soup.select("table.items tbody tr"):
        name_el = row.select_one("td.hauptlink a")
        if not name_el:
            continue

        name = name_el.text.strip()
        href = name_el["href"]
        tm_id = re.search(r"/spieler/(\d+)", href).group(1)

        dob = ""
        for td in row.find_all("td"):
            dob = parse_dob(td.text)
            if dob:
                break

        shirt_el = row.select_one("div.rn_nummer")
        shirt = shirt_el.text.strip() if shirt_el else ""

        players.append((tm_id, club, name, dob, shirt, datetime.utcnow().isoformat()))

    with db() as d:
        d.execute("DELETE FROM tm_players WHERE club=?", (club,))
        d.executemany("INSERT INTO tm_players VALUES (?,?,?,?,?,?)", players)
        d.execute("INSERT OR REPLACE INTO cache VALUES (?,?)", (club, datetime.utcnow().isoformat()))

# ---------------------------------------------------------------------------
# CACHE CONTROL
# ---------------------------------------------------------------------------

def ensure_tm(club):
    with db() as d:
        row = d.execute("SELECT last_update FROM cache WHERE club=?", (club,)).fetchone()

    if not row:
        scrape_tm(club)
        return

    last = datetime.fromisoformat(row["last_update"])
    if datetime.utcnow() - last > timedelta(hours=TM_CACHE_HOURS):
        scrape_tm(club)

# ---------------------------------------------------------------------------
# REDCAP
# ---------------------------------------------------------------------------

def rc(token, **kw):
    data = {"token": token, "content": "record", "format": "json"}
    data.update(kw)
    return requests.post(REDCAP_BASE, data=data).json()

def rc83(club):
    return rc(TOKEN83, filterLogic=f'[bas_spieler_id] like "{club}-%"')

def rc84(club):
    return rc(TOKEN84, filterLogic=f'[spieler_id] like "{club}-%"')

# ---------------------------------------------------------------------------
# MATCH ENGINE
# ---------------------------------------------------------------------------

def match_players(club):
    ensure_tm(club)

    base = rc83(club)

    with db() as d:
        tm = d.execute("SELECT * FROM tm_players WHERE club=?", (club,)).fetchall()

        for r in base:
            sid = r["bas_spieler_id"]
            dob = r["bas_geburtsdatum"]

            match = None

            # DOB match
            for p in tm:
                if p["dob"] == dob:
                    match = p
                    method = "dob"
                    break

            # fallback shirt
            if not match:
                shirt = sid.split("-")[-1].lstrip("0")
                for p in tm:
                    if p["shirt"] == shirt:
                        match = p
                        method = "shirt"
                        break

            d.execute("""
            INSERT OR REPLACE INTO spieler_map
            VALUES (?, ?, ?, ?, ?, ?)
            """, (
                sid,
                club,
                match["tm_id"] if match else None,
                match["name"] if match else sid,
                dob,
                method if match else "unmatched"
            ))

# ---------------------------------------------------------------------------
# AUTH
# ---------------------------------------------------------------------------

def login_required(f):
    @wraps(f)
    def w(*a, **kw):
        if "u" not in session:
            return redirect("/login")
        return f(*a, **kw)
    return w

# ---------------------------------------------------------------------------
# ROUTES
# ---------------------------------------------------------------------------

@app.route("/login", methods=["GET","POST"])
def login():
    if request.method == "POST":
        u = request.form["u"]
        p = request.form["p"]

        acc = USERS.get(u)
        if acc and acc[0] == p:
            session["u"] = u
            return redirect("/")

    return """<form method=post>
    <input name=u>
    <input name=p type=password>
    <button>Login</button></form>"""

@app.route("/")
@login_required
def index():
    return "✅ Production running"

@app.route("/sync")
@login_required
def sync():
    club = USERS[session["u"]][1]
    match_players(club)
    return "synced"

@app.route("/players")
@login_required
def players():
    club = USERS[session["u"]][1]
    with db() as d:
        rows = d.execute("SELECT * FROM spieler_map WHERE club=?", (club,)).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/records")
@login_required
def records():
    club = USERS[session["u"]][1]
    return jsonify(rc84(club))

# ---------------------------------------------------------------------------
# RUN
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    init_db()
    print("🚀 Production system ready")
    app.run(host="0.0.0.0", port=5000)