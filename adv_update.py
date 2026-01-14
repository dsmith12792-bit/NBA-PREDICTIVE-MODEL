# adv_update.py
# Cloud Run–ready NBA Advanced Stats updater for Google Sheets
#
# ✅ No browser OAuth
# ✅ Uses Cloud Run's Service Account via Application Default Credentials (ADC)
# ✅ Reads NBA_COMPLETED_GAMES (ESPN Game ID + date + teams)
# ✅ Maps ESPN -> NBA Game ID using scoreboardv2 (by date + home/away names)
# ✅ Writes NBA Game ID back into NBA_COMPLETED_GAMES (optional but helpful)
# ✅ Pulls boxscoretraditionalv2 + boxscoreadvancedv2
# ✅ Upserts rows into ADV_GAME_STATS (updates existing Game ID rows, else appends)
#
# REQUIRED:
# - Enable Google Sheets API in your GCP project
# - Deploy Cloud Run with a Service Account that has Sheets API access
# - Share the target Google Sheet with that Service Account email (Viewer/Editor)
#
# ENV VARS (Cloud Run):
# - GOOGLE_SHEET_ID                (required)
# - TAB_COMPLETED                  default: NBA_COMPLETED_GAMES
# - TAB_ADV                        default: ADV_GAME_STATS
# - LOOKBACK_DAYS                  default: 60
# - MAX_GAMES_PER_RUN              default: 12
# - REQ_TIMEOUT                    default: 45
# - MAX_TRIES                      default: 8
# - SLEEP_BASE                     default: 1.25

import os
import time
import random
import re
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple, Optional

import requests

import google.auth
from googleapiclient.discovery import build


# =========================
# CONFIG
# =========================
SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()
TAB_COMPLETED = os.environ.get("TAB_COMPLETED", "NBA_COMPLETED_GAMES").strip()
TAB_ADV = os.environ.get("TAB_ADV", "ADV_GAME_STATS").strip()

LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "60"))
MAX_GAMES_PER_RUN = int(os.environ.get("MAX_GAMES_PER_RUN", "12"))

REQ_TIMEOUT = int(os.environ.get("REQ_TIMEOUT", "45"))
MAX_TRIES = int(os.environ.get("MAX_TRIES", "8"))
SLEEP_BASE = float(os.environ.get("SLEEP_BASE", "1.25"))

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# NBA endpoints
URL_SCOREBOARD = "https://stats.nba.com/stats/scoreboardv2"
URL_TRAD = "https://stats.nba.com/stats/boxscoretraditionalv2"
URL_ADV = "https://stats.nba.com/stats/boxscoreadvancedv2"

NBA_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
    "Connection": "keep-alive",
}

# ADV_GAME_STATS expected columns (order matters for writing A:AB)
ADV_COLUMNS = [
    "Game Date",
    "Key",
    "Game ID",
    "Home Team",
    "Away Team",
    "Home Pts",
    "Away Pts",
    "Total Pts",
    "Home Poss",
    "Away Poss",
    "Poss Avg",
    "Home OffRtg",
    "Away OffRtg",
    "Home eFG%",
    "Away eFG%",
    "Home TS%",
    "Away TS%",
    "Home FTr",
    "Away FTr",
    "Home TOV%",
    "Away TOV%",
    "Home 3PAr",
    "Away 3PAr",
    "Home ORB%",
    "Away ORB%",
    "Status",
    "Last Attempt",
]

COMPLETED_EXPECTED = [
    "Team",
    "Opponent",
    "HomeAway",
    "Game Date",
    "ESPN Game ID",
    "NBA Game ID",
    "Points For",
    "Points Allowed",
]


# =========================
# SMALL UTILS
# =========================
def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def is_num(x: Any) -> bool:
    try:
        return x is not None and x != "" and float(x) == float(x)
    except Exception:
        return False


def jitter_sleep(i: int) -> None:
    time.sleep(SLEEP_BASE * (1.6 ** i) + random.uniform(0, 0.5))


def normalize_team_name(s: Any) -> str:
    t = str(s or "").strip().lower()
    t = t.replace("\u00a0", " ")
    t = re.sub(r"\s+", " ", t)

    # ESPN shorthand fixes
    t = t.replace("la clippers", "los angeles clippers")
    t = t.replace("la lakers", "los angeles lakers")

    return t


def format_mdy(dt: datetime) -> str:
    return dt.strftime("%m/%d/%Y")


def parse_sheet_date(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    s = str(value).strip()

    for fmt in (
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass

    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def build_key(game_dt: Optional[datetime], home_team: str, away_team: str) -> str:
    if not game_dt:
        return ""
    ymd = game_dt.strftime("%Y%m%d")
    return f"{ymd}|{normalize_team_name(home_team)}|{normalize_team_name(away_team)}"


def col_to_a1(col_idx_0: int) -> str:
    """0-based column index -> A1 letter(s)"""
    n = col_idx_0 + 1
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# =========================
# GOOGLE SHEETS (Cloud ADC)
# =========================
def sheets_service():
    if not SHEET_ID:
        raise RuntimeError("Missing GOOGLE_SHEET_ID env var")

    creds, _ = google.auth.default(scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def get_values(svc, tab: str, a1: str) -> List[List[Any]]:
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"{tab}!{a1}",
    ).execute()
    return resp.get("values", [])


def update_values(svc, tab: str, a1: str, values: List[List[Any]]) -> None:
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"{tab}!{a1}",
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()


def append_values(svc, tab: str, a1: str, values: List[List[Any]]) -> None:
    svc.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=f"{tab}!{a1}",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": values},
    ).execute()


def batch_update_cells(svc, updates: List[Tuple[str, List[List[Any]]]]) -> None:
    """updates: [(A1range, [[val]]), ...]"""
    if not updates:
        return
    data = [{"range": f"{SHEET_ID_RANGE}", "values": vals} for (SHEET_ID_RANGE, vals) in updates]


def ensure_headers(svc, tab: str, required: List[str]) -> Dict[str, int]:
    """
    Ensures required headers exist on row 1.
    Missing headers are appended at the end.
    Returns header->index map (0-based).
    """
    header = get_values(svc, tab, "1:1")
    if not header or not header[0]:
        raise RuntimeError(f"{tab} missing header row")

    headers = [str(h).strip() for h in header[0]]
    existing = {h: i for i, h in enumerate(headers) if h}

    missing = [h for h in required if h not in existing]
    if missing:
        # append missing headers
        start_col = len(headers)
        new_headers = headers + missing
        end_col = len(new_headers) - 1
        a1 = f"{col_to_a1(0)}1:{col_to_a1(end_col)}1"
        update_values(svc, tab, a1, [new_headers])
        headers = new_headers
        existing = {h: i for i, h in enumerate(headers) if h}

    return existing


# =========================
# NBA FETCH (robust)
# =========================
def nba_fetch(url: str, params: Dict[str, str]) -> Dict[str, Any]:
    last_err = None
    for i in range(MAX_TRIES):
        try:
            r = requests.get(url, headers=NBA_HEADERS, params=params, timeout=REQ_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            last_err = RuntimeError(f"NBA HTTP {r.status_code}: {r.text[:250]}")
        except Exception as e:
            last_err = e
        jitter_sleep(i)
    raise RuntimeError(f"NBA fetch failed after {MAX_TRIES} tries: {last_err}")


def parse_resultset(j: Dict[str, Any], name: str) -> Tuple[List[str], List[List[Any]]]:
    rs = j.get("resultSets")
    target = None
    if isinstance(rs, list):
        for item in rs:
            if item and item.get("name") == name:
                target = item
                break
    if not target:
        raise RuntimeError(f"Could not find {name} in resultSets")
    headers = target.get("headers")
    rows = target.get("rowSet")
    if not headers or rows is None:
        raise RuntimeError(f"{name} missing headers/rows")
    return headers, rows


def idx_map(headers: List[str]) -> Dict[str, int]:
    return {str(h).strip(): i for i, h in enumerate(headers)}


def pick_home_away(rows: List[List[Any]], hidx: Dict[str, int]) -> Tuple[List[Any], List[Any]]:
    mi = hidx.get("MATCHUP")
    if mi is None:
        return rows[0], rows[1] if len(rows) > 1 else rows[0]

    home = None
    away = None
    for r in rows:
        m = str(r[mi] or "")
        if " vs " in m:
            home = r
        if " @ " in m:
            away = r

    if home is None or away is None:
        if len(rows) >= 2:
            return rows[0], rows[1]
        return rows[0], rows[0]
    return home, away


# =========================
# COMPLETED GAMES
# =========================
def read_completed_index(svc) -> Dict[str, Any]:
    col = ensure_headers(svc, TAB_COMPLETED, COMPLETED_EXPECTED)

    data = get_values(svc, TAB_COMPLETED, "A2:ZZ")
    cutoff = datetime.now() - timedelta(days=LOOKBACK_DAYS)

    espn_to_info: Dict[str, Dict[str, Any]] = {}
    row_refs: List[Dict[str, Any]] = []

    for i, r in enumerate(data, start=2):  # actual sheet row number
        def getv(name: str) -> Any:
            idx = col[name]
            return r[idx] if idx < len(r) else ""

        team = getv("Team")
        opp = getv("Opponent")
        homeaway = str(getv("HomeAway") or "").strip().lower()
        gdate_raw = getv("Game Date")
        espn_id = str(getv("ESPN Game ID") or "").strip()
        nba_id_existing = str(getv("NBA Game ID") or "").strip()

        gdt = parse_sheet_date(gdate_raw)
        if not espn_id or not gdt:
            continue
        if gdt < cutoff:
            continue

        row_refs.append({"rownum": i, "espn_id": espn_id, "nba_id_existing": nba_id_existing})

        # define game identity from HOME row
        if espn_id not in espn_to_info and homeaway == "home":
            espn_to_info[espn_id] = {"date": gdt, "home": str(team), "away": str(opp)}

    return {"col": col, "espn_to_info": espn_to_info, "row_refs": row_refs}


# =========================
# ESPN -> NBA mapping via ScoreboardV2
# =========================
def build_scoreboard_games_for_date(game_date: datetime) -> List[Dict[str, str]]:
    params = {"GameDate": format_mdy(game_date), "LeagueID": "00", "DayOffset": "0"}
    j = nba_fetch(URL_SCOREBOARD, params)

    gh_headers, gh_rows = parse_resultset(j, "GameHeader")
    ls_headers, ls_rows = parse_resultset(j, "LineScore")

    gh = idx_map(gh_headers)
    ls = idx_map(ls_headers)

    # TEAM_ID -> "City Nickname"
    team_id_to_name: Dict[str, str] = {}
    for r in ls_rows:
        tid = str(r[ls["TEAM_ID"]]) if "TEAM_ID" in ls else ""
        city = str(r[ls.get("TEAM_CITY_NAME", -1)]) if ls.get("TEAM_CITY_NAME") is not None else ""
        nick = str(r[ls.get("TEAM_NICKNAME", -1)]) if ls.get("TEAM_NICKNAME") is not None else ""
        full = f"{city} {nick}".strip()
        if tid and full:
            team_id_to_name[tid] = full

    games: List[Dict[str, str]] = []
    for r in gh_rows:
        game_id = str(r[gh["GAME_ID"]]).strip()
        home_tid = str(r[gh["HOME_TEAM_ID"]]).strip()
        away_tid = str(r[gh["VISITOR_TEAM_ID"]]).strip()

        home_name = team_id_to_name.get(home_tid, "")
        away_name = team_id_to_name.get(away_tid, "")

        if game_id and home_name and away_name:
            games.append({"nba_game_id": game_id, "home": home_name, "away": away_name})

    return games


def map_espn_to_nba(espn_to_info: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    date_to_list: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {}
    for espn_id, info in espn_to_info.items():
        dkey = info["date"].strftime("%Y-%m-%d")
        date_to_list.setdefault(dkey, []).append((espn_id, info))

    out: Dict[str, str] = {}

    for dkey, games in date_to_list.items():
        dt = games[0][1]["date"]
        dt_day = datetime(dt.year, dt.month, dt.day)

        nba_games = build_scoreboard_games_for_date(dt_day)

        lookup: Dict[str, str] = {}
        for g in nba_games:
            h = normalize_team_name(g["home"])
            a = normalize_team_name(g["away"])
            lookup[f"{h}|{a}"] = g["nba_game_id"]

        for espn_id, info in games:
            h = normalize_team_name(info["home"])
            a = normalize_team_name(info["away"])
            nba_id = lookup.get(f"{h}|{a}", "")
            if nba_id:
                out[espn_id] = nba_id

        time.sleep(0.7 + random.uniform(0, 0.4))

    return out


# =========================
# ADV sheet index
# =========================
def read_adv_index(svc) -> Dict[str, Any]:
    col = ensure_headers(svc, TAB_ADV, ADV_COLUMNS)
    data = get_values(svc, TAB_ADV, "A2:ZZ")

    gid_idx = col["Game ID"]
    status_idx = col["Status"]
    key_idx = col["Key"]

    gid_to_row: Dict[str, int] = {}
    gid_to_status: Dict[str, str] = {}
    gid_to_key: Dict[str, str] = {}

    for i, r in enumerate(data, start=2):
        gid = str(r[gid_idx]).strip() if gid_idx < len(r) else ""
        if not gid:
            continue
        gid_to_row[gid] = i
        gid_to_status[gid] = str(r[status_idx]).strip().upper() if status_idx < len(r) else ""
        gid_to_key[gid] = str(r[key_idx]).strip() if key_idx < len(r) else ""

    return {"col": col, "gid_to_row": gid_to_row, "gid_to_status": gid_to_status, "gid_to_key": gid_to_key}


# =========================
# Build ADV row from NBA API
# =========================
def fetch_adv_row(nba_game_id: str, game_dt_hint: Optional[datetime], existing_key: str) -> List[Any]:
    params = {
        "GameID": nba_game_id,
        "StartPeriod": "0",
        "EndPeriod": "10",
        "StartRange": "0",
        "EndRange": "28800",
        "RangeType": "0",
    }

    trad = nba_fetch(URL_TRAD, params)
    adv = nba_fetch(URL_ADV, params)

    trad_headers, trad_rows = parse_resultset(trad, "TeamStats")
    adv_headers, adv_rows = parse_resultset(adv, "TeamStats")

    tix = idx_map(trad_headers)
    aix = idx_map(adv_headers)

    home_adv, away_adv = pick_home_away(adv_rows, aix)
    home_trad, away_trad = pick_home_away(trad_rows, tix)

    def team_full(row, idx) -> str:
        city = ""
        name = ""
        if idx.get("TEAM_CITY") is not None:
            city = str(row[idx["TEAM_CITY"]] or "")
        elif idx.get("TEAM_CITY_NAME") is not None:
            city = str(row[idx["TEAM_CITY_NAME"]] or "")
        if idx.get("TEAM_NAME") is not None:
            name = str(row[idx["TEAM_NAME"]] or "")
        full = f"{city} {name}".strip()
        return full if full else str(name or city or "").strip()

    home_team = team_full(home_adv, aix)
    away_team = team_full(away_adv, aix)

    home_pts = home_trad[tix.get("PTS")]
    away_pts = away_trad[tix.get("PTS")]
    total_pts = (float(home_pts) + float(away_pts)) if is_num(home_pts) and is_num(away_pts) else ""

    def h(field: str):
        i = aix.get(field)
        return home_adv[i] if i is not None else ""

    def a(field: str):
        i = aix.get(field)
        return away_adv[i] if i is not None else ""

    home_poss = h("POSS")
    away_poss = a("POSS")
    poss_avg = ((float(home_poss) + float(away_poss)) / 2) if is_num(home_poss) and is_num(away_poss) else ""

    home_off = h("OFF_RATING")
    away_off = a("OFF_RATING")

    home_efg = h("EFG_PCT")
    away_efg = a("EFG_PCT")
    home_ts = h("TS_PCT")
    away_ts = a("TS_PCT")
    home_ftr = h("FTA_RATE")
    away_ftr = a("FTA_RATE")
    home_tov = h("TM_TOV_PCT")
    away_tov = a("TM_TOV_PCT")
    home_3par = h("FG3A_RATE")
    away_3par = a("FG3A_RATE")
    home_orb = h("OREB_PCT")
    away_orb = a("OREB_PCT")

    game_date_value = ""
    if game_dt_hint:
        game_date_value = game_dt_hint.strftime("%m/%d/%Y %H:%M")

    key_value = existing_key.strip() if existing_key else build_key(game_dt_hint, home_team, away_team)

    row = [
        game_date_value,     # Game Date
        key_value,           # Key
        str(nba_game_id),    # Game ID
        str(home_team),      # Home Team
        str(away_team),      # Away Team
        home_pts,            # Home Pts
        away_pts,            # Away Pts
        total_pts,           # Total Pts
        home_poss,           # Home Poss
        away_poss,           # Away Poss
        poss_avg,            # Poss Avg
        home_off,            # Home OffRtg
        away_off,            # Away OffRtg
        home_efg,            # Home eFG%
        away_efg,            # Away eFG%
        home_ts,             # Home TS%
        away_ts,             # Away TS%
        home_ftr,            # Home FTr
        away_ftr,            # Away FTr
        home_tov,            # Home TOV%
        away_tov,            # Away TOV%
        home_3par,           # Home 3PAr
        away_3par,           # Away 3PAr
        home_orb,            # Home ORB%
        away_orb,            # Away ORB%
        "OK",                # Status
        now_iso(),           # Last Attempt
    ]

    if not is_num(home_pts) or not is_num(away_pts):
        row[ADV_COLUMNS.index("Status")] = "ERR: missing points"

    return row


# =========================
# MAIN
# =========================
def main():
    svc = sheets_service()

    # 1) Read completed games (home rows define matchup identity)
    completed = read_completed_index(svc)
    espn_to_info = completed["espn_to_info"]
    row_refs = completed["row_refs"]
    col_completed = completed["col"]

    if not espn_to_info:
        print("No recent completed games found in lookback window.")
        return

    # 2) Map ESPN -> NBA Game ID using scoreboard per date
    espn_to_nba = map_espn_to_nba(espn_to_info)
    if not espn_to_nba:
        print("No ESPN->NBA mappings found (lookback window).")
        return

    # 3) Write NBA Game ID back into Completed Games tab (best effort)
    nba_id_col_idx = col_completed["NBA Game ID"]  # 0-based
    nba_id_col_letter = col_to_a1(nba_id_col_idx)

    batch_data = []
    for ref in row_refs:
        espn_id = ref["espn_id"]
        existing = str(ref["nba_id_existing"] or "").strip()
        nba_id = espn_to_nba.get(espn_id, "")
        if nba_id and nba_id != existing:
            rownum = ref["rownum"]
            rng = f"{TAB_COMPLETED}!{nba_id_col_letter}{rownum}"
            batch_data.append({"range": rng, "values": [[nba_id]]})

    if batch_data:
        # send in chunks
        for i in range(0, len(batch_data), 200):
            chunk = batch_data[i:i + 200]
            svc.spreadsheets().values().batchUpdate(
                spreadsheetId=SHEET_ID,
                body={"valueInputOption": "USER_ENTERED", "data": chunk},
            ).execute()
        print(f"✅ Updated NBA Game ID in {TAB_COMPLETED}: {len(batch_data)} cells")
    else:
        print("Completed Games NBA IDs already up to date (or nothing to write).")

    # 4) Read ADV index so we can upsert by Game ID
    adv = read_adv_index(svc)
    gid_to_row = adv["gid_to_row"]
    gid_to_status = adv["gid_to_status"]
    gid_to_key = adv["gid_to_key"]

    # 5) Build list of NBA games to process (newest first)
    nba_games: List[Tuple[str, datetime]] = []
    for espn_id, info in espn_to_info.items():
        nba_id = espn_to_nba.get(espn_id, "")
        if nba_id:
            nba_games.append((nba_id, info["date"]))

    nba_games.sort(key=lambda x: x[1], reverse=True)

    processed = 0
    updates: List[Tuple[int, List[Any]]] = []
    appends: List[List[Any]] = []

    for nba_id, dt_hint in nba_games:
        if processed >= MAX_GAMES_PER_RUN:
            break

        existing_row = gid_to_row.get(nba_id)
        existing_status = gid_to_status.get(nba_id, "")
        existing_key = gid_to_key.get(nba_id, "")

        # Skip rows already OK
        if existing_row and existing_status == "OK":
            continue

        processed += 1

        try:
            rowvals = fetch_adv_row(nba_id, dt_hint, existing_key)
        except Exception as e:
            rowvals = [""] * len(ADV_COLUMNS)
            rowvals[ADV_COLUMNS.index("Game ID")] = str(nba_id)
            rowvals[ADV_COLUMNS.index("Status")] = f"ERR: {str(e)[:160]}"
            rowvals[ADV_COLUMNS.index("Last Attempt")] = now_iso()

        if existing_row:
            updates.append((existing_row, rowvals))
        else:
            appends.append(rowvals)

        time.sleep(0.9 + random.uniform(0, 0.6))  # be polite

    # 6) Apply updates/appends
    # Determine A1 width from ADV_COLUMNS count
    last_col_letter = col_to_a1(len(ADV_COLUMNS) - 1)

    if updates:
        updates.sort(key=lambda x: x[0])
        for rownum, vals in updates:
            a1 = f"A{rownum}:{last_col_letter}{rownum}"
            update_values(svc, TAB_ADV, a1, [vals])

    if appends:
        append_values(svc, TAB_ADV, f"A:{last_col_letter}", appends)

    print(f"Done. processed={processed} updates={len(updates)} appends={len(appends)}")


if __name__ == "__main__":
    main()
