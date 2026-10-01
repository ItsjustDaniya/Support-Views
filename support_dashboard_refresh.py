"""
Support Dashboard refresh
=========================
Pulls every Support saved question from Metabase, reads the Groomers Google
Sheet, and writes everything into ONE output Google Sheet that Looker Studio
reads.

Output tabs
-----------
  Student_360        one row per student: profile + progress + attendance +
                     projects + certificates + grooming + referrals + mocks +
                     payment + access + certificate eligibility + module
                     contest + placement (company / date / last groomer) +
                     Groomers-sheet fields, plus a computed "pending_actions"
                     column (#6)
  MB_<name>          raw copy of each Metabase card (one tab per card)
  Groomers           cleaned copy of the Groomers tab
  _Refresh_Log       one line per run (row counts, errors)

Environment (GitHub secrets – the only two inputs)
----------------------------
  METABASE_API_KEY              Metabase API key
  GOOGLE_SERVICE_ACCOUNT_JSON   service-account JSON text, or a path to the file

All card ids and sheet ids are set in CONFIG below.

The service account must have Editor on the output sheet and Viewer on the
Groomers sheet.
"""

from __future__ import annotations

import io
import json
import os
import re
import sys
import time
import traceback
from datetime import datetime, timezone, timedelta

import gspread
import pandas as pd
import requests
from google.oauth2.service_account import Credentials

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
METABASE_URL = "https://metabase-lierhfgoeiwhr.newtonschool.co"

GROOMERS_SHEET_ID = "13HWMhfMX3i5qsDCEYnQD1h1iFL-ScoNz0HQSgd4CIks"
GROOMERS_TAB = "Groomers"

# output sheet Looker Studio reads
OUTPUT_SHEET_ID = "1K8QzqBO8PQKMnyifS5SgIbYgphYs_UZuyN6_2UHOTak"

IST = timezone(timedelta(hours=5, minutes=30))

# Metabase saved questions (Support Analytics dashboard #681) -> output tab
# A card with id 0 is skipped (not saved on Metabase yet).
CARDS: dict[str, int] = {
    "Student_Details":  13073,   # Support – Student Details
    "Upcoming_Batches": 13074,   # Upcoming batches - Support      (SQL: 02_upcoming_batches_v2)
    "Learning":         13075,   # Learning Progress
    "Attendance":       13076,   # Support - Attendance
    "Certificates":     13077,   # certificate updates
    "Projects":         13078,   # Project Completion - support
    "Grooming":         13079,   # Support - grooming sessions
    "Payments":         13080,   # Support - payment fee
    "Profile":          13081,   # User Profiles
    "Portal_Placement": 13082,   # Placement status                (SQL: 16_placement_status_v2)
    "Access_Revoked":   13083,   # Portal access - revoked
    "Mocks":            13084,   # Mock Interviews - support
    "Referrals":        13086,   # Referral Status - Support
    # new cards – put the ids here once saved
    "Cert_Eligibility": 0,       # Certificate Eligibility - Support (07_certificate_eligibility)
    "Module_Contest":   0,       # Module Contest - Support          (24_module_contest)
    "Track_View":       0,       # Student Track View - Support      (25_student_track_view)
}

# Groomers-sheet columns carried into Student_360 (renamed on the right)
GROOMER_COLS = {
    "Phase": "phase",
    "Enrolled Status": "sheet_enrolled_status",
    "Type of Experience": "type_of_experience",
    "Resume/Profile Filled": "resume_profile_filled",
    "Recommended date": "pi_recommended_on",
    "Recommended Pool (PI)": "pi_recommended_pool",
    "Times Recommended": "times_recommended",
    "Status": "picking_status",
    "Picked Date": "picked_on",
    "Grooming Pool (Picked)": "grooming_pool",
    "Groomer Name": "groomer",
    "New Groomer": "new_groomer",
    "Success Manager": "success_manager",
    "Student Flags": "student_flags",
    "Return to PI Date": "returned_to_pi_on",
    "Return Reason": "return_reason",
    "Times Flagged (Returned)": "times_returned",
    "Grooming Age (Days)": "grooming_age_days",
    "Grooming Level": "grooming_level",
    "PR": "pr",
    "Date of PR": "pr_date",
    "PR_Status": "pr_status_flag",
    "PR Age": "pr_age",
    "PR TAT": "pr_tat",
    "Graduation Pool": "graduation_pool",
    "Resume Cleared": "resume_cleared",
    "Supply-Demand Pool": "supply_demand_pool",
    "Placed": "placed",
    "Placement Month": "placement_month",
    "LPA": "lpa",
    "Cool Off": "cool_off",
    "Debarred Date": "debarred_on",
    "Debarred Flag": "debarred_flag",
    "Interview Pending NEW": "interview_pending_with",
    "No. of interview intake": "interview_intake_count",
    "Form not filled": "form_not_filled",
    "REPORT LINK": "grooming_report_link",
}
GROOMER_DATE_COLS = ["Recommended date", "Picked Date", "Return to PI Date",
                     "Date of PR", "Placement Month", "Debarred Date"]

SHEET_ERRORS = {"#N/A", "#REF!", "#NUM!", "#VALUE!", "#DIV/0!", "#NAME?", "#ERROR!", "NA", "N/A"}
WRITE_CHUNK_ROWS = 5000


def log(msg: str) -> None:
    print(f"[{datetime.now(IST):%H:%M:%S}] {msg}", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# METABASE
# ─────────────────────────────────────────────────────────────────────────────
class Metabase:
    def __init__(self, url: str, api_key: str):
        self.url = url.rstrip("/")
        self.s = requests.Session()
        self.s.headers.update({"x-api-key": api_key})

    def _req(self, method: str, path: str, **kw) -> requests.Response:
        for attempt in range(4):
            try:
                r = self.s.request(method, self.url + path, timeout=600, **kw)
                if r.status_code in (502, 503, 504):
                    raise requests.HTTPError(f"{r.status_code} gateway", response=r)
                r.raise_for_status()
                return r
            except requests.RequestException as e:
                if attempt == 3:
                    raise
                wait = 15 * (attempt + 1)
                log(f"  retry {attempt + 1} for {path} in {wait}s ({e})")
                time.sleep(wait)
        raise RuntimeError("unreachable")

    def card_df(self, card_id: int) -> pd.DataFrame:
        r = self._req("POST", f"/api/card/{card_id}/query/csv",
                      data={"parameters": "[]", "format_rows": "false"})
        text = r.content.decode("utf-8-sig")
        if not text.strip():
            return pd.DataFrame()
        return pd.read_csv(io.StringIO(text), dtype=str, keep_default_na=False)


# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────────────────────────────────────────
def gsheets_client() -> gspread.Client:
    raw = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].strip()
    info = json.loads(raw) if raw.startswith("{") else json.load(open(raw))
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets",
                      "https://www.googleapis.com/auth/drive.readonly"])
    return gspread.authorize(creds)


def read_tab(gc: gspread.Client, sheet_id: str, tab: str) -> pd.DataFrame:
    values = gc.open_by_key(sheet_id).worksheet(tab).get_all_values()
    if not values:
        return pd.DataFrame()
    header = dedupe_headers(values[0])
    width = len(header)
    rows = [(r + [""] * width)[:width] for r in values[1:]]
    return pd.DataFrame(rows, columns=header)


def dedupe_headers(cols: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    out = []
    for i, c in enumerate(cols):
        c = c.strip() or f"col_{i + 1}"
        if c in seen:
            seen[c] += 1
            c = f"{c}_{seen[c]}"
        else:
            seen[c] = 0
        out.append(c)
    return out


def write_tab(book: gspread.Spreadsheet, tab: str, df: pd.DataFrame) -> None:
    df = df.fillna("").astype(str)
    rows = [df.columns.tolist()] + df.values.tolist()
    n_rows, n_cols = len(rows), max(len(df.columns), 1)
    try:
        ws = book.worksheet(tab)
        ws.clear()
        if ws.row_count < n_rows or ws.col_count < n_cols:
            ws.resize(rows=max(n_rows, 2), cols=n_cols)
    except gspread.WorksheetNotFound:
        ws = book.add_worksheet(title=tab, rows=max(n_rows, 2), cols=n_cols)
    for start in range(0, n_rows, WRITE_CHUNK_ROWS):
        chunk = rows[start:start + WRITE_CHUNK_ROWS]
        ws.update(values=chunk, range_name=f"A{start + 1}", value_input_option="RAW")
    # trim leftover rows from a longer previous run
    if ws.row_count > n_rows + 1:
        ws.resize(rows=max(n_rows, 2))


def append_log(book: gspread.Spreadsheet, row: list[str]) -> None:
    try:
        ws = book.worksheet("_Refresh_Log")
    except gspread.WorksheetNotFound:
        ws = book.add_worksheet(title="_Refresh_Log", rows=2, cols=len(row))
        ws.update(values=[["run_at_ist", "status", "seconds", "rows_student_360",
                           "card_rows", "errors"]], range_name="A1")
    ws.append_row(row, value_input_option="RAW")


# ─────────────────────────────────────────────────────────────────────────────
# CLEANING
# ─────────────────────────────────────────────────────────────────────────────
ISO_TS = re.compile(r"^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(\.\d+)?([+-]\d{2}:\d{2}|Z)?$")


def iso_to_looker(v: str) -> str:
    """Metabase exports 2026-09-28T00:00:00+05:30 – Looker can't read that as a date."""
    m = ISO_TS.match(v)
    if not m:
        return v
    if m.group(4) and m.group(4) not in ("+05:30",):
        ts = pd.Timestamp(v).tz_convert(IST)
        return ts.strftime("%Y-%m-%d") if ts.strftime("%H:%M:%S") == "00:00:00" else ts.strftime("%Y-%m-%d %H:%M:%S")
    return m.group(1) if m.group(2) == "00:00:00" else f"{m.group(1)} {m.group(2)}"


def clean_card(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df.apply(lambda col: col.map(lambda v: iso_to_looker(v) if isinstance(v, str) else v))


def clean_sheet(df: pd.DataFrame, id_col: str, date_cols: list[str] | None = None) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.apply(lambda col: col.map(lambda v: "" if str(v).strip() in SHEET_ERRORS else str(v).strip()))
    df = df.rename(columns={id_col: "user_id"})
    df = df[df["user_id"].str.fullmatch(r"\d+")].copy()        # drops stray header rows / blanks
    df = df.drop_duplicates("user_id", keep="first")
    for c in date_cols or []:
        if c in df.columns:
            parsed = pd.to_datetime(df[c], format="%d/%m/%Y", errors="coerce")
            df[c] = parsed.dt.strftime("%Y-%m-%d").where(parsed.notna(), df[c])
    return df


def num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


# ─────────────────────────────────────────────────────────────────────────────
# STUDENT 360
# ─────────────────────────────────────────────────────────────────────────────
def per_user(df: pd.DataFrame | None, cols: list[str], sort_col: str | None = None) -> pd.DataFrame:
    """Keep one row per user (first after sorting desc by sort_col) with the chosen columns."""
    if df is None or df.empty or "user_id" not in df.columns:
        return pd.DataFrame(columns=["user_id"] + cols)
    d = df.copy()
    if sort_col and sort_col in d.columns:
        d = d.sort_values(sort_col, ascending=False, kind="stable")
    keep = ["user_id"] + [c for c in cols if c in d.columns]
    return d.drop_duplicates("user_id")[keep]


def build_student_360(cards: dict[str, pd.DataFrame], groomers: pd.DataFrame) -> pd.DataFrame:
    # base: every DS student the portal knows about (profile card), plus anyone
    # only present in the Groomers sheet
    base = per_user(cards.get("Profile"), [
        "student_name", "username", "email", "phone", "program_batch", "enrolment_status",
        "account_active", "last_login_on", "days_since_last_login", "current_city",
        "college", "graduation_year", "resume_approved", "resume_link", "linkedin_url"])
    if not groomers.empty:
        extra = groomers.loc[~groomers["user_id"].isin(base["user_id"]),
                             ["user_id", "Student_name", "Batch", "Email", "Phone Number"]]
        extra = extra.rename(columns={"Student_name": "student_name", "Batch": "program_batch",
                                      "Email": "email", "Phone Number": "phone"})
        base = pd.concat([base, extra], ignore_index=True)

    out = base

    # learning progress – one row per track -> per student
    lp = cards.get("Learning")
    if lp is not None and not lp.empty:
        lp = lp.assign(pct=num(lp["assignment_completion_pct"]))
        g = lp.groupby("user_id")
        agg = pd.DataFrame({
            "assignment_completion_pct": g["pct"].mean().round(1),
            "progress_by_track": g.apply(lambda x: " | ".join(
                f"{t}: {s}" for t, s in zip(x["track"], x["progress_stage"])), include_groups=False),
            "last_activity_on": g["last_activity_on"].max(),
            "program_labels": g["program_labels"].first(),
        }).reset_index()
        out = out.merge(agg, on="user_id", how="left")

    # attendance
    at = cards.get("Attendance")
    if at is not None and not at.empty:
        at = at.assign(pct=num(at["overall_attendance_pct"]), live=num(at["live_attendance_pct"]))
        agg = at.groupby("user_id").agg(overall_attendance_pct=("pct", "mean"),
                                        live_attendance_pct=("live", "mean"),
                                        last_live_attended_on=("last_live_attended_on", "max")).round(1).reset_index()
        agg["attendance_band"] = pd.cut(agg["overall_attendance_pct"], [-1, 50, 75, 101],
                                        labels=["Low (<50%)", "At risk (50-75%)", "Good (75%+)"], right=False).astype(str)
        agg.loc[agg["overall_attendance_pct"].isna(), "attendance_band"] = "No lectures yet"
        out = out.merge(agg, on="user_id", how="left")

    # projects
    pj = cards.get("Projects")
    if pj is not None and not pj.empty:
        g = pj.groupby("user_id")
        agg = pd.DataFrame({
            "projects_released": g.size(),
            "projects_cleared": g["project_status"].apply(lambda s: (s == "Cleared").sum()),
            "projects_pending": g.apply(lambda x: ", ".join(
                x.loc[x["project_status"] != "Cleared", "project"].astype(str)), include_groups=False),
        }).reset_index()
        agg["project_clearance_pct"] = (100 * agg["projects_cleared"] / agg["projects_released"]).round(1)
        out = out.merge(agg, on="user_id", how="left")

    # certificates
    ce = cards.get("Certificates")
    if ce is not None and not ce.empty:
        rel = ce[ce["certificate_status"] == "Released"]
        agg = rel.groupby("user_id").agg(
            certificates_released=("certificate", lambda s: ", ".join(sorted(set(s)))),
            last_certificate_on=("released_on", "max")).reset_index()
        out = out.merge(agg, on="user_id", how="left")

    # per-student cards (already one row per student, or keep the latest)
    out = out.merge(per_user(cards.get("Grooming"), [
        "latest_groomer_on_portal", "sessions_booked", "sessions_conducted", "sessions_upcoming",
        "student_no_shows", "groomer_no_shows", "grooming_mocks_done", "last_conducted_at",
        "next_session_at", "days_since_last_session"]), on="user_id", how="left")
    out = out.merge(per_user(cards.get("Referrals"), [
        "referrals_received", "companies_referred_to", "in_process", "shortlisted_or_ahead",
        "rejected_by_company", "process_stopped", "student_dropped", "selected_or_placed",
        "last_referral_on", "latest_company", "latest_company_status", "latest_round",
        "most_common_rejection_reason"]), on="user_id", how="left")
    out = out.merge(per_user(cards.get("Mocks"), [
        "ai_mocks_taken", "last_ai_mock_on", "avg_ai_mock_rating", "mock_tokens", "mocks_booked",
        "unused_valid_tokens", "next_token_expiry", "can_book_now"]), on="user_id", how="left")
    out = out.merge(per_user(cards.get("Portal_Placement"), [
        "portal_placement_status", "portal_pr_marked_on", "placement_status", "placed_company",
        "placed_on", "ctc", "offer_status", "offers_count", "last_grooming_on", "last_groomer",
        "last_grooming_type", "days_since_last_grooming", "npr_till", "number_of_no_shows"]),
        on="user_id", how="left")

    # certificate eligibility (80% assignments, 80% attendance, project >= 8,
    # module contest >= 65 on all 3 stacks)
    out = out.merge(per_user(cards.get("Cert_Eligibility"), [
        "certificate_eligibility", "stacks_done", "spreadsheets_status", "sql_status",
        "power_bi_status", "pending_items"]).rename(columns={"pending_items": "certificate_pending"}),
        on="user_id", how="left")

    # module contest – one row per track -> one line per student
    mc = cards.get("Module_Contest")
    if mc is not None and not mc.empty:
        mc = mc.copy()
        mc["line"] = mc.apply(lambda x: f"{x['track']}: {x['contest_result']}"
                              + (f" ({x['best_contest_score']}%)" if str(x.get("best_contest_score", "")) not in ("", "nan") else ""),
                              axis=1)
        mc["step"] = mc["track"] + ": " + mc["next_step"].astype(str)
        g = mc.groupby("user_id")
        agg = pd.DataFrame({
            "module_contest_by_track": g["line"].apply(" | ".join),
            "module_contest_next_step": g["step"].apply(" | ".join),
            "module_contest_cleared_tracks": g["contest_result"].apply(lambda s: (s == "Cleared").sum()),
        }).reset_index()
        out = out.merge(agg, on="user_id", how="left")
    out = out.merge(per_user(cards.get("Payments"), [
        "booking_fee_paid", "block_fee_paid", "nbfc_status", "nbfc_status_on"]), on="user_id", how="left")
    out = out.merge(per_user(cards.get("Access_Revoked"),
                             ["revoke_reason", "reason_logged_on", "access_state"],
                             sort_col="reason_logged_on"), on="user_id", how="left")

    # Groomers sheet
    if not groomers.empty:
        gcols = {k: v for k, v in GROOMER_COLS.items() if k in groomers.columns}
        g = groomers[["user_id"] + list(gcols)].rename(columns=gcols)
        out = out.merge(g, on="user_id", how="left")

    # sheet PR vs portal PR mismatch
    if "pr" in out.columns and "portal_placement_status" in out.columns:
        sheet_pr = out["pr"].eq("PR")
        portal_pr = out["portal_placement_status"].eq("PR")
        has_portal = out["portal_placement_status"].fillna("").ne("")
        out["pr_mismatch"] = ((sheet_pr != portal_pr) & has_portal).map({True: "Mismatch", False: ""})

    out["pending_actions"] = out.apply(pending_actions, axis=1)
    out["refreshed_at_ist"] = datetime.now(IST).strftime("%Y-%m-%d %H:%M")
    return out


def pending_actions(r: pd.Series) -> str:
    """#6 – what the student still has to do. Only meaningful for students who are not PR / placed."""
    placed = str(r.get("placed", "") or "")
    if (str(r.get("pr", "")) == "PR"
            or (placed.startswith("Placed") and "now returned" not in placed)
            or str(r.get("placement_status", "")) == "Placed"
            or str(r.get("access_state", "")) == "Access revoked / restricted"):
        return ""
    acts = []
    pend = str(r.get("projects_pending", "") or "")
    if pend and pend != "nan":
        acts.append(f"Clear projects: {pend}")
    att = pd.to_numeric(r.get("overall_attendance_pct"), errors="coerce")
    if pd.notna(att) and att < 75:
        acts.append(f"Attendance {att:.0f}% (<75%)")
    comp = pd.to_numeric(r.get("assignment_completion_pct"), errors="coerce")
    if pd.notna(comp) and comp < 80:
        acts.append(f"Assignments {comp:.0f}% (<80%)")
    if str(r.get("resume_profile_filled", "")).upper() == "FALSE":
        acts.append("Fill resume / profile")
    if str(r.get("resume_cleared", "")).upper() == "FALSE" and str(r.get("picking_status", "")) == "Picked":
        acts.append("Resume not cleared")
    if str(r.get("form_not_filled", "")).upper() == "TRUE":
        acts.append("Placement form not filled")
    flags = str(r.get("student_flags", "") or "")
    if flags and flags != "nan":
        acts.append(f"Flag: {flags}")
    steps = str(r.get("module_contest_next_step", "") or "")
    for part in steps.split(" | "):
        if any(k in part for k in ("take next attempt", "Missed attempt", "Attempts exhausted")):
            acts.append(f"Module contest – {part}")
    ns = pd.to_numeric(r.get("student_no_shows"), errors="coerce")
    if pd.notna(ns) and ns > 0:
        acts.append(f"{int(ns)} grooming no-show(s)")
    return "; ".join(acts)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main() -> int:
    t0 = time.time()
    errors: list[str] = []
    card_rows: dict[str, int] = {}

    mb = Metabase(METABASE_URL, os.environ["METABASE_API_KEY"])
    gc = gsheets_client()
    out_book = gc.open_by_key(OUTPUT_SHEET_ID)

    # 1. Metabase cards
    cards: dict[str, pd.DataFrame] = {}
    for key, cid in CARDS.items():
        if not cid:
            log(f"MB {key:<17} skipped – card id not set")
            continue
        try:
            t = time.time()
            df = clean_card(mb.card_df(cid))
            cards[key] = df
            card_rows[key] = len(df)
            log(f"MB {key:<17} #{cid:<6} {len(df):>6} rows  {time.time() - t:5.1f}s")
        except Exception as e:
            errors.append(f"{key}: {e}")
            log(f"MB {key:<17} FAILED – {e}")

    # 2. Sheets
    try:
        groomers = clean_sheet(read_tab(gc, GROOMERS_SHEET_ID, GROOMERS_TAB), "UserID", GROOMER_DATE_COLS)
        log(f"Groomers sheet      {len(groomers):>6} rows")
    except Exception as e:
        groomers = pd.DataFrame()
        errors.append(f"Groomers sheet: {e}")
        log(f"Groomers sheet FAILED – {e}")

    # 3. Build + write
    s360 = build_student_360(cards, groomers)
    log(f"Student_360         {len(s360):>6} rows x {s360.shape[1]} cols")

    write_tab(out_book, "Student_360", s360)
    for key, df in cards.items():
        write_tab(out_book, f"MB_{key}", df)
    if not groomers.empty:
        write_tab(out_book, "Groomers", groomers)

    secs = round(time.time() - t0)
    status = "OK" if not errors else "PARTIAL"
    append_log(out_book, [datetime.now(IST).strftime("%Y-%m-%d %H:%M"), status, str(secs),
                          str(len(s360)), json.dumps(card_rows), " | ".join(errors)[:45000]])
    log(f"Done: {status} in {secs}s")
    # fail the job only if nothing useful came back
    return 1 if len(cards) == 0 else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
