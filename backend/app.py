from __future__ import annotations

import io
import os
import re
import uuid
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber
from fastapi import Depends, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import Column, DateTime, String, create_engine, func
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.types import JSON
from starlette.responses import JSONResponse, StreamingResponse

# Excel
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter

# PDF (ReportLab)
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


# =========================
# App & CORS
# =========================
app = FastAPI(title="Mini Konsolidasi LK PDF + Arsip")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================
# Database
# =========================
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    DATABASE_URL = "sqlite:///./local.db"

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


class Report(Base):
    __tablename__ = "reports"
    report_id = Column(String(64), primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    period_label = Column(String(128), nullable=True)
    as_of_date = Column(String(32), nullable=True)
    companies_text = Column(String, nullable=True)
    mapping_count = Column(String(16), nullable=True)

    payload = Column(JSON, nullable=False)
    result = Column(JSON, nullable=False)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)


# =========================
# PDF Parsing helpers
# =========================
ACCOUNT_CODE_RE = re.compile(r"^\s*(\d{3}\.\d{3}-\d{2}(?:\.\d+)?[A-Za-z]?)\s+")
AMOUNT_RE = re.compile(r"(-?\(?\s*\d{1,3}(?:[.,]\d{3})*(?:[.,]\d+)?\s*\)?)\s*$")

ID_MONTHS = {
    "januari": 1, "jan": 1,
    "februari": 2, "feb": 2,
    "maret": 3, "mar": 3,
    "april": 4, "apr": 4,
    "mei": 5,
    "juni": 6, "jun": 6,
    "juli": 7, "jul": 7,
    "agustus": 8, "agu": 8, "ags": 8,
    "september": 9, "sep": 9,
    "oktober": 10, "okt": 10,
    "november": 11, "nov": 11,
    "desember": 12, "des": 12,
}


def parse_amount_id(amount_str: str) -> Optional[int]:
    s = amount_str.strip()
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1].strip()

    s = s.replace(" ", "")

    # normalize separators
    if "." in s and "," in s:
        s = s.replace(".", "")
        s = s.split(",")[0]
    else:
        s = s.replace(",", "")
        s = s.replace(".", "")

    if not s or not s.lstrip("-").isdigit():
        return None

    val = int(s)
    if neg:
        val = -abs(val)
    return val


def extract_lines_from_pdf(pdf_bytes: bytes) -> List[str]:
    lines: List[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for ln in text.splitlines():
                ln = ln.rstrip()
                if ln:
                    lines.append(ln)
    return lines


def detect_period(lines: List[str]) -> Dict[str, Optional[str]]:
    head = " \n ".join(lines[:60]).lower()

    m1 = re.search(r"\bper\s+(\d{1,2})\s+([a-zA-Z]{3,9})\s+(\d{4})\b", head)
    if m1:
        d = int(m1.group(1))
        mon = ID_MONTHS.get(m1.group(2).lower())
        y = int(m1.group(3))
        if mon:
            as_of = date(y, mon, d).isoformat()
            label = f"Per {d} {m1.group(2)} {y}"
            return {"label": label, "as_of": as_of}

    m2 = re.search(r"\bper\s+(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})\b", head)
    if m2:
        d, mon, y = int(m2.group(1)), int(m2.group(2)), int(m2.group(3))
        try:
            as_of = date(y, mon, d).isoformat()
            label = f"Per {d:02d}-{mon:02d}-{y}"
            return {"label": label, "as_of": as_of}
        except Exception:
            pass

    m3 = re.search(
        r"(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4}).{0,30}(s\.?d\.?|sampai|to|-\s*)(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})",
        head
    )
    if m3:
        d2, m2_, y2 = int(m3.group(5)), int(m3.group(6)), int(m3.group(7))
        try:
            as_of = date(y2, m2_, d2).isoformat()
            label = f"Periode s/d {d2:02d}-{m2_:02d}-{y2}"
            return {"label": label, "as_of": as_of}
        except Exception:
            pass

    return {"label": None, "as_of": None}


def parse_statement_rows(lines: List[str]) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Output rows (akun detail) + group_path dari heading yang terdeteksi.
    Catatan: Ini masih heuristik (text-based). Untuk PDF Accurate standar, group_path biasanya cukup rapi.
    """
    rows: List[Dict[str, Any]] = []
    warnings: List[str] = []

    current_section: Optional[str] = None
    group_stack: List[str] = []

    GROUP_KEYWORDS = {
        "ASET", "ASSET", "AKTIVA",
        "LIABILITAS", "KEWAJIBAN", "HUTANG",
        "EKUITAS", "MODAL",
        "PENDAPATAN", "BEBAN", "LABA", "RUGI",
        "LANCAR", "TIDAK LANCAR", "OPERASIONAL",
        "KAS", "BANK", "PIUTANG", "PERSEDIAAN", "UTANG",
        "PENJUALAN", "HPP", "BEBAN USAHA", "BEBAN OPERASIONAL"
    }

    SECTION_SET = {
        "ASET", "ASET LANCAR", "ASET TIDAK LANCAR",
        "LIABILITAS", "EKUITAS",
        "PENDAPATAN", "BEBAN", "BEBAN OPERASIONAL"
    }

    def is_heading(line: str) -> bool:
        t = line.strip()
        if not t:
            return False
        if ACCOUNT_CODE_RE.match(t):
            return False
        if AMOUNT_RE.search(t):
            return False

        letters = sum(ch.isalpha() for ch in t)
        if letters < 3:
            return False

        up = t.upper()
        mostly_upper = (sum(ch.isupper() for ch in t if ch.isalpha()) / max(1, letters)) > 0.7
        has_keyword = any(k in up for k in GROUP_KEYWORDS)
        short_enough = len(t) <= 70

        return short_enough and (mostly_upper or has_keyword)

    for ln in lines:
        t = ln.strip()
        up = t.upper()

        if up in SECTION_SET:
            current_section = t
            group_stack = []
            continue

        if is_heading(t):
            # heuristik besar/kecil
            if any(key in up for key in ["ASET", "LIABILITAS", "EKUITAS", "PENDAPATAN", "BEBAN"]):
                group_stack = [t]
            elif any(key in up for key in ["LANCAR", "TIDAK LANCAR", "OPERASIONAL"]):
                # tetap dianggap level besar
                if current_section:
                    group_stack = [current_section, t] if t != current_section else [t]
                else:
                    group_stack = [t]
            else:
                if not group_stack:
                    group_stack = [t]
                else:
                    if len(group_stack) >= 3:
                        group_stack = group_stack[:2] + [t]
                    else:
                        group_stack.append(t)
            continue

        m_code = ACCOUNT_CODE_RE.match(t)
        if not m_code:
            continue

        m_amt = AMOUNT_RE.search(t)
        if not m_amt:
            continue

        code = m_code.group(1).strip()
        amt_raw = m_amt.group(1).strip()
        amt = parse_amount_id(amt_raw)
        if amt is None:
            warnings.append(f"Gagal parse amount: '{amt_raw}' pada line: {t}")
            continue

        body = t[m_code.end():].strip()
        if amt_raw in body:
            body = body[: body.rfind(amt_raw)].strip()

        gp: List[str] = []
        if current_section and (not group_stack or group_stack[0] != current_section):
            gp.append(current_section)
        gp.extend(group_stack)

        rows.append({
            "account_code": code,
            "account_name": body,
            "amount": amt,
            "section": current_section,
            "raw_line": t,
            "group_path": gp
        })

    if not rows:
        warnings.append("Tidak ada baris akun terdeteksi. Cek format PDF / regex kode akun.")
    return rows, warnings


# =========================
# Models (API)
# =========================
class ParsedRow(BaseModel):
    account_code: str
    account_name: str
    amount: int
    section: Optional[str] = None
    raw_line: Optional[str] = None
    group_path: List[str] = Field(default_factory=list)


class CompanyPayload(BaseModel):
    company_name: str
    period: Optional[str] = None
    bs_rows: List[ParsedRow] = Field(default_factory=list)
    is_rows: List[ParsedRow] = Field(default_factory=list)


class PairMapping(BaseModel):
    pair_name: str
    company_ar: str
    ar_account_code: str
    company_ap: str
    ap_account_code: str
    note: Optional[str] = None


class ConsolidateOptions(BaseModel):
    elim_method: str = "MIN_ABS"
    strict_match: bool = False
    include_details: bool = True  # True=Lengkap, False=Ringkas


class ConsolidateRequest(BaseModel):
    companies: List[CompanyPayload]
    pair_mappings: List[PairMapping] = Field(default_factory=list)
    options: ConsolidateOptions = Field(default_factory=ConsolidateOptions)


# =========================
# Consolidation logic
# =========================
def index_balances(rows: List[ParsedRow]) -> Dict[str, ParsedRow]:
    return {r.account_code: r for r in rows}


def build_company_amount_map(companies: List[CompanyPayload], statement: str) -> Dict[str, Dict[str, int]]:
    res: Dict[str, Dict[str, int]] = {}
    for c in companies:
        rlist = c.bs_rows if statement == "BS" else c.is_rows
        for r in rlist:
            res.setdefault(r.account_code, {})
            res[r.account_code][c.company_name] = res[r.account_code].get(c.company_name, 0) + int(r.amount or 0)
    return res


def union_accounts_meta(companies: List[CompanyPayload], statement: str) -> Dict[str, Dict[str, Any]]:
    """
    Return: code -> {"name":..., "group_path":[...]}
    """
    out: Dict[str, Dict[str, Any]] = {}
    for c in companies:
        rlist = c.bs_rows if statement == "BS" else c.is_rows
        for r in rlist:
            gp = list(getattr(r, "group_path", []) or [])
            if r.account_code not in out:
                out[r.account_code] = {"name": r.account_name, "group_path": gp}
            else:
                # isi group_path jika sebelumnya kosong
                if not out[r.account_code].get("group_path") and gp:
                    out[r.account_code]["group_path"] = gp
    return out


def _sum_by_company(a: Dict[str, int], b: Dict[str, int]) -> Dict[str, int]:
    out = dict(a or {})
    for k, v in (b or {}).items():
        out[k] = out.get(k, 0) + int(v or 0)
    return out


def build_hierarchy_rows(
    meta: Dict[str, Dict[str, Any]],
    by_company: Dict[str, Dict[str, int]],
    companies_order: List[str],
    include_details: bool,
    elim_effect: Optional[Dict[str, int]] = None
) -> List[Dict[str, Any]]:
    """
    Output list rows:
    - GROUP row: {type:"GROUP", level:int, label:str, by_company:{}, elimination:int, total_after:int}
    - ACCOUNT row: {type:"ACCOUNT", level:int, account_code, account_name, ...}
    """
    # 1) build account rows
    accounts: List[Dict[str, Any]] = []
    for code, m in meta.items():
        name = m.get("name", "") or ""
        gp = m.get("group_path", []) or []
        bc = by_company.get(code, {}) or {}
        # normalize order: ensure all companies present
        bc_norm = {cn: int(bc.get(cn, 0) or 0) for cn in companies_order}
        total_before = sum(bc_norm.values())
        elimination = int((elim_effect or {}).get(code, 0) or 0)
        total_after = total_before + elimination

        accounts.append({
            "type": "ACCOUNT",
            "group_path": gp,
            "account_code": code,
            "account_name": name,
            "by_company": bc_norm,
            "total_before": total_before,
            "elimination": elimination,
            "total_after": total_after
        })

    # sort by group_path then code
    accounts.sort(key=lambda a: (a.get("group_path") or [], a.get("account_code") or ""))

    # 2) compute totals per group prefix
    group_totals: Dict[Tuple[str, ...], Dict[str, Any]] = {}

    def ensure_group(key: Tuple[str, ...]) -> Dict[str, Any]:
        if key not in group_totals:
            group_totals[key] = {
                "type": "GROUP",
                "key": key,
                "level": len(key),
                "label": key[-1] if key else "LAINNYA",
                "by_company": {cn: 0 for cn in companies_order},
                "elimination": 0,
                "total_after": 0,
            }
        return group_totals[key]

    for acc in accounts:
        gp = acc.get("group_path") or []
        if not gp:
            # taruh ke group LAINNYA supaya ringkas tetap ada tempatnya
            gp = ["LAINNYA"]

        for i in range(1, len(gp) + 1):
            key = tuple(gp[:i])
            g = ensure_group(key)
            g["by_company"] = _sum_by_company(g["by_company"], acc.get("by_company") or {})
            g["elimination"] += int(acc.get("elimination", 0) or 0)
            g["total_after"] += int(acc.get("total_after", 0) or 0)

    # 3) emit in order: group headings (once) + optional details
    out: List[Dict[str, Any]] = []
    emitted: set[Tuple[str, ...]] = set()

    for acc in accounts:
        gp = acc.get("group_path") or []
        if not gp:
            gp = ["LAINNYA"]

        for i in range(1, len(gp) + 1):
            key = tuple(gp[:i])
            if key not in emitted:
                g = ensure_group(key)
                out.append({
                    "type": "GROUP",
                    "level": g["level"],
                    "label": g["label"],
                    "by_company": g["by_company"],
                    "elimination": g["elimination"],
                    "total_after": g["total_after"],
                })
                emitted.add(key)

        if include_details:
            out.append({
                **acc,
                "level": len(gp) + 1
            })

    return out


def consolidate(companies: List[CompanyPayload], mappings: List[PairMapping], options: ConsolidateOptions):
    companies_order = [c.company_name for c in companies]

    bs_meta = union_accounts_meta(companies, "BS")
    is_meta = union_accounts_meta(companies, "IS")
    bs_by_company = build_company_amount_map(companies, "BS")
    is_by_company = build_company_amount_map(companies, "IS")

    elim_effect_bs: Dict[str, int] = {}
    elimination_journal: List[Dict[str, Any]] = []
    unreconciled: List[Dict[str, Any]] = []

    company_map = {c.company_name: c for c in companies}

    for mp in mappings:
        c_ar = company_map.get(mp.company_ar)
        c_ap = company_map.get(mp.company_ap)
        if not c_ar or not c_ap:
            unreconciled.append({"pair_name": mp.pair_name, "error": "Company not found", **mp.model_dump()})
            continue

        ar_idx = index_balances(c_ar.bs_rows)
        ap_idx = index_balances(c_ap.bs_rows)
        ar_row = ar_idx.get(mp.ar_account_code)
        ap_row = ap_idx.get(mp.ap_account_code)

        if not ar_row or not ap_row:
            unreconciled.append({
                "pair_name": mp.pair_name,
                "company_ar": mp.company_ar,
                "ar_account_code": mp.ar_account_code,
                "company_ap": mp.company_ap,
                "ap_account_code": mp.ap_account_code,
                "error": "AR/AP code not found in BS rows"
            })
            continue

        ar_bal = int(ar_row.amount or 0)
        ap_bal = int(ap_row.amount or 0)

        elim_amt = min(abs(ar_bal), abs(ap_bal))
        diff = abs(ar_bal) - abs(ap_bal)

        elim_effect_bs[mp.ar_account_code] = elim_effect_bs.get(mp.ar_account_code, 0) - elim_amt
        elim_effect_bs[mp.ap_account_code] = elim_effect_bs.get(mp.ap_account_code, 0) - elim_amt

        elimination_journal.append({
            "pair_name": mp.pair_name,
            "amount": elim_amt,
            "ar_balance": ar_bal,
            "ap_balance": ap_bal,
            "difference": diff,
            "status": "MATCH" if diff == 0 else "MISMATCH",
            "lines": [
                {"company": mp.company_ap, "drcr": "DR", "account_code": mp.ap_account_code, "account_name": ap_row.account_name, "amount": elim_amt},
                {"company": mp.company_ar, "drcr": "CR", "account_code": mp.ar_account_code, "account_name": ar_row.account_name, "amount": elim_amt},
            ]
        })

        if diff != 0:
            unreconciled.append({
                "pair_name": mp.pair_name,
                "company_ar": mp.company_ar,
                "ar_account_code": mp.ar_account_code,
                "company_ap": mp.company_ap,
                "ap_account_code": mp.ap_account_code,
                "ar_balance": ar_bal,
                "ap_balance": ap_bal,
                "difference": diff
            })

    bs_rows = build_hierarchy_rows(
        meta=bs_meta,
        by_company=bs_by_company,
        companies_order=companies_order,
        include_details=options.include_details,
        elim_effect=elim_effect_bs
    )

    is_rows = build_hierarchy_rows(
        meta=is_meta,
        by_company=is_by_company,
        companies_order=companies_order,
        include_details=options.include_details,
        elim_effect=None
    )

    return {
        "bs_comparison": bs_rows,
        "is_comparison": is_rows,
        "elimination_journal": elimination_journal,
        "unreconciled": unreconciled,
        "options_echo": options.model_dump()
    }


# =========================
# Routes: Parse + Consolidate
# =========================
@app.post("/api/parse")
async def api_parse(
    company_name: str = Form(...),
    period: str = Form(""),
    statement: str = Form(...),
    file: UploadFile = File(...)
):
    pdf_bytes = await file.read()
    lines = extract_lines_from_pdf(pdf_bytes)
    detected = detect_period(lines)
    rows, warnings = parse_statement_rows(lines)

    return JSONResponse({
        "company_name": company_name,
        "period": period or None,
        "statement": statement,
        "detected_period": detected,
        "rows": rows,
        "warnings": warnings
    })


@app.post("/api/consolidate")
async def api_consolidate(req: ConsolidateRequest):
    return JSONResponse(consolidate(req.companies, req.pair_mappings, req.options))


# =========================
# Routes: Reports (Arsip)
# =========================
class CreateReportRequest(BaseModel):
    companies: List[CompanyPayload]
    pair_mappings: List[PairMapping] = Field(default_factory=list)
    options: ConsolidateOptions = Field(default_factory=ConsolidateOptions)
    period_label: Optional[str] = None
    as_of: Optional[str] = None


@app.post("/api/reports")
def create_report(req: CreateReportRequest, db: Session = Depends(get_db)):
    result = consolidate(req.companies, req.pair_mappings, req.options)
    rid = "rpt_" + datetime.utcnow().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]

    company_names = [c.company_name for c in req.companies]
    companies_text = "|".join(company_names)

    r = Report(
        report_id=rid,
        period_label=req.period_label,
        as_of_date=req.as_of,
        companies_text=companies_text,
        mapping_count=str(len(req.pair_mappings)),
        payload=req.model_dump(),
        result=result
    )
    db.add(r)
    db.commit()

    return {"report_id": rid, "created_at": datetime.utcnow().isoformat() + "Z"}


@app.get("/api/reports")
def list_reports(query: Optional[str] = None, limit: int = 50, db: Session = Depends(get_db)):
    q = db.query(Report).order_by(Report.created_at.desc())
    if query:
        like = f"%{query.lower()}%"
        q = q.filter(
            func.lower(Report.companies_text).like(like)
            | func.lower(func.coalesce(Report.period_label, "")).like(like)
        )
    items = q.limit(min(max(limit, 1), 200)).all()

    return {
        "items": [
            {
                "report_id": r.report_id,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "period_label": r.period_label,
                "as_of": r.as_of_date,
                "companies": (r.companies_text.split("|") if r.companies_text else []),
                "mapping_count": int(r.mapping_count or "0"),
            }
            for r in items
        ]
    }


@app.get("/api/reports/{report_id}")
def get_report(report_id: str, db: Session = Depends(get_db)):
    r = db.query(Report).filter(Report.report_id == report_id).first()
    if not r:
        raise HTTPException(status_code=404, detail="Report not found")
    return {
        "report_id": r.report_id,
        "created_at": r.created_at.isoformat() if r.created_at else None,
        "period_label": r.period_label,
        "as_of": r.as_of_date,
        "payload": r.payload,
        "result": r.result
    }


@app.delete("/api/reports/{report_id}")
def delete_report(report_id: str, db: Session = Depends(get_db)):
    r = db.query(Report).filter(Report.report_id == report_id).first()
    if not r:
        raise HTTPException(status_code=404, detail="Report not found")
    db.delete(r)
    db.commit()
    return {"ok": True}


# =========================
# Export helpers
# =========================
def _company_names(req: ConsolidateRequest) -> List[str]:
    return [c.company_name for c in req.companies]


def _fmt_id(n: int) -> str:
    s = f"{int(n):,}"
    return s.replace(",", ".")


def _indent_text(text: str, level: int) -> str:
    level = max(0, int(level or 0))
    return ("   " * (level - 1)) + text if level > 1 else text


def _make_excel(req: ConsolidateRequest) -> bytes:
    result = consolidate(req.companies, req.pair_mappings, req.options)
    companies = _company_names(req)

    wb = Workbook()

    def add_sheet(title: str, rows: List[Dict[str, Any]]):
        ws = wb.create_sheet(title)
        headers = ["Kode", "Nama Akun"] + companies + ["Eliminasi", "Total Konsol"]
        ws.append(headers)

        # header style
        for col in range(1, len(headers) + 1):
            ws.cell(row=1, column=col).font = Font(bold=True)
            ws.cell(row=1, column=col).alignment = Alignment(horizontal="center")

        for r in rows:
            rtype = r.get("type") or "ACCOUNT"
            level = int(r.get("level", 0) or 0)

            if rtype == "GROUP":
                code = ""
                name = _indent_text(str(r.get("label", "") or ""), level)
            else:
                code = r.get("account_code", "") or ""
                name = _indent_text(str(r.get("account_name", "") or ""), level)

            byc = r.get("by_company", {}) or {}
            line = [code, name]
            for cn in companies:
                line.append(int(byc.get(cn, 0) or 0))
            line.append(int(r.get("elimination", 0) or 0))
            line.append(int(r.get("total_after", 0) or 0))
            ws.append(line)

            # bold group rows
            if rtype == "GROUP":
                ws.cell(row=ws.max_row, column=2).font = Font(bold=True)

        # align numbers right
        for row in range(2, ws.max_row + 1):
            for col in range(3, len(headers) + 1):
                ws.cell(row=row, column=col).alignment = Alignment(horizontal="right")

        # widths
        for col in range(1, len(headers) + 1):
            ws.column_dimensions[get_column_letter(col)].width = 18 if col >= 3 else (22 if col == 1 else 55)

    default = wb.active
    wb.remove(default)

    add_sheet("Neraca_Konsol", result.get("bs_comparison", []))
    add_sheet("LabaRugi_Konsol", result.get("is_comparison", []))

    # JE sheet
    ws = wb.create_sheet("Jurnal_Eliminasi")
    ws.append(["Pair", "DR Akun", "CR Akun", "Elim", "Selisih", "Status"])
    for col in range(1, 7):
        ws.cell(row=1, column=col).font = Font(bold=True)
        ws.cell(row=1, column=col).alignment = Alignment(horizontal="center")

    for j in (result.get("elimination_journal") or []):
        dr = (j.get("lines") or [{}])[0].get("account_code", "")
        cr = (j.get("lines") or [{}, {}])[1].get("account_code", "")
        ws.append([
            j.get("pair_name", ""),
            dr,
            cr,
            int(j.get("amount", 0) or 0),
            int(j.get("difference", 0) or 0),
            j.get("status", "")
        ])

    for row in range(2, ws.max_row + 1):
        ws.cell(row=row, column=4).alignment = Alignment(horizontal="right")
        ws.cell(row=row, column=5).alignment = Alignment(horizontal="right")

    for col in range(1, 7):
        ws.column_dimensions[get_column_letter(col)].width = 22

    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()


def _table_style_clear() -> TableStyle:
    return TableStyle([
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 9),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f2f4f7")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#1f2937")),

        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 9),
        ("TEXTCOLOR", (0, 1), (-1, -1), colors.HexColor("#111827")),

        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#d1d5db")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f9fafb")]),

        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ])


def _make_pdf(req: ConsolidateRequest) -> bytes:
    result = consolidate(req.companies, req.pair_mappings, req.options)
    companies = _company_names(req)

    bio = io.BytesIO()
    doc = SimpleDocTemplate(
        bio,
        pagesize=landscape(A4),
        leftMargin=12 * mm,
        rightMargin=12 * mm,
        topMargin=10 * mm,
        bottomMargin=10 * mm
    )

    styles = getSampleStyleSheet()
    title_style = styles["Heading2"]
    title_style.fontName = "Helvetica-Bold"
    title_style.fontSize = 13

    normal = styles["Normal"]
    normal.fontName = "Helvetica"
    normal.fontSize = 8.5
    normal.leading = 10

    def build_table(title: str, rows: List[Dict[str, Any]]):
        story_part = [Paragraph(title, title_style), Spacer(1, 6)]

        headers = ["Kode", "Nama Akun"] + companies + ["Elim", "Total"]
        data: List[List[Any]] = [headers]

        # track which row indexes are GROUP to bold them
        group_row_indexes: List[int] = []

        for r in rows:
            rtype = r.get("type") or "ACCOUNT"
            level = int(r.get("level", 0) or 0)

            if rtype == "GROUP":
                code = ""
                name = _indent_text(str(r.get("label", "") or ""), level)
            else:
                code = r.get("account_code", "") or ""
                name = _indent_text(str(r.get("account_name", "") or ""), level)

            byc = r.get("by_company", {}) or {}
            row_line: List[Any] = [
                code,
                Paragraph(name, normal),
            ]
            for cn in companies:
                row_line.append(_fmt_id(int(byc.get(cn, 0) or 0)))
            row_line.append(_fmt_id(int(r.get("elimination", 0) or 0)))
            row_line.append(_fmt_id(int(r.get("total_after", 0) or 0)))
            data.append(row_line)

            if rtype == "GROUP":
                group_row_indexes.append(len(data) - 1)  # 0-based in data list

        col_widths = [26 * mm, 92 * mm] + [26 * mm] * len(companies) + [22 * mm, 26 * mm]
        tbl = Table(data, colWidths=col_widths, repeatRows=1)

        st = _table_style_clear()
        st.add("ALIGN", (0, 0), (1, -1), "LEFT")
        st.add("ALIGN", (2, 0), (-1, -1), "RIGHT")

        # bold GROUP rows
        for ridx in group_row_indexes:
            st.add("FONTNAME", (0, ridx), (-1, ridx), "Helvetica-Bold")
            st.add("BACKGROUND", (0, ridx), (-1, ridx), colors.HexColor("#eef2f7"))

        tbl.setStyle(st)
        story_part.append(tbl)
        return story_part

    story: List[Any] = []
    story += build_table("Neraca Konsolidasi (Komparasi)", result.get("bs_comparison", []))
    story.append(PageBreak())
    story += build_table("Laba/Rugi Konsolidasi (Komparasi)", result.get("is_comparison", []))
    story.append(PageBreak())

    # ===== JE =====
    story.append(Paragraph("Jurnal Eliminasi", title_style))
    story.append(Spacer(1, 6))

    je = result.get("elimination_journal", []) or []
    je_headers = ["Pair", "DR", "CR", "Elim", "Selisih", "Status"]
    je_data: List[List[Any]] = [je_headers]

    for j in je:
        dr = (j.get("lines") or [{}])[0].get("account_code", "")
        cr = (j.get("lines") or [{}, {}])[1].get("account_code", "")
        je_data.append([
            j.get("pair_name", ""),
            dr,
            cr,
            _fmt_id(int(j.get("amount", 0) or 0)),
            _fmt_id(int(j.get("difference", 0) or 0)),
            j.get("status", "")
        ])

    je_tbl = Table(
        je_data,
        colWidths=[80 * mm, 28 * mm, 28 * mm, 25 * mm, 25 * mm, 22 * mm],
        repeatRows=1
    )

    je_style = _table_style_clear()
    je_style.add("ALIGN", (0, 0), (2, -1), "LEFT")
    je_style.add("ALIGN", (3, 0), (4, -1), "RIGHT")
    je_tbl.setStyle(je_style)

    story.append(je_tbl)

    doc.build(story)
    return bio.getvalue()


# =========================
# Export routes
# =========================
@app.post("/api/export/excel")
def export_excel(req: ConsolidateRequest):
    data = _make_excel(req)
    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=konsolidasi.xlsx"},
    )


@app.post("/api/export/pdf")
def export_pdf(req: ConsolidateRequest):
    data = _make_pdf(req)
    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=konsolidasi.pdf"},
    )