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
from sqlalchemy import Column, DateTime, String, Text, create_engine, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from starlette.responses import JSONResponse

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
# Database (Postgres Railway)
# =========================
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    # fallback local sqlite (dev)
    DATABASE_URL = "sqlite:///./local.db"

# Psycopg2 URL compatibility (railway sometimes gives postgres://)
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


class Report(Base):
    __tablename__ = "reports"
    report_id = Column(String(64), primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # search helpers
    period_label = Column(String(128), nullable=True)
    as_of_date = Column(String(32), nullable=True)  # store ISO string for simplicity
    companies_text = Column(Text, nullable=True)    # "PT A|PT B|PT C"
    mapping_count = Column(String(16), nullable=True)

    payload = Column(JSONB().with_variant(Text, "sqlite"), nullable=False)
    result = Column(JSONB().with_variant(Text, "sqlite"), nullable=False)


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

# Contoh kode akun: 111.102-01
ACCOUNT_CODE_RE = re.compile(r"^\s*(\d{3}\.\d{3}-\d{2}(?:\.\d+)?[A-Za-z]?)\s+")

# Amount di ujung: 14,260,127,477 atau 14.260.127.477 atau (1.234.000)
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

    # normalize separators:
    # if both '.' and ',' appear, assume '.' thousands and ',' decimals
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
    """
    Deteksi periode dari teks PDF.
    Return: {"label": "...", "as_of": "YYYY-MM-DD" or None}
    """
    head = " \n ".join(lines[:60]).lower()

    # Pattern 1: "Per 31 Desember 2025" / "Per 31-12-2025" / "Per 31/12/2025"
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

    # Pattern 2: rentang periode "01/12/2025 s.d 31/12/2025"
    m3 = re.search(r"(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4}).{0,30}(s\.?d\.?|sampai|to|-\s*)(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})", head)
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
    rows: List[Dict[str, Any]] = []
    warnings: List[str] = []
    current_section = None

    for ln in lines:
        up = ln.strip().upper()
        if up in {
            "ASET", "ASET LANCAR", "ASET TIDAK LANCAR",
            "LIABILITAS", "EKUITAS",
            "PENDAPATAN", "BEBAN", "BEBAN OPERASIONAL"
        }:
            current_section = ln.strip()
            continue

        m_code = ACCOUNT_CODE_RE.match(ln)
        if not m_code:
            continue

        m_amt = AMOUNT_RE.search(ln)
        if not m_amt:
            continue

        code = m_code.group(1).strip()
        amt_raw = m_amt.group(1).strip()
        amt = parse_amount_id(amt_raw)
        if amt is None:
            warnings.append(f"Gagal parse amount: '{amt_raw}' pada line: {ln}")
            continue

        body = ln[m_code.end():].strip()
        if amt_raw in body:
            body = body[: body.rfind(amt_raw)].strip()

        rows.append({
            "account_code": code,
            "account_name": body,
            "amount": amt,
            "section": current_section,
            "raw_line": ln
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
    elim_method: str = "MIN_ABS"   # MIN_ABS | STRICT_EQUAL
    strict_match: bool = False


class ConsolidateRequest(BaseModel):
    companies: List[CompanyPayload]
    pair_mappings: List[PairMapping] = Field(default_factory=list)
    options: ConsolidateOptions = Field(default_factory=ConsolidateOptions)


# =========================
# Consolidation logic
# =========================
def index_balances(rows: List[ParsedRow]) -> Dict[str, ParsedRow]:
    return {r.account_code: r for r in rows}


def union_accounts(companies: List[CompanyPayload], statement: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for c in companies:
        rlist = c.bs_rows if statement == "BS" else c.is_rows
        for r in rlist:
            out.setdefault(r.account_code, r.account_name)
    return out


def build_company_amount_map(companies: List[CompanyPayload], statement: str) -> Dict[str, Dict[str, int]]:
    res: Dict[str, Dict[str, int]] = {}
    for c in companies:
        rlist = c.bs_rows if statement == "BS" else c.is_rows
        for r in rlist:
            res.setdefault(r.account_code, {})
            res[r.account_code][c.company_name] = res[r.account_code].get(c.company_name, 0) + r.amount
    return res


def consolidate(companies: List[CompanyPayload], mappings: List[PairMapping], options: ConsolidateOptions):
    bs_names = union_accounts(companies, "BS")
    is_names = union_accounts(companies, "IS")
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

        ar_bal = ar_row.amount
        ap_bal = ap_row.amount

        if options.elim_method == "STRICT_EQUAL":
            if abs(ar_bal) != abs(ap_bal):
                unreconciled.append({
                    "pair_name": mp.pair_name,
                    "company_ar": mp.company_ar,
                    "ar_account_code": mp.ar_account_code,
                    "company_ap": mp.company_ap,
                    "ap_account_code": mp.ap_account_code,
                    "ar_balance": ar_bal,
                    "ap_balance": ap_bal,
                    "difference": abs(ar_bal) - abs(ap_bal)
                })
                if options.strict_match:
                    continue
            elim_amt = min(abs(ar_bal), abs(ap_bal))
        else:
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

    def build(names: Dict[str, str], by_company: Dict[str, Dict[str, int]], elim_effect: Optional[Dict[str, int]] = None):
        out = []
        for code, name in sorted(names.items()):
            bc = by_company.get(code, {})
            total_before = sum(bc.values())
            elimination = (elim_effect or {}).get(code, 0)
            total_after = total_before + elimination
            out.append({
                "account_code": code,
                "account_name": name,
                "by_company": bc,          # ini kunci untuk laporan komparasi multi-company
                "total_before": total_before,
                "elimination": elimination,
                "total_after": total_after
            })
        return out

    return {
        "bs_comparison": build(bs_names, bs_by_company, elim_effect_bs),
        "is_comparison": build(is_names, is_by_company, None),
        "elimination_journal": elimination_journal,
        "unreconciled": unreconciled
    }


# =========================
# Routes: Parse + Consolidate
# =========================
@app.post("/api/parse")
async def api_parse(
    company_name: str = Form(...),
    period: str = Form(""),
    statement: str = Form(...),  # BS / IS
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
        "detected_period": detected,  # <-- NEW
        "rows": rows,
        "warnings": warnings
    })


@app.post("/api/consolidate")
async def api_consolidate(req: ConsolidateRequest):
    result = consolidate(req.companies, req.pair_mappings, req.options)
    return JSONResponse(result)


# =========================
# Routes: Reports (Arsip)
# =========================
class CreateReportRequest(BaseModel):
    # payloadnya sama seperti consolidate
    companies: List[CompanyPayload]
    pair_mappings: List[PairMapping] = Field(default_factory=list)
    options: ConsolidateOptions = Field(default_factory=ConsolidateOptions)

    # metadata tambahan (opsional) dari frontend
    period_label: Optional[str] = None
    as_of: Optional[str] = None


@app.post("/api/reports")
def create_report(req: CreateReportRequest, db: Session = Depends(get_db)):
    # hitung konsolidasi
    result = consolidate(req.companies, req.pair_mappings, req.options)

    rid = "rpt_" + datetime.utcnow().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]

    company_names = [c.company_name for c in req.companies]
    companies_text = "|".join(company_names)
    mapping_count = str(len(req.pair_mappings))

    # simpan
    r = Report(
        report_id=rid,
        period_label=req.period_label,
        as_of_date=req.as_of,
        companies_text=companies_text,
        mapping_count=mapping_count,
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
        q = q.filter(func.lower(Report.companies_text).like(like) | func.lower(func.coalesce(Report.period_label, "")).like(like))
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