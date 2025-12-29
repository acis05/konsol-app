from __future__ import annotations

import io
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber
from fastapi import FastAPI, File, Form, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from starlette.responses import StreamingResponse, JSONResponse

app = FastAPI(title="Mini Konsolidasi LK PDF")

# CORS (ubah origins sesuai domain frontend kamu)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ACCOUNT_CODE_RE = re.compile(r"^\s*(\d{3}\.\d{3}-\d{2}|\d{3}\.\d{3}-\d{2}\.\d+|\d{3}\.\d{3}-\d{2}[A-Za-z]?|\d{3}\.\d{3}-\d{2})\s+")
# NOTE: regex di atas bisa kamu perketat sesuai format kode akun kamu. Di contoh: 111.102-01

AMOUNT_RE = re.compile(r"(-?\(?\s*\d{1,3}(?:\.\d{3})*(?:,\d+)?\s*\)?)$")  # supports 1.234.567 or (1.234)

def parse_amount_id(amount_str: str) -> Optional[int]:
    """
    Parse angka format Indonesia: 14,260,127,477 atau 14.260.127.477.
    Support negatif pakai tanda minus atau ( ... ).
    Return integer (IDR) tanpa desimal.
    """
    s = amount_str.strip()
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1].strip()

    s = s.replace(" ", "")
    # normalize separators:
    # if comma used as thousand sep in your pdf, remove commas
    # if dot used as thousand sep, remove dots
    # if decimal exists, we ignore decimals for IDR reporting.
    # Heuristic: if both '.' and ',' exist, assume '.' thousand and ',' decimal OR vice versa -> drop both then handle decimal
    if "." in s and "," in s:
        # assume thousand separator is '.' and decimal separator is ',' -> remove thousand and drop decimal
        s = s.replace(".", "")
        s = s.split(",")[0]
    else:
        # remove thousand separators: prefer both
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

def parse_statement_rows(lines: List[str]) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Parse baris yang punya pola:
    <kode akun> <deskripsi ...> <nilai di ujung>
    """
    rows: List[Dict[str, Any]] = []
    warnings: List[str] = []

    current_section = None
    for ln in lines:
        # tangkap section sederhana (opsional)
        # kamu bisa tambah keyword seperti ASET, LIABILITAS, EKUITAS, PENDAPATAN, BEBAN
        if ln.strip().upper() in {"ASET", "ASET LANCAR", "ASET TIDAK LANCAR", "LIABILITAS", "EKUITAS",
                                  "PENDAPATAN", "BEBAN", "BEBAN OPERASIONAL"}:
            current_section = ln.strip()
            continue

        # match kode akun di awal
        m_code = ACCOUNT_CODE_RE.match(ln)
        if not m_code:
            continue

        # ambil angka di akhir baris
        m_amt = AMOUNT_RE.search(ln)
        if not m_amt:
            continue

        code = m_code.group(1).strip()
        amt_raw = m_amt.group(1).strip()
        amt = parse_amount_id(amt_raw)
        if amt is None:
            warnings.append(f"Gagal parse amount: '{amt_raw}' pada line: {ln}")
            continue

        # deskripsi = buang kode di awal dan amount di akhir
        body = ln[m_code.end():].strip()
        body = body[: body.rfind(amt_raw)].strip() if amt_raw in body else body
        name = body

        rows.append({
            "account_code": code,
            "account_name": name,
            "amount": amt,
            "section": current_section,
            "raw_line": ln
        })

    if not rows:
        warnings.append("Tidak ada baris akun yang terdeteksi. Cek format PDF / regex kode akun.")
    return rows, warnings

# ---------------- Pydantic models ----------------

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

# ---------------- Core consolidation logic ----------------

def index_balances(rows: List[ParsedRow]) -> Dict[str, ParsedRow]:
    return {r.account_code: r for r in rows}

def union_accounts(companies: List[CompanyPayload], statement: str) -> Dict[str, str]:
    """Return dict account_code -> account_name (first seen)."""
    out: Dict[str, str] = {}
    for c in companies:
        rlist = c.bs_rows if statement == "BS" else c.is_rows
        for r in rlist:
            out.setdefault(r.account_code, r.account_name)
    return out

def build_company_amount_map(companies: List[CompanyPayload], statement: str) -> Dict[str, Dict[str, int]]:
    """
    Return: account_code -> {company_name: amount}
    """
    res: Dict[str, Dict[str, int]] = {}
    for c in companies:
        rlist = c.bs_rows if statement == "BS" else c.is_rows
        for r in rlist:
            res.setdefault(r.account_code, {})
            res[r.account_code][c.company_name] = res[r.account_code].get(c.company_name, 0) + r.amount
    return res

def consolidate(companies: List[CompanyPayload], mappings: List[PairMapping], options: ConsolidateOptions):
    # 1) base tables (rule 1 & 2 built-in via union+by_company)
    bs_names = union_accounts(companies, "BS")
    is_names = union_accounts(companies, "IS")
    bs_by_company = build_company_amount_map(companies, "BS")
    is_by_company = build_company_amount_map(companies, "IS")

    # 2) elimination journal & elimination impacts per account_code per company (net effect)
    # We'll aggregate elimination impact at consolidated-level per account_code:
    elim_effect_bs: Dict[str, int] = {}  # account_code -> signed elimination effect on total
    elimination_journal: List[Dict[str, Any]] = []
    unreconciled: List[Dict[str, Any]] = []

    # index for quick lookup
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
                "error": "AR/AP account code not found in BS rows"
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

        # Apply elimination on consolidated totals:
        # - AR is asset (usually positive), eliminate by reducing consolidated total => negative effect on AR total
        # - AP is liability (usually positive), eliminate by reducing consolidated total => negative effect on AP total
        # We'll store signed effect at account_code level as -elim_amt for both accounts.
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

    # 3) Build comparison rows
    def build(statement: str, names: Dict[str, str], by_company: Dict[str, Dict[str, int]], elim_effect: Optional[Dict[str, int]] = None):
        out = []
        for code, name in sorted(names.items()):
            bc = by_company.get(code, {})
            total_before = sum(bc.values())
            elimination = (elim_effect or {}).get(code, 0)
            total_after = total_before + elimination
            out.append({
                "account_code": code,
                "account_name": name,
                "by_company": bc,
                "total_before": total_before,
                "elimination": elimination,
                "total_after": total_after
            })
        return out

    bs_comp = build("BS", bs_names, bs_by_company, elim_effect_bs)
    is_comp = build("IS", is_names, is_by_company, None)  # default: eliminasi hanya BS (AR/AP). Bisa diperluas.

    return {
        "bs_comparison": bs_comp,
        "is_comparison": is_comp,
        "elimination_journal": elimination_journal,
        "unreconciled": unreconciled
    }

# ---------------- Routes ----------------

@app.post("/api/parse")
async def api_parse(
    company_name: str = Form(...),
    period: str = Form(""),
    statement: str = Form(...),  # BS / IS
    file: UploadFile = File(...)
):
    pdf_bytes = await file.read()
    lines = extract_lines_from_pdf(pdf_bytes)
    rows, warnings = parse_statement_rows(lines)
    return JSONResponse({
        "company_name": company_name,
        "period": period or None,
        "statement": statement,
        "rows": rows,
        "warnings": warnings
    })

@app.post("/api/consolidate")
async def api_consolidate(req: ConsolidateRequest):
    result = consolidate(req.companies, req.pair_mappings, req.options)
    return JSONResponse(result)

# Export endpoints: skeleton (isi nanti pakai openpyxl & reportlab)
@app.post("/api/export/excel")
async def api_export_excel(req: ConsolidateRequest):
    # TODO: generate xlsx bytes with openpyxl
    content = b"TODO: excel"
    return StreamingResponse(io.BytesIO(content), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition": 'attachment; filename="konsolidasi.xlsx"'})

@app.post("/api/export/pdf")
async def api_export_pdf(req: ConsolidateRequest):
    # TODO: generate pdf bytes with reportlab
    content = b"%PDF-1.4\n%TODO\n"
    return StreamingResponse(io.BytesIO(content), media_type="application/pdf",
                             headers={"Content-Disposition": 'attachment; filename="konsolidasi.pdf"'})