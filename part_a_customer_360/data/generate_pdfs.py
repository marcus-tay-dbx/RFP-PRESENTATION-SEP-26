#!/usr/bin/env python3
"""Generate 5 Alliance Bank product catalog PDFs. Run locally: python generate_pdfs.py"""

from reportlab.lib.pagesizes import A4
from reportlab.lib.colors import HexColor, white
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                  TableStyle, HRFlowable)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
import os

NAVY  = HexColor("#1B3A6B")
RED   = HexColor("#C8102E")
LGREY = HexColor("#F5F5F5")
BLACK = HexColor("#222222")

def make_styles():
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle("Title2",     fontName="Helvetica-Bold", fontSize=22, textColor=white, spaceAfter=6,  leading=26))
    styles.add(ParagraphStyle("Subtitle2",  fontName="Helvetica",      fontSize=13, textColor=white, spaceAfter=4))
    styles.add(ParagraphStyle("BodyText2",  fontName="Helvetica",      fontSize=10, textColor=BLACK, spaceAfter=4,  leading=14))
    styles.add(ParagraphStyle("SectionHead",fontName="Helvetica-Bold", fontSize=12, textColor=NAVY,  spaceAfter=4,  spaceBefore=8))
    styles.add(ParagraphStyle("SmallPrint", fontName="Helvetica",      fontSize=8,  textColor=HexColor("#666666"), spaceAfter=2))
    return styles

def header_table(product_name, category, code, styles):
    data = [[Paragraph(f'<font color="white"><b>{product_name}</b></font>', styles["Title2"]),
             Paragraph(f'<font color="white">{category} | {code}</font>', styles["Subtitle2"])]]
    t = Table(data, colWidths=[14*cm, 5*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,-1), NAVY),
        ("ALIGN",      (0,0), (-1,-1), "LEFT"),
        ("VALIGN",     (0,0), (-1,-1), "MIDDLE"),
        ("PADDING",    (0,0), (-1,-1), 16),
        ("LINEBELOW",  (0,-1),(-1,-1), 2, RED),
    ]))
    return t

def feature_table(rows, styles):
    data = [[Paragraph(f"<b>{k}</b>", styles["BodyText2"]),
             Paragraph(str(v), styles["BodyText2"])] for k,v in rows]
    t = Table(data, colWidths=[7*cm, 12*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,-1), LGREY),
        ("BACKGROUND", (0,0), (0,-1),  HexColor("#E8EDF4")),
        ("GRID",       (0,0), (-1,-1), 0.5, HexColor("#CCCCCC")),
        ("PADDING",    (0,0), (-1,-1), 6),
        ("VALIGN",     (0,0), (-1,-1), "TOP"),
    ]))
    return t

PRODUCTS = [
    {
        "filename": "alliance_visa_platinum.pdf",
        "name": "Alliance Bank Visa Platinum Credit Card",
        "code": "CC-VISA-PLAT-001", "category": "CREDIT CARD",
        "tagline": "Live more, earn more with every purchase",
        "features": [
            ("Annual Fee (Principal)", "RM 800 (waived with min. 12 transactions/year)"),
            ("Annual Fee (Supplementary)", "RM 400"),
            ("Credit Limit", "RM 3,000 – RM 500,000"),
            ("Cashback Rate", "5% on Dining, Grocery & Online"),
            ("Cashback Cap", "RM 50/month"),
            ("Reward Points", "2x TreatsPoints per RM1 spent"),
            ("Interest Rate", "18% p.a. (1.5% per month)"),
            ("Interest-Free Period", "Up to 20 days"),
            ("Balance Transfer Rate", "0% for 12 months (3% processing fee)"),
            ("Late Payment Charge", "1% of outstanding or RM 10, max RM 100"),
            ("Minimum Income", "RM 36,000 p.a."),
            ("Eligible Age", "21 – 65 years"),
            ("Forex Fee", "1.5% of transaction amount"),
            ("Airport Lounge Access", "Yes — LoungeKey (2 visits/year)"),
            ("Travel Insurance", "Yes — complimentary travel PA"),
            ("Concierge Service", "24/7 Alliance Concierge"),
            ("Contactless Limit", "RM 250 per transaction"),
            ("Shariah Compliant", "No (conventional card)"),
            ("Effective Date", "1 September 2026"),
        ],
        "benefits": ["Earn 5% cashback on everyday spending",
                     "Complimentary airport lounge access worldwide",
                     "0% balance transfer for 12 months"],
        "eligibility": "Malaysian/PR, 21–65 years, min. income RM 36,000 p.a. Permanent employee, self-employed, or professional. Clean CCRIS record.",
        "docs": "MyKad / Passport, 3 months' payslip, latest EPF statement (self-employed: 6 months bank statement + business registration)",
    },
    {
        "filename": "alliance_cashfirst_financing.pdf",
        "name": "Alliance CashFirst Personal Financing-i",
        "code": "PF-CASHFIRST-I-001", "category": "PERSONAL LOAN",
        "tagline": "Fast, flexible Islamic financing for your needs",
        "features": [
            ("Financing Type", "Unsecured Islamic Financing"),
            ("Shariah Concept", "Tawarruq (Commodity Murabahah)"),
            ("Profit Rate", "From 3.99% p.a. (fixed)"),
            ("Effective Rate", "From 7.47% p.a. (EIR)"),
            ("Min Financing Amount", "RM 5,000"),
            ("Max Financing Amount", "RM 200,000"),
            ("Min Tenure", "12 months"),
            ("Max Tenure", "84 months"),
            ("Processing Fee", "Nil"),
            ("Early Settlement Fee", "Nil"),
            ("Minimum Monthly Income", "RM 2,000"),
            ("Eligible Employment", "Permanent, Contract (min. 1 year), Self-Employed"),
            ("Max DSR", "60%"),
            ("Collateral Required", "No"),
            ("Guarantor Required", "No"),
            ("Takaful Coverage", "Optional — MRTA available"),
            ("Disbursement SLA", "3 working days"),
            ("Top-Up Facility", "Yes — available after 12 months"),
            ("Max Age at Maturity", "60 years"),
            ("Effective Date", "1 September 2026"),
        ],
        "benefits": ["No processing fee, no early settlement penalty",
                     "Flexible tenure up to 7 years",
                     "Shariah-compliant — Tawarruq concept"],
        "eligibility": "Malaysian/PR, 21–57 years at application. Minimum monthly income RM 2,000. Clean CCRIS (no arrears > 90 days in past 12 months).",
        "docs": "MyKad, 3 months' payslips, EPF statement, employment confirmation letter",
    },
    {
        "filename": "alliance_homesmart_financing.pdf",
        "name": "Alliance HomeSmart Financing",
        "code": "HF-HOMESMART-001", "category": "HOME LOAN",
        "tagline": "Own your dream home with flexible financing",
        "features": [
            ("Financing Type", "Conventional & Islamic-i (Tawarruq)"),
            ("Base Rate", "Alliance Bank BR: 3.00% p.a."),
            ("Spread Above BR", "+1.00% to +2.00% p.a."),
            ("Effective Rate", "From 4.00% p.a."),
            ("Max Financing Margin", "Up to 90% of property value"),
            ("Max Tenure", "35 years"),
            ("Lock-In Period", "3 years"),
            ("Lock-In Penalty", "3% of approved financing amount"),
            ("Flexi Feature", "Semi-Flexi with linked current account"),
            ("Legal Fees", "Can be financed"),
            ("MRTA", "Optional (recommended)"),
            ("Fire Insurance", "Required"),
            ("Eligible Properties", "Residential: Landed, Stratified; Select Commercial"),
            ("Max Age at Maturity", "70 years"),
            ("Min Property Value", "RM 100,000"),
            ("First Home Scheme", "Eligible for Skim Jaminan Kredit Perumahan"),
            ("Stamp Duty Exemption", "Yes — for first home below RM 500,000"),
            ("Valuation Fee", "Borne by borrower"),
            ("Effective Date", "1 September 2026"),
        ],
        "benefits": ["Up to 90% financing margin",
                     "Semi-flexi redraw facility for extra savings",
                     "Eligible for first home buyer government guarantee"],
        "eligibility": "Malaysian/PR, 18–68 years. Property must be in Malaysia. Minimum income assessed based on DSR. Joint application allowed.",
        "docs": "MyKad, 3 months' payslips, EPF, sale & purchase agreement, property valuation report",
    },
    {
        "filename": "alliance_wealthsmart_fund.pdf",
        "name": "Alliance WealthSmart Income Fund",
        "code": "UT-WEALTHSMART-INC-001", "category": "INVESTMENT",
        "tagline": "Grow your wealth with consistent quarterly distributions",
        "features": [
            ("Fund Type", "Unit Trust — Income / Bond Fund"),
            ("Risk Rating", "Medium"),
            ("Fund Manager", "Alliance Islamic Asset Management Sdn Bhd"),
            ("Trustee", "CIMB Commerce Trustee Berhad"),
            ("Currency", "MYR"),
            ("Min Initial Investment", "RM 1,000"),
            ("Min Additional Investment", "RM 100"),
            ("Annual Management Fee", "0.80% p.a."),
            ("Annual Trustee Fee", "0.07% p.a."),
            ("Sales Charge", "Up to 3.00%"),
            ("Repurchase Charge", "Nil"),
            ("Distribution Frequency", "Quarterly"),
            ("Target Distribution", "4.5% – 5.5% p.a. (not guaranteed)"),
            ("Benchmark", "Maybank 3-month fixed deposit rate"),
            ("Investment Horizon", "Min. 3 years"),
            ("Asset Allocation", "70–100% fixed income, 0–30% cash"),
            ("Capital Guaranteed", "No"),
            ("Shariah Compliant", "Yes"),
            ("Liquidity", "T+2 business days"),
            ("Effective Date", "1 September 2026"),
        ],
        "benefits": ["Regular quarterly income distributions",
                     "Shariah-compliant with Medium risk profile",
                     "Low minimum entry at RM 1,000"],
        "eligibility": "Open to all Malaysian residents and non-residents. Subject to KYC and suitability assessment. Not eligible for KWSP (EPF) withdrawal.",
        "docs": "MyKad / Passport, completed application form, suitability questionnaire",
    },
    {
        "filename": "alliance_carstar_takaful.pdf",
        "name": "Alliance CarStar Takaful",
        "code": "INS-CARSTAR-TAKAFUL-001", "category": "INSURANCE",
        "tagline": "Comprehensive motor takaful protection for your vehicle",
        "features": [
            ("Coverage Type", "Comprehensive Motor Takaful"),
            ("Takaful Model", "Wakalah with Waqf"),
            ("Contribution Basis", "Agreed Value"),
            ("Max Sum Covered", "RM 500,000"),
            ("Third-Party Liability", "RM 3,000,000"),
            ("Coverage Period", "12 months"),
            ("Max No-Claim Discount", "55%"),
            ("NCD Transferable", "Yes"),
            ("Named Drivers", "Up to 3"),
            ("Passenger PA Coverage", "RM 10,000 per passenger"),
            ("Roadside Assistance", "24/7 — included"),
            ("Towing Limit", "Up to 150 km"),
            ("Flood Coverage", "Add-on available"),
            ("Windscreen Coverage", "Up to RM 1,500"),
            ("Claim Settlement (Total Loss)", "7 working days"),
            ("Panel Workshops", "500+ nationwide"),
            ("Eligible Vehicles", "Private cars & light commercial"),
            ("Max Vehicle Age (Comprehensive)", "15 years"),
            ("Standard Excess", "RM 400"),
            ("Effective Date", "1 September 2026"),
        ],
        "benefits": ["24/7 roadside assistance included",
                     "Up to 55% No-Claim Discount",
                     "Fast 7-day total loss settlement"],
        "eligibility": "Vehicle owner aged 18–70. Vehicle not exceeding 15 years old (comprehensive). Must hold valid driving license. Subject to underwriting.",
        "docs": "MyKad, vehicle grant, previous takaful/insurance certificate, roadtax",
    },
]

def generate_pdf(product, output_dir):
    styles = make_styles()
    path   = os.path.join(output_dir, product["filename"])
    doc    = SimpleDocTemplate(path, pagesize=A4,
                               leftMargin=1.5*cm, rightMargin=1.5*cm,
                               topMargin=1.5*cm,  bottomMargin=1.5*cm)
    story = []
    story.append(header_table(product["name"], product["category"], product["code"], styles))
    story.append(Spacer(1, 0.4*cm))
    story.append(Paragraph(product["tagline"], styles["Subtitle2"]))
    story.append(Spacer(1, 0.6*cm))
    story.append(Paragraph("Key Benefits", styles["SectionHead"]))
    for b in product["benefits"]:
        story.append(Paragraph(f"- {b}", styles["BodyText2"]))
    story.append(Spacer(1, 0.4*cm))
    story.append(Paragraph("Product Features & Rates", styles["SectionHead"]))
    story.append(feature_table(product["features"], styles))
    story.append(Spacer(1, 0.6*cm))
    story.append(Paragraph("Eligibility Criteria", styles["SectionHead"]))
    story.append(Paragraph(product["eligibility"], styles["BodyText2"]))
    story.append(Spacer(1, 0.4*cm))
    story.append(Paragraph("Documents Required", styles["SectionHead"]))
    story.append(Paragraph(product["docs"], styles["BodyText2"]))
    story.append(Spacer(1, 0.6*cm))
    story.append(HRFlowable(width="100%", thickness=1, color=RED))
    story.append(Spacer(1, 0.2*cm))
    story.append(Paragraph(
        "Alliance Bank Malaysia Berhad (198201008390 / 112748-V). This brochure is for "
        "illustrative purposes only. Terms and conditions apply. Subject to credit assessment "
        "and approval. Rates and fees are effective as of the date shown above.",
        styles["SmallPrint"]))
    doc.build(story)
    print(f"Generated: {product['filename']}")

if __name__ == "__main__":
    OUT = os.path.join(os.path.dirname(__file__), "pdfs")
    os.makedirs(OUT, exist_ok=True)
    for p in PRODUCTS:
        generate_pdf(p, OUT)
    print("\nAll PDFs generated.")
    print("Upload command:")
    print("  databricks fs cp part_a_customer_360/data/pdfs/ dbfs:/Volumes/fevm_master_classic_marcus_catalog/abmb_rfp_presentation/product_pdfs/ --recursive --profile fevm-master-classic-marcus")
