"""
DataOps Mentorship — Automated Grading Script
==============================================
Grades student dbt submissions for Weeks 1–4.

Usage:
    python scripts/grade_assignment.py --week 1
    python scripts/grade_assignment.py --week 2
    python scripts/grade_assignment.py --week 3
    python scripts/grade_assignment.py --week 4
"""

import argparse
import io
import json
import os
import re
import sys

# Fix Unicode output on Windows terminals
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf-8", errors="replace"
    )

# ── Paths ─────────────────────────────────────────────────────
DBT_PROJECT_DIR = os.path.join(os.path.dirname(__file__), "..", "dbt_learning")
MODELS_DIR = os.path.join(DBT_PROJECT_DIR, "models")
STAGE_DIR = os.path.join(MODELS_DIR, "stage")
DEV_DIR = os.path.join(MODELS_DIR, "dev")
SNAPSHOTS_DIR = os.path.join(DBT_PROJECT_DIR, "snapshots")
TESTS_DIR = os.path.join(DBT_PROJECT_DIR, "tests")
DOCS_DIR = os.path.join(DBT_PROJECT_DIR, "docs")
MACROS_DIR = os.path.join(DBT_PROJECT_DIR, "macros")
RESULTS_PATH = os.path.join(DBT_PROJECT_DIR, "target", "run_results.json")


# ═════════════════════════════════════════════════════════════
#  HELPERS
# ═════════════════════════════════════════════════════════════

def file_exists(path):
    """Check if a file exists and return its content if so."""
    full = os.path.normpath(path)
    if os.path.isfile(full):
        with open(full, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    return None


def check_file_exists(path, label):
    """Return (pass, message) for file existence check."""
    content = file_exists(path)
    if content is not None:
        return True, f"✅ {label} — file found"
    return False, f"❌ {label} — file NOT found"


def check_file_contains(path, pattern, label, case_insensitive=True):
    """Check if file content matches a regex pattern."""
    content = file_exists(path)
    if content is None:
        return False, f"❌ {label} — file not found"
    flags = re.IGNORECASE if case_insensitive else 0
    if re.search(pattern, content, flags):
        return True, f"✅ {label}"
    return False, f"❌ {label}"


def check_word_count(path, min_words, label):
    """Check if a file has at least min_words words."""
    content = file_exists(path)
    if content is None:
        return False, f"❌ {label} — file not found"
    words = len(content.split())
    if words >= min_words:
        return True, f"✅ {label} ({words} words)"
    return False, f"❌ {label} — only {words} words (need {min_words}+)"


def load_dbt_results():
    """Load dbt run_results.json if available."""
    if os.path.isfile(RESULTS_PATH):
        with open(RESULTS_PATH, "r") as f:
            return json.load(f)
    return None


def check_dbt_result(results_data, model_unique_id_fragment, label):
    """Check if a specific model/test passed in dbt results."""
    if results_data is None:
        return False, f"⏳ {label} — no dbt results found (run dbt first)"

    for res in results_data.get("results", []):
        uid = res.get("unique_id", "")
        if model_unique_id_fragment in uid:
            status = res.get("status", "")
            if status in ("pass", "success"):
                return True, f"✅ {label}"
            else:
                msg = res.get("message", "")[:80] if res.get("message") else ""
                return False, f"❌ {label} — status: {status} {msg}"

    return False, f"⏳ {label} — not found in dbt results"


# ═════════════════════════════════════════════════════════════
#  WEEK 1 GRADING
# ═════════════════════════════════════════════════════════════

def grade_week_1():
    """Grade Week 1: Sources, Models, and Seeds."""
    report = []
    total = 0
    max_score = 0

    report.append("# 📊 Week 1 — Grade Report\n")
    report.append("## Sources, Models, and Seeds\n")
    report.append("| Task | Check | Points | Status |")
    report.append("| :--- | :--- | :---: | :---: |")

    checks = []

    # ── Task 1.2: Sources (15 pts) ──────────────────────────
    sources_path = os.path.join(STAGE_DIR, "sources.yml")
    checks.append(("1.2", *check_file_exists(sources_path, "sources.yml exists"), 5))
    checks.append(("1.2", *check_file_contains(sources_path, r"name:\s*RAW", "Sources reference RAW schema"), 5))
    checks.append(("1.2", *check_file_contains(sources_path, r"raw_customers", "raw_customers declared"), 5))

    # ── Task 1.3: Stage Models (40 pts) ─────────────────────
    stage_models = [
        ("stg_customers.sql", 8),
        ("stg_products.sql", 8),
        ("stg_orders.sql", 8),
        ("stg_order_items.sql", 8),
        ("stg_store_locations.sql", 8),
    ]
    for model, pts in stage_models:
        path = os.path.join(STAGE_DIR, model)
        checks.append(("1.3", *check_file_exists(path, model), pts))

    # ── Task 1.4: Fact Model (20 pts) ───────────────────────
    fct_path = os.path.join(DEV_DIR, "fct_order_details.sql")
    checks.append(("1.4", *check_file_exists(fct_path, "fct_order_details.sql exists"), 5))
    checks.append(("1.4", *check_file_contains(fct_path, r"ref\s*\(\s*['\"]stg_", "Uses ref() to staged models"), 5))
    checks.append(("1.4", *check_file_contains(fct_path, r"gross_amount|net_amount|total_amount", "Calculated columns present"), 5))
    checks.append(("1.4", *check_file_contains(fct_path, r"join", "Has JOIN logic"), 5))

    # ── Task 1.5: Dimension Model (10 pts) ──────────────────
    dim_path = os.path.join(DEV_DIR, "dim_customers.sql")
    checks.append(("1.5", *check_file_exists(dim_path, "dim_customers.sql exists"), 5))
    checks.append(("1.5", *check_file_contains(dim_path, r"total_orders|total_spent", "Aggregation columns present"), 5))

    # ── Build report ────────────────────────────────────────
    for task, passed, message, points in checks:
        max_score += points
        earned = points if passed else 0
        total += earned
        status = f"{earned}/{points}"
        report.append(f"| {task} | {message} | {status} | {'✅' if passed else '❌'} |")

    _append_summary(report, total, max_score)
    return "\n".join(report)


# ═════════════════════════════════════════════════════════════
#  WEEK 2 GRADING
# ═════════════════════════════════════════════════════════════

def grade_week_2():
    """Grade Week 2: Materializations."""
    report = []
    total = 0
    max_score = 0

    report.append("# 📊 Week 2 — Grade Report\n")
    report.append("## Materializations\n")
    report.append("| Task | Check | Points | Status |")
    report.append("| :--- | :--- | :---: | :---: |")

    checks = []
    dbt_results = load_dbt_results()

    # ── Task 2.1: Materialization Doc (15 pts) ──────────────
    doc_path = os.path.join(DOCS_DIR, "materializations.md")
    checks.append(("2.1", *check_file_exists(doc_path, "materializations.md exists"), 5))
    checks.append(("2.1", *check_file_contains(doc_path, r"(?i)(table|view)", "Discusses table vs view"), 5))
    checks.append(("2.1", *check_word_count(doc_path, 200, "At least 200 words"), 5))

    # ── Task 2.2: Incremental Model (40 pts) ────────────────
    fct_path = os.path.join(DEV_DIR, "fct_order_details.sql")

    checks.append(("2.2", *check_file_contains(
        fct_path,
        r"materialized\s*=\s*['\"]incremental['\"]",
        "materialized='incremental' in config"
    ), 5))

    checks.append(("2.2", *check_file_contains(
        fct_path,
        r"unique_key\s*=\s*['\"]order_item_id['\"]",
        "unique_key='order_item_id'"
    ), 5))

    checks.append(("2.2", *check_file_contains(
        fct_path,
        r"is_incremental\s*\(\s*\)",
        "{% if is_incremental() %} block present"
    ), 10))

    checks.append(("2.2", *check_file_contains(
        fct_path,
        r"interval\s*['\"]3\s*days?['\"]",
        "3-day look-back window"
    ), 5))

    checks.append(("2.2", *check_file_contains(
        fct_path,
        r"order_date",
        "Filters on order_date"
    ), 5))

    # Check dbt run results for fct_order_details
    checks.append(("2.2", *check_dbt_result(
        dbt_results,
        "fct_order_details",
        "fct_order_details runs successfully"
    ), 10))

    # ── Task 2.3: Snapshot (30 pts) ─────────────────────────
    snap_path = os.path.join(SNAPSHOTS_DIR, "snap_products.sql")

    checks.append(("2.3", *check_file_exists(snap_path, "snap_products.sql exists"), 5))

    checks.append(("2.3", *check_file_contains(
        snap_path,
        r"strategy\s*=\s*['\"]check['\"]",
        "Uses strategy='check'"
    ), 5))

    checks.append(("2.3", *check_file_contains(
        snap_path,
        r"check_cols\s*=\s*\[\s*['\"]list_price['\"]",
        "check_cols includes list_price"
    ), 5))

    checks.append(("2.3", *check_file_contains(
        snap_path,
        r"check_cols.*is_active",
        "check_cols includes is_active"
    ), 5))

    checks.append(("2.3", *check_file_contains(
        snap_path,
        r"unique_key\s*=\s*['\"]product_id['\"]",
        "unique_key='product_id'"
    ), 5))

    checks.append(("2.3", *check_file_contains(
        snap_path,
        r"\{%\s*snapshot",
        "{% snapshot %} block syntax"
    ), 5))

    # ── Task 2.4: Simulation (15 pts) ───────────────────────
    # Simulation is manually verified — we check if snapshot ran
    checks.append(("2.4", *check_dbt_result(
        dbt_results,
        "snap_products",
        "Snapshot ran successfully (dbt snapshot)"
    ), 15))

    # ── Build report ────────────────────────────────────────
    for task, passed, message, points in checks:
        max_score += points
        earned = points if passed else 0
        total += earned
        status = f"{earned}/{points}"
        report.append(f"| {task} | {message} | {status} | {'✅' if passed else '❌'} |")

    _append_summary(report, total, max_score)
    return "\n".join(report)


# ═════════════════════════════════════════════════════════════
#  WEEK 3 GRADING
# ═════════════════════════════════════════════════════════════

def grade_week_3():
    """Grade Week 3: Data Tests."""
    report = []
    total = 0
    max_score = 0

    report.append("# 📊 Week 3 — Grade Report\n")
    report.append("## Data Tests\n")
    report.append("| Task | Check | Points | Status |")
    report.append("| :--- | :--- | :---: | :---: |")

    checks = []

    # ── Task 3.1: Generic Tests in YAML (30 pts) ────────────
    schema_path = os.path.join(STAGE_DIR, "schema.yml")

    checks.append(("3.1", *check_file_exists(schema_path, "schema.yml exists"), 5))

    checks.append(("3.1", *check_file_contains(
        schema_path,
        r"- unique",
        "Contains 'unique' tests"
    ), 3))

    checks.append(("3.1", *check_file_contains(
        schema_path,
        r"- not_null",
        "Contains 'not_null' tests"
    ), 3))

    checks.append(("3.1", *check_file_contains(
        schema_path,
        r"accepted_values",
        "Contains 'accepted_values' test"
    ), 3))

    checks.append(("3.1", *check_file_contains(
        schema_path,
        r"relationships",
        "Contains 'relationships' test"
    ), 3))

    # Check all models are listed
    for model_name in ["stg_customers", "stg_products", "stg_orders",
                        "stg_order_items", "stg_store_locations"]:
        checks.append(("3.1", *check_file_contains(
            schema_path,
            rf"name:\s*{model_name}",
            f"{model_name} declared in schema"
        ), 2))

    checks.append(("3.1", *check_file_contains(
        schema_path,
        r"version:\s*2",
        "Schema uses version 2"
    ), 3))

    # ── Task 3.2: Custom Singular Tests (35 pts) ────────────
    singular_tests = [
        ("test_no_future_orders.sql", 5),
        ("test_positive_quantities.sql", 5),
        ("test_valid_discount_range.sql", 5),
        ("test_positive_shipping.sql", 5),
        ("test_positive_cost_price.sql", 5),
    ]
    for test_file, pts in singular_tests:
        path = os.path.join(TESTS_DIR, test_file)
        checks.append(("3.2", *check_file_exists(path, f"{test_file} exists"), pts))

    # Check tests use ref()
    for test_file in ["test_no_future_orders.sql", "test_positive_quantities.sql",
                      "test_valid_discount_range.sql", "test_positive_shipping.sql",
                      "test_positive_cost_price.sql"]:
        path = os.path.join(TESTS_DIR, test_file)
        checks.append(("3.2", *check_file_contains(
            path,
            r"ref\s*\(",
            f"{test_file} uses ref()"
        ), 2))

    # ── Task 3.3: Quarantine Table (20 pts) ─────────────────
    quarantine_path = os.path.join(DEV_DIR, "quarantine_orders.sql")

    checks.append(("3.3", *check_file_exists(quarantine_path, "quarantine_orders.sql exists"), 5))

    checks.append(("3.3", *check_file_contains(
        quarantine_path,
        r"materialized\s*=\s*['\"]table['\"]",
        "Materialized as table"
    ), 5))

    checks.append(("3.3", *check_file_contains(
        quarantine_path,
        r"failure_reason|union\s+all",
        "Contains failure_reason or UNION ALL logic"
    ), 5))

    checks.append(("3.3", *check_file_contains(
        quarantine_path,
        r"ref\s*\(",
        "Uses ref() to reference staged models"
    ), 5))

    # ── Task 3.4: Data Quality Report (15 pts) ──────────────
    report_path = os.path.join(DOCS_DIR, "data_quality_report.md")

    checks.append(("3.4", *check_file_exists(report_path, "data_quality_report.md exists"), 5))
    checks.append(("3.4", *check_word_count(report_path, 200, "At least 200 words"), 5))
    checks.append(("3.4", *check_file_contains(
        report_path,
        r"(issue|fix|recommend|table|test)",
        "Contains issue analysis content"
    ), 5))

    # ── Build report ────────────────────────────────────────
    for task, passed, message, points in checks:
        max_score += points
        earned = points if passed else 0
        total += earned
        status = f"{earned}/{points}"
        report.append(f"| {task} | {message} | {status} | {'✅' if passed else '❌'} |")

    _append_summary(report, total, max_score)
    return "\n".join(report)


# ═════════════════════════════════════════════════════════════
#  WEEK 4 GRADING
# ═════════════════════════════════════════════════════════════

def grade_week_4():
    """Grade Week 4: Macros and Packages."""
    report = []
    total = 0
    max_score = 0

    report.append("# 📊 Week 4 — Grade Report\n")
    report.append("## Macros and Packages\n")
    report.append("| Task | Check | Points | Status |")
    report.append("| :--- | :--- | :---: | :---: |")

    checks = []
    dbt_results = load_dbt_results()

    # ── Task 4.1: Jinja Basics — Monthly Revenue (15 pts) ────
    fct_monthly_path = os.path.join(DEV_DIR, "fct_monthly_revenue.sql")

    checks.append(("4.1", *check_file_exists(fct_monthly_path, "fct_monthly_revenue.sql exists"), 3))

    checks.append(("4.1", *check_file_contains(
        fct_monthly_path,
        r"\{%\s*set\s+",
        "Uses {% set %} variable"
    ), 3))

    checks.append(("4.1", *check_file_contains(
        fct_monthly_path,
        r"\{%\s*for\s+",
        "Uses {% for %} loop"
    ), 4))

    checks.append(("4.1", *check_file_contains(
        fct_monthly_path,
        r"(jan|feb|mar|apr)_revenue",
        "Generates monthly revenue columns"
    ), 2))

    checks.append(("4.1", *check_dbt_result(
        dbt_results,
        "fct_monthly_revenue",
        "fct_monthly_revenue runs successfully"
    ), 3))

    # ── Task 4.2: Currency Converter Macro (30 pts) ──────────
    macro_currency_path = os.path.join(MACROS_DIR, "convert_currency.sql")

    checks.append(("4.2", *check_file_exists(macro_currency_path, "convert_currency.sql exists"), 5))

    checks.append(("4.2", *check_file_contains(
        macro_currency_path,
        r"\{%\s*macro\s+convert_currency",
        "Defines convert_currency macro"
    ), 5))

    checks.append(("4.2", *check_file_contains(
        macro_currency_path,
        r"OMR",
        "Handles OMR currency"
    ), 3))

    checks.append(("4.2", *check_file_contains(
        macro_currency_path,
        r"EUR",
        "Handles EUR currency"
    ), 3))

    checks.append(("4.2", *check_file_contains(
        macro_currency_path,
        r"2\.60|2\.6[^0-9]",
        "OMR rate is 2.60"
    ), 2))

    checks.append(("4.2", *check_file_contains(
        macro_currency_path,
        r"1\.08",
        "EUR rate is 1.08"
    ), 2))

    # Check macro is used in fct_order_details
    fct_path = os.path.join(DEV_DIR, "fct_order_details.sql")
    checks.append(("4.2", *check_file_contains(
        fct_path,
        r"convert_currency",
        "convert_currency used in fct_order_details"
    ), 5))

    checks.append(("4.2", *check_file_contains(
        fct_path,
        r"total_amount_usd",
        "total_amount_usd column present in fct_order_details"
    ), 5))

    # ── Task 4.3: Revenue Macro (20 pts) ─────────────────────
    macro_revenue_path = os.path.join(MACROS_DIR, "calculate_revenue.sql")

    checks.append(("4.3", *check_file_exists(macro_revenue_path, "calculate_revenue.sql exists"), 5))

    checks.append(("4.3", *check_file_contains(
        macro_revenue_path,
        r"\{%\s*macro\s+calculate_revenue",
        "Defines calculate_revenue macro"
    ), 5))

    checks.append(("4.3", *check_file_contains(
        fct_path,
        r"calculate_revenue",
        "calculate_revenue used in fct_order_details"
    ), 5))

    checks.append(("4.3", *check_dbt_result(
        dbt_results,
        "fct_order_details",
        "fct_order_details runs successfully"
    ), 5))

    # ── Task 4.4: dbt-utils Package (20 pts) ─────────────────
    packages_path = os.path.join(DBT_PROJECT_DIR, "packages.yml")

    checks.append(("4.4", *check_file_exists(packages_path, "packages.yml exists"), 3))

    checks.append(("4.4", *check_file_contains(
        packages_path,
        r"dbt.utils|dbt_utils",
        "packages.yml references dbt-utils"
    ), 2))

    # Check dbt_packages directory exists (dbt deps was run)
    dbt_packages_dir = os.path.join(DBT_PROJECT_DIR, "dbt_packages", "dbt_utils")
    if os.path.isdir(dbt_packages_dir):
        checks.append(("4.4", True, "✅ dbt deps installed dbt_utils", 5))
    else:
        checks.append(("4.4", False, "❌ dbt_utils not found in dbt_packages/ (run dbt deps)", 5))

    checks.append(("4.4", *check_file_contains(
        fct_path,
        r"generate_surrogate_key|dbt_utils\.generate",
        "generate_surrogate_key used in fct_order_details"
    ), 5))

    # Check for a second dbt-utils usage anywhere in models
    all_usages = set()
    for root, dirs, files in os.walk(MODELS_DIR):
        for fname in files:
            if fname.endswith(".sql"):
                fpath = os.path.join(root, fname)
                content = file_exists(fpath)
                if content:
                    all_usages.update(re.findall(r"dbt_utils\.(\w+)", content, re.IGNORECASE))

    if len(all_usages) >= 2:
        second_usage = True
        second_msg = f"✅ Multiple dbt-utils macros used: {', '.join(sorted(all_usages))}"
    else:
        second_usage = False
        second_msg = "❌ Second dbt-utils macro not found in models"

    checks.append(("4.4", second_usage, second_msg, 5))

    # ── Task 4.5: Macro Documentation (15 pts) ───────────────
    macros_yml_path = os.path.join(MACROS_DIR, "macros.yml")

    checks.append(("4.5", *check_file_exists(macros_yml_path, "macros.yml exists"), 3))

    checks.append(("4.5", *check_file_contains(
        macros_yml_path,
        r"name:\s*convert_currency",
        "convert_currency documented"
    ), 3))

    checks.append(("4.5", *check_file_contains(
        macros_yml_path,
        r"name:\s*calculate_revenue",
        "calculate_revenue documented"
    ), 3))

    checks.append(("4.5", *check_file_contains(
        macros_yml_path,
        r"arguments:",
        "Macro arguments documented"
    ), 3))

    checks.append(("4.5", *check_file_contains(
        macros_yml_path,
        r"description:",
        "Descriptions provided"
    ), 3))

    # ── Build report ────────────────────────────────────────
    for task, passed, message, points in checks:
        max_score += points
        earned = points if passed else 0
        total += earned
        status = f"{earned}/{points}"
        report.append(f"| {task} | {message} | {status} | {'✅' if passed else '❌'} |")

    _append_summary(report, total, max_score)
    return "\n".join(report)


# ═════════════════════════════════════════════════════════════
#  SHARED
# ═════════════════════════════════════════════════════════════

def _append_summary(report, total, max_score):
    """Append score summary and emoji feedback."""
    pct = (total / max_score * 100) if max_score > 0 else 0
    report.append(f"\n## **Total Score: {total} / {max_score}  ({pct:.0f}%)**")

    if pct >= 90:
        report.append("\n🟢 **Excellent Work!** Exceeds expectations.")
    elif pct >= 75:
        report.append("\n🔵 **Good progress.** Review the failing checks to reach 90%+.")
    elif pct >= 60:
        report.append("\n🟡 **Satisfactory.** Core tasks present but gaps remain.")
    else:
        report.append("\n🔴 **Needs Work.** Significant gaps — review the assignment instructions.")


# ═════════════════════════════════════════════════════════════
#  MAIN
# ═════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="DataOps Mentorship — Assignment Grader"
    )
    parser.add_argument(
        "--week", type=int, required=True, choices=[1, 2, 3, 4],
        help="Which week to grade (1, 2, 3, or 4)"
    )
    args = parser.parse_args()

    if args.week == 1:
        print(grade_week_1())
    elif args.week == 2:
        print(grade_week_2())
    elif args.week == 3:
        print(grade_week_3())
    elif args.week == 4:
        print(grade_week_4())


if __name__ == "__main__":
    main()
