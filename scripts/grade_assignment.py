"""
DataOps Mentorship — Automated Grading Script
==============================================
Grades student dbt submissions for Weeks 1–5.

Usage:
    python scripts/grade_assignment.py --week 1
    python scripts/grade_assignment.py --week 2
    python scripts/grade_assignment.py --week 3
    python scripts/grade_assignment.py --week 4
    python scripts/grade_assignment.py --week 5
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
CATALOG_PATH = os.path.join(DBT_PROJECT_DIR, "target", "catalog.json")
DBT_PROJECT_YML = os.path.join(DBT_PROJECT_DIR, "dbt_project.yml")


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
    dbt_results = load_dbt_results()

    # ── Task 1.1: Load Seeds (15 pts) ───────────────────────
    # dbt seed results land in run_results.json when dbt seed is the last command run.
    # Check for all 5 seeds; fall back to a screenshot reminder if results are absent.
    seed_names = ["raw_customers", "raw_products", "raw_orders",
                  "raw_order_items", "raw_store_locations"]
    seed_results_found = (
        dbt_results is not None and
        any(
            any(s in r.get("unique_id", "") for s in seed_names)
            for r in dbt_results.get("results", [])
        )
    )
    if seed_results_found:
        seed_passed = all(
            any(
                s in r.get("unique_id", "") and r.get("status") in ("pass", "success")
                for r in dbt_results.get("results", [])
            )
            for s in seed_names
        )
        checks.append(("1.1", seed_passed,
                       ("✅ All 5 seeds loaded successfully (dbt seed results)"
                        if seed_passed else
                        "❌ Some seeds failed — check dbt seed output"),
                       10))
    else:
        checks.append(("1.1", False,
                       "⏳ Seeds not in dbt results — run `dbt seed` then re-grade", 10))

    # Screenshot deliverable (5 pts — manual check via file in week_1/)
    week1_dir = os.path.join(os.path.dirname(__file__), "..", "week_1")
    screenshot_found = any(
        f.lower().endswith((".png", ".jpg", ".jpeg"))
        for f in os.listdir(week1_dir)
        if os.path.isfile(os.path.join(week1_dir, f))
    )
    checks.append(("1.1", screenshot_found,
                   ("✅ dbt seed screenshot found in week_1/"
                    if screenshot_found else
                    "❌ No screenshot in week_1/ — add dbt seed output screenshot"),
                   5))

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
#  WEEK 5 GRADING
# ═════════════════════════════════════════════════════════════

def _find_schema_file(directory, base_names=("_schema.yml", "schema.yml")):
    """Return path to the first schema file that exists under `directory`."""
    for name in base_names:
        candidate = os.path.join(directory, name)
        if os.path.isfile(candidate):
            return candidate
    return os.path.join(directory, base_names[0])


def grade_week_5():
    """Grade Week 5: Hooks, Exposures, and Documentation."""
    report = []
    total = 0
    max_score = 0

    report.append("# 📊 Week 5 — Grade Report\n")
    report.append("## Hooks, Exposures, and Documentation\n")
    report.append("| Task | Check | Points | Status |")
    report.append("| :--- | :--- | :---: | :---: |")

    checks = []
    dbt_results = load_dbt_results()

    fct_path = os.path.join(DEV_DIR, "fct_order_details.sql")
    dim_path = os.path.join(DEV_DIR, "dim_customers.sql")

    # ── Task 5.1: Post-Hook Indexes (25 pts) ────────────────
    # fct_order_details must define a post_hook with two indexes
    checks.append(("5.1", *check_file_contains(
        fct_path,
        r"post_hook",
        "fct_order_details has post_hook in config"
    ), 3))

    checks.append(("5.1", *check_file_contains(
        fct_path,
        r"create\s+index\s+if\s+not\s+exists[^\n]*order_date",
        "fct_order_details index on order_date (IF NOT EXISTS)"
    ), 6))

    checks.append(("5.1", *check_file_contains(
        fct_path,
        r"create\s+index\s+if\s+not\s+exists[^\n]*customer_id",
        "fct_order_details index on customer_id (IF NOT EXISTS)"
    ), 6))

    # dim_customers must define a post_hook with an index on country
    checks.append(("5.1", *check_file_contains(
        dim_path,
        r"post_hook",
        "dim_customers has post_hook in config"
    ), 2))

    checks.append(("5.1", *check_file_contains(
        dim_path,
        r"create\s+index\s+if\s+not\s+exists[^\n]*country",
        "dim_customers index on country (IF NOT EXISTS)"
    ), 5))

    # Models must still build cleanly
    checks.append(("5.1", *check_dbt_result(
        dbt_results,
        "fct_order_details",
        "fct_order_details builds successfully"
    ), 3))

    # ── Task 5.2: Project-level GRANT Hook (10 pts) ─────────
    checks.append(("5.2", *check_file_contains(
        DBT_PROJECT_YML,
        r"\+post[_-]hook",
        "Project-level +post_hook defined in dbt_project.yml"
    ), 5))

    checks.append(("5.2", *check_file_contains(
        DBT_PROJECT_YML,
        r"grant\s+select\s+on\s+\{\{\s*this\s*\}\}",
        "GRANT SELECT ON {{ this }} present"
    ), 5))

    # ── Task 5.3: Exposures (25 pts) ────────────────────────
    exposures_path = os.path.join(DEV_DIR, "_exposures.yml")
    # Fall back to exposures.yml if the student named it without the underscore
    if not os.path.isfile(exposures_path):
        alt = os.path.join(DEV_DIR, "exposures.yml")
        if os.path.isfile(alt):
            exposures_path = alt

    checks.append(("5.3", *check_file_exists(exposures_path, "_exposures.yml exists"), 4))

    checks.append(("5.3", *check_file_contains(
        exposures_path,
        r"exposures:",
        "Top-level 'exposures:' key present"
    ), 2))

    checks.append(("5.3", *check_file_contains(
        exposures_path,
        r"name:\s*revenue_dashboard",
        "revenue_dashboard exposure defined"
    ), 4))

    checks.append(("5.3", *check_file_contains(
        exposures_path,
        r"name:\s*inventory_report",
        "inventory_report exposure defined"
    ), 4))

    # Use a bounded window (600 chars) after each depends_on: to avoid
    # one exposure's models satisfying another exposure's check.
    checks.append(("5.3", *check_file_contains(
        exposures_path,
        r"depends_on:[\s\S]{0,600}?ref\(['\"]fct_order_details['\"]\)",
        "depends_on includes ref('fct_order_details')"
    ), 3))

    checks.append(("5.3", *check_file_contains(
        exposures_path,
        r"depends_on:[\s\S]{0,600}?ref\(['\"]dim_customers['\"]\)",
        "depends_on includes ref('dim_customers')"
    ), 2))

    checks.append(("5.3", *check_file_contains(
        exposures_path,
        r"owner:[\s\S]*email:",
        "owner.email field filled in"
    ), 3))

    checks.append(("5.3", *check_file_contains(
        exposures_path,
        r"maturity:\s*(low|medium|high)",
        "maturity set to low/medium/high"
    ), 3))

    # ── Task 5.4: Model Documentation (25 pts) ──────────────
    stage_schema_path = _find_schema_file(STAGE_DIR)
    dev_schema_path = _find_schema_file(DEV_DIR)

    checks.append(("5.4", *check_file_exists(dev_schema_path, "models/dev schema file exists"), 2))

    # Every stage model should have a description in the stage schema file
    stage_models = ["stg_customers", "stg_products", "stg_orders",
                    "stg_order_items", "stg_store_locations"]
    stage_content = file_exists(stage_schema_path) or ""
    stage_descs = 0
    for model_name in stage_models:
        # Match "name: <model>" followed (within a short window) by "description:"
        if re.search(rf"name:\s*{model_name}\b[\s\S]{{0,400}}?description:", stage_content):
            stage_descs += 1
    if stage_descs == len(stage_models):
        checks.append(("5.4", True, f"✅ All {len(stage_models)} stage models have descriptions", 3))
    else:
        checks.append(("5.4", False,
                       f"❌ Only {stage_descs}/{len(stage_models)} stage models have descriptions", 3))

    dev_content = file_exists(dev_schema_path) or ""

    # fct_order_details model description
    checks.append(("5.4", *check_file_contains(
        dev_schema_path,
        r"name:\s*fct_order_details\b[\s\S]{0,400}?description:",
        "fct_order_details has model description"
    ), 1))

    # fct_order_details column descriptions — require at least 10 of the key columns documented
    fct_columns = ["order_item_id", "order_id", "order_date", "customer_id",
                   "product_id", "quantity", "unit_price", "discount_pct",
                   "net_amount", "total_amount"]
    fct_doc_count = 0
    for col in fct_columns:
        # Look for "- name: <col>" followed shortly by "description:"
        if re.search(rf"-\s*name:\s*{col}\b[\s\S]{{0,200}}?description:", dev_content):
            fct_doc_count += 1
    if fct_doc_count >= 10:
        checks.append(("5.4", True,
                       f"✅ fct_order_details has {fct_doc_count}/{len(fct_columns)} key columns documented", 10))
    elif fct_doc_count >= 6:
        checks.append(("5.4", False,
                       f"❌ fct_order_details has only {fct_doc_count}/{len(fct_columns)} key columns documented (need 10)", 10))
    else:
        checks.append(("5.4", False,
                       f"❌ fct_order_details column documentation missing ({fct_doc_count}/{len(fct_columns)})", 10))

    # dim_customers model + column documentation
    checks.append(("5.4", *check_file_contains(
        dev_schema_path,
        r"name:\s*dim_customers\b[\s\S]{0,400}?description:",
        "dim_customers has model description"
    ), 1))

    dim_columns = ["customer_id", "email", "country", "total_orders", "total_spent"]
    dim_doc_count = 0
    for col in dim_columns:
        if re.search(rf"-\s*name:\s*{col}\b[\s\S]{{0,200}}?description:", dev_content):
            dim_doc_count += 1
    if dim_doc_count >= len(dim_columns):
        checks.append(("5.4", True,
                       f"✅ dim_customers has all {len(dim_columns)} key columns documented", 5))
    else:
        checks.append(("5.4", False,
                       f"❌ dim_customers has only {dim_doc_count}/{len(dim_columns)} key columns documented", 5))

    # Verify descriptions aren't just placeholders.
    # YAML block scalars write "description: >" — strip those indicators before checking.
    _yaml_scalar = re.compile(r'^[>|][>|\-0-9]*$')
    _raw_desc = re.findall(r"description:\s*['\"]?([^\n'\"]{1,400})", dev_content)
    # Keep only entries that aren't YAML block-scalar markers (>, |, >-, |- …)
    checkable_descs = [d.strip() for d in _raw_desc if not _yaml_scalar.match(d.strip())]
    meaningful = [d for d in checkable_descs if len(d) > 20]
    if checkable_descs and len(meaningful) / max(len(checkable_descs), 1) >= 0.8:
        checks.append(("5.4", True, "✅ Descriptions are meaningful (not just placeholders)", 3))
    else:
        found = len(meaningful)
        total_d = len(checkable_descs)
        checks.append(("5.4", False,
                       f"❌ Only {found}/{total_d} descriptions are meaningful (>20 chars)", 3))

    # ── Task 5.5: Docs Site (15 pts) ────────────────────────
    # `dbt docs generate` writes catalog.json — we use it as the proof.
    if os.path.isfile(CATALOG_PATH):
        checks.append(("5.5", True, "✅ catalog.json found (dbt docs generate succeeded)", 8))
    else:
        checks.append(("5.5", False,
                       "❌ catalog.json not found — run `dbt docs generate`", 8))

    # Verify the catalog actually references the documented models
    if os.path.isfile(CATALOG_PATH):
        try:
            with open(CATALOG_PATH, "r", encoding="utf-8") as f:
                catalog = json.load(f)
            node_ids = " ".join(catalog.get("nodes", {}).keys())
            if "fct_order_details" in node_ids:
                checks.append(("5.5", True, "✅ fct_order_details present in docs catalog", 4))
            else:
                checks.append(("5.5", False, "❌ fct_order_details missing from catalog.json", 4))
        except (json.JSONDecodeError, OSError):
            checks.append(("5.5", False, "❌ catalog.json is malformed", 4))
    else:
        checks.append(("5.5", False, "❌ catalog.json not available — cannot verify models", 4))

    # Encourage students to keep screenshots in week_5/ submission folder
    week5_dir = os.path.join(os.path.dirname(__file__), "..", "week_5")
    screenshot_found = False
    if os.path.isdir(week5_dir):
        for entry in os.listdir(week5_dir):
            if entry.lower().endswith((".png", ".jpg", ".jpeg")):
                screenshot_found = True
                break
    if screenshot_found:
        checks.append(("5.5", True, "✅ Screenshot(s) found in week_5/", 3))
    else:
        checks.append(("5.5", False,
                       "❌ No screenshots in week_5/ (DAG / fct docs / exposure)", 3))

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
        "--week", type=int, required=True, choices=[1, 2, 3, 4, 5],
        help="Which week to grade (1, 2, 3, 4, or 5)"
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
    elif args.week == 5:
        print(grade_week_5())


if __name__ == "__main__":
    main()
