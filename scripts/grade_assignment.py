import json
import os
import sys

# Configuration: Map unit test names to point values
# These should match the names defined in your unit_tests YAML files
GRADING_RUBRIC = {
    "unit_test:stg_customers_evaluation": 8,
    "unit_test:stg_products_evaluation": 8,
    "unit_test:stg_orders_evaluation": 8,
    "unit_test:stg_order_items_evaluation": 8,
    "unit_test:stg_store_locations_evaluation": 8,
    "unit_test:fct_order_details_evaluation": 20,
    "unit_test:dim_customers_evaluation": 10,
    # Note: Task 1.1 (Seeds) and 1.2 (Sources) are usually graded by presence 
    # but here we'll give partial credit if the STAGE layer passes
}

def grade():
    results_path = "dbt_learning/target/run_results.json"
    
    if not os.path.exists(results_path):
        print("## ❌ Error: dbt results not found.")
        print("Could not find `run_results.json`. Make sure dbt tests were executed.")
        return

    with open(results_path, "r") as f:
        data = json.load(f)

    results = data.get("results", [])
    total_score = 0
    max_score = sum(GRADING_RUBRIC.values())
    
    report = []
    report.append("# 📊 DataOps Mentorship — Grade Report\n")
    report.append("| Task / Test | Points | Status |")
    report.append("| :--- | :---: | :---: |")

    found_tests = {}

    for res in results:
        unique_id = res.get("unique_id", "")
        status = res.get("status", "")
        
        # Check if this test is in our rubric
        # dbt unique_ids for unit tests look like: unit_test.dbt_learning.model_name.test_name
        test_key = None
        for key in GRADING_RUBRIC:
            if key in unique_id:
                test_key = key
                break
        
        if test_key:
            points = GRADING_RUBRIC[test_key]
            if status == "pass":
                total_score += points
                status_icon = "✅"
            else:
                status_icon = "❌"
            
            display_name = test_key.split(":")[-1].replace("_", " ").title()
            report.append(f"| {display_name} | {points} | {status_icon} |")
            found_tests[test_key] = True

    # Add missing tests as failures
    for test_key, points in GRADING_RUBRIC.items():
        if test_key not in found_tests:
            display_name = test_key.split(":")[-1].replace("_", " ").title()
            report.append(f"| {display_name} | {points} | ⏳ Not Run |")

    report.append(f"\n## **Total Score: {total_score} / {max_score}**")
    
    if total_score == max_score:
        report.append("\n🎉 **Excellent Work! All automated checks passed.**")
    elif total_score >= (max_score * 0.7):
        report.append("\n🔵 **Good progress.** Review the failing tests to reach 100%.")
    else:
        report.append("\n🟡 **Keep trying.** Check the logic in your models.")

    print("\n".join(report))

if __name__ == "__main__":
    grade()
