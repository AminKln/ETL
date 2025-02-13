import logging

from colorama import Fore, Style

logger = logging.getLogger(__name__)

def colorize(text, color, use_color=True):
    return f"{color}{text}{Style.RESET_ALL}" if use_color else text

def print_section_summary(section_name, section_data, console_output, file_output, use_color):
    total = section_data["total"]
    successes = section_data.get("success", [])
    failures = section_data.get("failure", {})
    skipped = section_data.get("skipped", [])

    # Summary line
    summary = f"{section_name}: {len(successes)}/{total} {section_name.lower()} successful."
    console_output.append(
        colorize(summary, Fore.GREEN, use_color) if len(successes) == total 
        else colorize(summary, Fore.YELLOW, use_color) if len(successes) != 0
        else colorize(summary, Fore.RED, use_color)
    )
    file_output.append(summary)

    # Print Successful
    if successes:
        console_output.append(colorize("-- Successful Tables:", Fore.GREEN, use_color))
        file_output.append("-- Successful Tables:")
        for table in successes:
            success_detail = f"   - {table}"
            console_output.append(colorize(success_detail, Fore.GREEN, use_color))
            file_output.append(success_detail)

    # Print Failed
    if failures:
        console_output.append(colorize("-- Failed Tables:", Fore.RED, use_color))
        file_output.append("-- Failed Tables:")
        for table, reason in failures.items():
            failure_detail = f"   - {table}: {reason}"
            console_output.append(colorize(failure_detail, Fore.RED, use_color))
            file_output.append(failure_detail)

    # Print Skipped (if applicable)
    if skipped:
        console_output.append(colorize("-- Skipped Tables:", Fore.YELLOW, use_color))
        file_output.append("-- Skipped Tables:")
        for table in skipped:
            skipped_detail = f"   - {table}"
            console_output.append(colorize(skipped_detail, Fore.YELLOW, use_color))
            file_output.append(skipped_detail)

def generate_report(report, use_color=True):
    """
    Generates a structured report summarizing the ETL pipeline and validations.
    Outputs the report to the console with optional colors and writes it to a log file.

    Args:
        report (dict): The pipeline report dictionary.
        use_color (bool): Whether to use colors in the console output.
    """
    console_output = []  # Collect all lines for console output
    file_output = []     # Collect all lines for file output

    def colorize(text, color):
        return f"{color}{text}{Style.RESET_ALL}" if use_color else text

    console_output.append("\n===== ETL Pipeline Report =====\n")
    file_output.append("===== ETL Pipeline Report =====\n")

    # Executed Summary
    print_section_summary(
        "Executed",
        report["executed"],
        console_output,
        file_output,
        use_color
    )

    # Table Creation Summary
    print_section_summary(
        "Table Creation",
        report["table_creation"],
        console_output,
        file_output,
        use_color
    )

    # Post Processing Summary
    print_section_summary(
        "Post Processing",
        report["post_processing"],
        console_output,
        file_output,
        use_color
    )

    # Primary Key Validation Summary
    print_section_summary(
        "Primary Key Validation",
        report["primary_key_validation"],
        console_output,
        file_output,
        use_color
    )

    # Custom Tests Summary
    # print_section_summary(
    #     "Custom Tests",
    #     report["custom_tests"],
    #     console_output,
    #     file_output,
    #     use_color
    # )

    # Overall Summary
    console_output.append("\n===== Pipeline Summary =====")
    file_output.append("\n===== Pipeline Summary =====")
    total_successes = (
        len(report["executed"]["success"]) +
        len(report["table_creation"]["success"]) +
        len(report["primary_key_validation"]["success"])
    )
    if total_successes == sum(x['total'] for x in report.values()):
        pass_message = "Pipeline Status: PASS"
        console_output.append(colorize(pass_message, Fore.GREEN))
        file_output.append(pass_message)
        logger.info("Pipeline completed successfully.")
    else:
        fail_message = "Pipeline Status: FAIL"
        console_output.append(colorize(fail_message, Fore.RED))
        file_output.append(fail_message)
        logger.error("Pipeline completed with errors.")

    # Print to console
    for line in console_output:
        print(line)

    # Write to file
    with open("logs/pipeline_report.log", "w") as report_file:
        for line in file_output:
            report_file.write(line + "\n")