import logging
from typing import Any, Dict, List


logger = logging.getLogger(__name__)


def summarize(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Convert detailed rule results into a compact pass/fail report."""
    errors = []

    # Preserve invalid row identifiers in the summary so callers can inspect data.
    for rule in results:

        for field_result in rule["results"]:

            if not field_result["passed"]:
                errors.append(
                    {
                        "rule": rule["rule"],
                        "field": field_result["field"],
                        "invalid_count": field_result.get("invalid_count", 0),
                        "invalid_uids": field_result.get("uids", []),
                        "message": field_result.get("message"),
                    }
                )

    report = {
        "status": "FAILED" if errors else "PASSED",
        "errors": errors,
    }
    logger.info(
        "Validation summary generated with status '%s' and %d errors",
        report["status"],
        len(errors),
    )
    return report
