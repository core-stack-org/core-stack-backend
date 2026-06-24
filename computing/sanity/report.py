def summarize(results):
    errors = []

    for rule in results:

        for field_result in rule["results"]:

            if not field_result["passed"]:
                errors.append(
                    {
                        "rule": rule["rule"],
                        "field": field_result["field"],
                        "invalid_count": field_result["invalid_count"],
                        "invalid_uids": field_result["uids"],
                    }
                )

    return {
        "status": "FAILED" if errors else "PASSED",
        "errors": errors,
    }
