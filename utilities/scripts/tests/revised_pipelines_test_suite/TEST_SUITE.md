# Revised Pipelines Test Suite

This directory contains the cross-pipeline Python runner and Postman UAT assets
for facilities, Antyodaya, and livestock. It is test-only and can be applied on
top of any matching implementation branch.

- `local_pipeline_active_locations_test.py`: focused and scale pipeline runner
- `interactive_cli.py`: guided local runner
- `render_run_report.py`: report renderer for a completed run
- `core_stack_local_pipeline_uat.postman_collection.json`: generated API requests
- `core_stack_local_pipeline_uat.postman_environment.json`: UAT environment
- `README.md`: detailed test contract, commands, and latest focused evidence

Generated run reports remain under ignored `data/local_pipeline_test_runs/`.
