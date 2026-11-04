#!/bin/bash
# Run `manage.py check` and the test suite on the target branch and on the
# pull request, and fail only if the pull request breaks `check` or adds
# failing tests.
# Usage: no_new_test_failures.sh <base dir> <head dir>
set -u
base="$1"
head="$2"

prepare() {
    # Migrations are git-ignored and generated at install time, the same
    # way as ensure_local_migration_packages in database-init.sh.
    find . -maxdepth 2 -name apps.py -type f | while IFS= read -r f; do
        mkdir -p "$(dirname "$f")/migrations"
        touch "$(dirname "$f")/migrations/__init__.py"
    done
    mkdir -p tmp logs
}

run_branch() {
    (
        cd "$1" || exit 1
        prepare
        if python manage.py check > check.txt 2>&1; then
            echo "manage.py check: ok"
        else
            echo "manage.py check: FAILED"
            tail -n 3 check.txt
        fi
        python manage.py makemigrations --skip-checks > /dev/null 2>&1
        python manage.py test --noinput -v 2 > suite.txt 2>&1
        # A test counts once, whether it errors or fails.
        grep -E '^(ERROR|FAIL):' suite.txt | sed -E 's/^(ERROR|FAIL): //' | sort -u > failing.txt
        grep -E '^Ran |^(OK|FAILED)' suite.txt
    )
}

check_ok() { tail -n 1 "$1/check.txt" | grep -q "no issues"; }

echo "Target branch:"
run_branch "$base"
echo "Pull request:"
run_branch "$head"

ran() { grep -q '^Ran ' "$1/suite.txt"; }
if ! ran "$head"; then
    if ! ran "$base"; then
        echo "Warning: the test suite can't run on the target branch either, so this isn't counted against the pull request:"
        tail -n 5 "$head/suite.txt"
        exit 0
    fi
    echo "The test suite does not run with this pull request:"
    tail -n 30 "$head/suite.txt"
    exit 1
fi
if ! ran "$base"; then
    echo "The test suite can't run on the target branch but runs with this pull request. Failing tests here, for information:"
    cat "$head/failing.txt"
    exit 0
fi

status=0
if check_ok "$base" && ! check_ok "$head"; then
    echo "This pull request breaks manage.py check:"
    cat "$head/check.txt"
    status=1
elif ! check_ok "$head"; then
    echo "Warning: manage.py check also fails on the target branch, so it isn't counted against this pull request."
fi

added=$(comm -13 "$base/failing.txt" "$head/failing.txt")
fixed=$(comm -23 "$base/failing.txt" "$head/failing.txt")
if [ -n "$fixed" ]; then
    echo "Now passing:"
    echo "$fixed"
fi
if [ -n "$added" ]; then
    echo "Failing only with this pull request:"
    echo "$added"
    echo "Full output of the pull request's run:"
    cat "$head/suite.txt"
    status=1
else
    echo "No new failing tests ($(wc -l < "$head/failing.txt" | tr -d ' ') already failing on the target branch)."
fi
exit $status
