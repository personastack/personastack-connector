#!/usr/bin/env bash
set -euo pipefail

go test ./internal/runtime -run 'Test(HermesAdapter|OpenClawAdapter)' -count=1
