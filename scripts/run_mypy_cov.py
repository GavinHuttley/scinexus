"""Run mypy via API so coverage can trace the plugin."""

import sys

import mypy.api

result = mypy.api.run(sys.argv[1:])
sys.stdout.write(result[0])
sys.stderr.write(result[1])
sys.exit(result[2])
