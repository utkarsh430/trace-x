"""`python -m services.worker`: the outbox relay process."""

import sys

from services.worker.relay import main

sys.exit(main())
