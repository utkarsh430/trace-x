"""Authentication and authorization primitives.

`docs/SECURITY.md` §3 keeps three planes independent: human RBAC, service
identity, and agent capability tokens. They are separate modules here for the
same reason they are separate planes -- a check that lives in the wrong one is a
check nobody audits.
"""

from trace_core.security.service_tokens import (
    ServiceToken,
    ServiceTokenVerifier,
    TokenConfigurationError,
)

__all__ = ["ServiceToken", "ServiceTokenVerifier", "TokenConfigurationError"]
