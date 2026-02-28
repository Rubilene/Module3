"""
src/rbac.py
============
Role-Based Access Control helper.
"""
import logging
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import RBAC_ROLES

log = logging.getLogger("etl.rbac")


def check_rbac(role: str, required_permission: str) -> bool:
    permissions = RBAC_ROLES.get(role, [])
    granted = required_permission in permissions
    if not granted:
        log.debug(f"RBAC DENIED: role='{role}' | required='{required_permission}'")
    return granted
