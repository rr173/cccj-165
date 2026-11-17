"""Two-party snapshot reconciliation over an untrusted transport.

The package deliberately has no signature or authentication primitives.  Both
parties exchange content commitments and verify that a locally applied
difference reconstructs the peer's commitment.
"""

__version__ = "1.0.0"
