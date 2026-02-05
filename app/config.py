"""
Feature flags for gate control
All gates disabled by default except security
"""
import os

# ============================================================================
# FEATURE FLAGS - GATE CONTROL
# ============================================================================
# Set to False to disable gates during testing/development
# Only enable after core loop is proven stable

# Authentication & Access Control
ENABLE_PIN_GATE = False  # ← DISABLED (removing PIN entirely)

# Rate Limiting & Abuse Prevention  
ENABLE_SILENCE_MODE = False  # ← DISABLED (broken, adds friction)
ENABLE_REPEAT_DETECTION = False  # ← DISABLED (creates dead-ends)
ENABLE_ABUSE_SCORING_BLOCKS = False  # ← DISABLED (log only, don't block)

# Security (KEEP THESE ENABLED)
ENABLE_SECURITY_BLOCKS = True  # Prompt injection, jailbreak detection
ENABLE_BURST_LIMIT = True  # Basic flood protection (if proven stable)

# Logging
LOG_DISABLED_GATES = True  # Log when gates are skipped
