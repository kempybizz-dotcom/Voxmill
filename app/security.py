"""
VOXMILL SECURITY MODULE - WORLD CLASS
======================================
Prompt injection protection, input sanitization, and security validation

LAYERS:
- Input validation (injection, XSS, SQL)
- LLM sanitization
- Gibberish detection (pre-filter)
- Response validation
- Security event logging
"""

import re
import logging
from typing import Tuple, List

logger = logging.getLogger(__name__)


class SecurityValidator:
    """Security validation for user inputs"""
    
    # Dangerous patterns that indicate prompt injection attempts
    INJECTION_PATTERNS = [
        r'ignore\s+(previous|above|all)\s+instructions?',
        r'disregard\s+(previous|above|all)\s+instructions?',
        r'forget\s+(everything|all)\s+(you|that)\s+(know|learned)',
        r'you\s+are\s+now\s+a?\s*\w+',  # "you are now a..."
        r'act\s+as\s+(a|an)\s+\w+',
        r'pretend\s+(to\s+be|you\s+are)',
        r'system\s*:\s*',  # Trying to inject system messages
        r'<\s*system\s*>',
        r'\[system\]',
        r'<\|im_start\|>',  # ChatML injection
        r'<\|im_end\|>',
        r'```python',  # Code injection attempts
        r'exec\s*\(',
        r'eval\s*\(',
        r'__import__',
        r'os\.system',
        r'subprocess\.',
        r'reveal\s+your\s+(prompt|instructions|system)',
        r'what\s+(is|are)\s+your\s+(instructions|prompt|system)',
        r'show\s+me\s+your\s+(instructions|prompt)',
        r'repeat\s+(your|the)\s+(instructions|prompt)',
        r'api[_\s]?key',
        r'secret[_\s]?key',
        r'password',
        r'token',
    ]
    
    # Suspicious character sequences
    SUSPICIOUS_CHARS = [
        '\x00',  # Null bytes
        '\uffff',  # Unicode edge cases
        '<script',  # XSS attempts
        'javascript:',
        'data:text/html',
    ]
    
    # Maximum lengths to prevent token overflow
    MAX_QUERY_LENGTH = 500
    MAX_WORD_LENGTH = 50
    
    @classmethod
    def validate_input(cls, user_input: str) -> Tuple[bool, str, List[str]]:
        """
        Validate user input for security threats
        
        Returns:
            (is_safe, sanitized_input, threat_types)
        """
        
        if not user_input:
            return True, "", []
        
        threats_detected = []
        original_input = user_input
        
        # Check 1: Length validation
        if len(user_input) > cls.MAX_QUERY_LENGTH:
            threats_detected.append("excessive_length")
            logger.warning(f"Input exceeds max length: {len(user_input)} chars")
            # Truncate but don't reject
            user_input = user_input[:cls.MAX_QUERY_LENGTH]
        
        # Check 2: Suspicious characters
        for char in cls.SUSPICIOUS_CHARS:
            if char in user_input:
                threats_detected.append("suspicious_characters")
                logger.warning(f"Suspicious character detected: {repr(char)}")
                # Remove suspicious chars
                user_input = user_input.replace(char, '')
        
        # Check 3: Prompt injection patterns
        input_lower = user_input.lower()
        for pattern in cls.INJECTION_PATTERNS:
            if re.search(pattern, input_lower, re.IGNORECASE):
                threats_detected.append("prompt_injection_attempt")
                logger.warning(f"Prompt injection pattern detected: {pattern}")
                # This is a hard block - reject the input
                return False, "", threats_detected
        
        # Check 4: Excessive repetition (flooding attack)
        words = user_input.split()
        if len(words) > 10:
            # Count word frequency
            word_freq = {}
            for word in words:
                word_lower = word.lower()
                word_freq[word_lower] = word_freq.get(word_lower, 0) + 1
            
            # If any word appears >50% of the time, it's likely spam
            max_freq = max(word_freq.values())
            if max_freq > len(words) * 0.5:
                threats_detected.append("repetition_attack")
                logger.warning(f"Excessive repetition detected: {max_freq}/{len(words)}")
                return False, "", threats_detected
        
        # Check 5: Extremely long words (buffer overflow attempts)
        for word in words:
            if len(word) > cls.MAX_WORD_LENGTH:
                threats_detected.append("buffer_overflow_attempt")
                logger.warning(f"Excessively long word: {len(word)} chars")
                # Truncate the word
                user_input = user_input.replace(word, word[:cls.MAX_WORD_LENGTH])
        
        # Check 6: SQL injection patterns (if somehow someone tries)
        sql_patterns = [
            r"'\s*OR\s+'1'\s*=\s*'1",
            r";\s*DROP\s+TABLE",
            r"UNION\s+SELECT",
            r"--\s*$",
        ]
        for pattern in sql_patterns:
            if re.search(pattern, user_input, re.IGNORECASE):
                threats_detected.append("sql_injection_attempt")
                logger.warning(f"SQL injection pattern detected: {pattern}")
                return False, "", threats_detected
        
        # Check 7: Unicode normalization attacks
        # Normalize unicode to prevent homograph attacks
        try:
            import unicodedata
            normalized = unicodedata.normalize('NFKC', user_input)
            if normalized != user_input:
                threats_detected.append("unicode_manipulation")
                logger.warning("Unicode normalization applied")
                user_input = normalized
        except Exception as e:
            logger.error(f"Unicode normalization error: {e}")
        
        # If we detected threats but sanitized successfully
        is_safe = len([t for t in threats_detected if t in [
            "prompt_injection_attempt", 
            "sql_injection_attempt",
            "repetition_attack"
        ]]) == 0
        
        if threats_detected:
            logger.info(f"Input sanitized. Threats: {threats_detected}")
        
        return is_safe, user_input, threats_detected
    
    @classmethod
    def sanitize_for_llm(cls, text: str) -> str:
        """
        Additional sanitization before sending to LLM
        Removes any remaining risky patterns
        """
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Remove any remaining control characters
        text = ''.join(char for char in text if ord(char) >= 32 or char in '\n\r\t')
        
        # Limit consecutive punctuation
        text = re.sub(r'([!?.]){4,}', r'\1\1\1', text)
        
        return text
    
    @classmethod
    def is_obvious_gibberish(cls, text: str) -> bool:
        """
        Detect gibberish without expensive LLM call
        
        CHATGPT SPEC: Pre-filter to save money on spam
        
        Returns: True if obvious gibberish (no LLM needed)
        """
        
        if not text or len(text.strip()) == 0:
            return True
        
        text_clean = text.strip()
        text_lower = text_clean.lower()
        
        # ✅ WHITELIST: Common valid patterns that should NEVER be flagged as gibberish
        valid_patterns = [
            'add property',
            'remove property',
            'show portfolio',
            'reset portfolio',
            'refresh portfolio',
            'portfolio',
            'market overview',
            'market',
            'price',
            'value',
            'worth',
            'compare',
            'comparison',
            'vs',
            'rightmove',
            'zoopla',
            'property',
            'address',
            'postcode',
            'street',
            'road',
            'avenue',
            'lane',
            'square',
            'park',
            'gardens',
            'place',
            'close',
            'mews',
            'terrace',
            'knightsbridge',
            'mayfair',
            'belgravia',
            'chelsea',
            'kensington'
        ]
        
        # If any valid pattern is present, it's NOT gibberish
        if any(pattern in text_lower for pattern in valid_patterns):
            logger.debug(f"✅ Whitelist match - not gibberish: '{text_clean}'")
            return False
        
        # Pattern 1: Too short and all caps random letters (no real words)
        if text_clean.isupper() and len(text_clean) < 12:
            # Check if it's a known command or real word
            known_patterns = ['HELP', 'STOP', 'START', 'RESET', 'CONFIRM', 'CANCEL', 
                            'STATUS', 'VERIFY', 'PORTFOLIO', 'MARKET', 'PRICE']
            if not any(pattern in text_clean for pattern in known_patterns):
                # Check for vowels (real words have vowels)
                vowels = set('AEIOU')
                if not any(c in vowels for c in text_clean):
                    logger.info(f"🗑️ Gibberish pre-filter: No vowels in '{text_clean}'")
                    return True
        
        # Pattern 2: Repeated characters (more than 70% same char)
        if len(text_clean) > 3:
            char_freq = {}
            for char in text_clean.lower():
                if char.isalpha():
                    char_freq[char] = char_freq.get(char, 0) + 1
            
            if char_freq:
                max_freq = max(char_freq.values())
                total_alpha = sum(char_freq.values())
                
                if total_alpha > 0 and max_freq / total_alpha > 0.7:
                    logger.info(f"🗑️ Gibberish pre-filter: Excessive repetition in '{text_clean}'")
                    return True
        
        # Pattern 3: No vowels at all (and not a command)
        vowels = set('aeiouAEIOU')
        alpha_chars = [c for c in text_clean if c.isalpha()]
        
        if len(alpha_chars) >= 4:  # At least 4 letters
            if not any(c in vowels for c in alpha_chars):
                logger.info(f"🗑️ Gibberish pre-filter: No vowels in '{text_clean}'")
                return True
        
        # Pattern 4: Very short nonsense (1-3 chars that aren't numbers or known abbreviations)
        if len(text_clean) <= 3 and text_clean.isalpha():
            # Allow common abbreviations
            allowed_short = ['OK', 'YES', 'NO', 'WHY', 'HOW', 'WHO', 'PIN', 'ADD', 'VS']
            if text_clean.upper() not in allowed_short:
                logger.info(f"🗑️ Gibberish pre-filter: Too short '{text_clean}'")
                return True
        
        # Pattern 5: DISABLED - Consonant sequence check (was too aggressive)
        # Previously flagged "Knightsbridge" and other valid place names
        # Left disabled to prevent false positives on addresses
        
        # Pattern 6: Keyboard mashing detection (adjacent keys on QWERTY)
        keyboard_patterns = [
            'asdf', 'qwer', 'zxcv', 'hjkl', 'uiop', 'bnm',
            'fdsa', 'rewq', 'vcxz', 'lkjh', 'poiu', 'mnb',
            'sdfg', 'dfgh', 'fghj', 'ghjk', 'jkl;', 'wertyuiop'
        ]
        
        for pattern in keyboard_patterns:
            if pattern in text_lower and len(text_clean) < 15:
                logger.info(f"🗑️ Gibberish pre-filter: Keyboard mashing in '{text_clean}'")
                return True
        
        return False


class ResponseValidator:
    """Validate LLM responses for safety and quality"""
    
    @classmethod
    def validate_response(cls, response: str) -> Tuple[bool, str]:
        """
        Validate LLM response for:
        - No leaked system prompts
        - No toxic content
        - No personal data exposure
        
        Returns: (is_safe, reason)
        """
        
        if not response:
            return False, "empty_response"
        
        response_lower = response.lower()
        
        # Check 1: System prompt leakage
        leaked_terms = [
            'system prompt',
            'your instructions are',
            'i was instructed to',
            'my training data',
            'as an ai language model',
            'openai',
            'anthropic',
        ]
        
        for term in leaked_terms:
            if term in response_lower:
                logger.warning(f"System prompt leakage detected: {term}")
                return False, "system_leakage"
        
        # Check 2: Credentials exposure (just in case)
        credential_patterns = [
            r'api[_\s]?key\s*[:=]\s*[\w-]+',
            r'password\s*[:=]\s*\w+',
            r'secret\s*[:=]\s*[\w-]+',
            r'token\s*[:=]\s*[\w-]+',
        ]
        
        for pattern in credential_patterns:
            if re.search(pattern, response_lower):
                logger.error(f"CRITICAL: Credential exposure in response!")
                return False, "credential_exposure"
        
        # Check 3: Excessive length (token overflow)
        if len(response) > 5000:
            logger.warning(f"Response too long: {len(response)} chars")
            return False, "excessive_length"
        
        # Check 4: Toxic content markers (light check - institutional tone allows strong language)
        # This is informational only, not blocking
        toxic_patterns = [
            r'\b(fuck|shit|damn|ass|bitch)\b',
        ]
        
        for pattern in toxic_patterns:
            if re.search(pattern, response, re.IGNORECASE):
                logger.info("Profanity detected in response (acceptable for institutional tone)")
        
        return True, "safe"


def log_security_event(event_type: str, details: dict):
    """Log security events for monitoring"""
    logger.warning(f"SECURITY EVENT: {event_type} | {details}")
    
    # In production, you could send to external monitoring
    # e.g., Sentry, DataDog, custom webhook
    
    try:
        from datetime import datetime, timezone
        from pymongo import MongoClient
        import os
        
        MONGODB_URI = os.getenv("MONGODB_URI")
        if MONGODB_URI:
            mongo_client = MongoClient(MONGODB_URI)
            db = mongo_client['Voxmill']
            
            security_log = {
                "event_type": event_type,
                "timestamp": datetime.now(timezone.utc),
                "details": details,
                "severity": "high" if event_type in ["prompt_injection", "sql_injection"] else "medium"
            }
            
            db['security_events'].insert_one(security_log)
    except Exception as e:
        logger.error(f"Failed to log security event to MongoDB: {e}")
