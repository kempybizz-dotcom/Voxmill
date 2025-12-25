"""
VOXMILL CONVERSATION MANAGER V2
================================
Session-aware conversations with multi-turn memory and context threading
NOW WITH IN-MEMORY FALLBACK for 99.9% reliability

WORLD-CLASS UPDATE:
- Dual-layer storage: Redis (primary) + in-memory (fallback)
- Zero dependency on Redis for core functionality
- Automatic entity extraction from conversations
"""

import os
import logging
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple
import json

logger = logging.getLogger(__name__)

# ============================================================
# REDIS CONNECTION (Primary Storage)
# ============================================================

REDIS_URL = os.getenv("REDIS_URL")
redis_client = None
redis_available = False

try:
    if REDIS_URL:
        import redis
        redis_client = redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=5)
        redis_client.ping()
        redis_available = True
        logger.info("✅ Redis connected for conversation storage")
    else:
        logger.warning("⚠️ REDIS_URL not configured - using in-memory conversation storage")
except ImportError:
    logger.error("❌ Redis library not installed")
    logger.warning("⚠️ Using in-memory conversation storage only")
except Exception as e:
    logger.error(f"❌ Redis connection failed: {e}")
    logger.warning("⚠️ Using in-memory conversation storage only")

# ============================================================
# IN-MEMORY STORAGE (Fallback)
# ============================================================

_memory_sessions = {}  # {client_id: session_dict}
_memory_sessions_lock = threading.Lock()
_last_cleanup = time.time()
CLEANUP_INTERVAL = 600  # Clean stale sessions every 10 minutes
SESSION_TTL = 3600  # 1 hour


def _cleanup_stale_sessions():
    """Remove expired sessions from memory"""
    global _last_cleanup
    
    now = time.time()
    
    if now - _last_cleanup < CLEANUP_INTERVAL:
        return
    
    with _memory_sessions_lock:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=SESSION_TTL)
        
        stale_keys = []
        for client_id, session in _memory_sessions.items():
            try:
                last_updated = datetime.fromisoformat(session['last_updated'])
                if last_updated < cutoff:
                    stale_keys.append(client_id)
            except:
                pass
        
        for key in stale_keys:
            del _memory_sessions[key]
        
        _last_cleanup = now
        
        if stale_keys:
            logger.debug(f"🗑️ Cleaned {len(stale_keys)} stale conversation sessions")


class ConversationSession:
    """Manages conversation state for a client with Redis + in-memory fallback"""
    
    SESSION_TTL = 3600  # 1 hour session timeout
    MAX_CONTEXT_MESSAGES = 5  # Keep last 5 exchanges
    
    def __init__(self, client_id: str):
        self.client_id = client_id
        self.session_key = f"voxmill:session:{client_id}"
        self.data_limitation_mentioned = False

    def has_mentioned_data_limitation(self):
        return self.data_limitation_mentioned

    def mark_data_limitation_mentioned(self):
        self.data_limitation_mentioned = True
    
    def get_session(self) -> Dict:
        """
        Get current session state with automatic fallback
        
        Priority:
        1. Try Redis (if connected)
        2. Fall back to in-memory storage
        3. Return empty session if neither has data
        """
        
        # TRY REDIS FIRST
        if redis_available and redis_client:
            try:
                session_data = redis_client.get(self.session_key)
                
                if session_data:
                    session = json.loads(session_data)
                    logger.debug(f"✅ REDIS: Loaded session for {self.client_id}: {len(session['messages'])} messages")
                    return session
                
            except Exception as e:
                logger.warning(f"Redis session read failed: {e}, trying memory")
        
        # FALLBACK TO IN-MEMORY
        _cleanup_stale_sessions()
        
        with _memory_sessions_lock:
            if self.client_id in _memory_sessions:
                session = _memory_sessions[self.client_id]
                
                # Check if stale
                try:
                    last_updated = datetime.fromisoformat(session['last_updated'])
                    age = (datetime.now(timezone.utc) - last_updated).total_seconds()
                    
                    if age < SESSION_TTL:
                        logger.debug(f"✅ MEMORY: Loaded session for {self.client_id}: {len(session['messages'])} messages")
                        return session
                    else:
                        # Stale, remove it
                        del _memory_sessions[self.client_id]
                except:
                    pass
        
        # NO SESSION FOUND - return empty
        logger.debug(f"❌ No session found for {self.client_id}, creating new")
        return self._empty_session()
    
    def update_session(self, user_message: str, assistant_response: str, 
                       metadata: Dict = None):
        """
        Add new exchange to session with dual storage
        
        CRITICAL: This must work even if Redis is down
        """
        
        try:
            session = self.get_session()
            
            # Add new exchange
            exchange = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'user': user_message,
                'assistant': assistant_response[:500],  # Truncate long responses
                'metadata': metadata or {}
            }
            
            session['messages'].append(exchange)
            
            # Keep only last N messages
            if len(session['messages']) > self.MAX_CONTEXT_MESSAGES:
                session['messages'] = session['messages'][-self.MAX_CONTEXT_MESSAGES:]
            
            # Update session metadata
            session['last_updated'] = datetime.now(timezone.utc).isoformat()
            session['total_exchanges'] += 1
            
            # CRITICAL: Extract and update contextual entities
            self._extract_context_entities(session, user_message, metadata)
            
            # SAVE TO REDIS (if available)
            if redis_available and redis_client:
                try:
                    redis_client.setex(
                        self.session_key,
                        self.SESSION_TTL,
                        json.dumps(session)
                    )
                    logger.debug(f"💾 REDIS: Session saved for {self.client_id}: {session['total_exchanges']} exchanges")
                except Exception as e:
                    logger.warning(f"Redis session write failed: {e}, using memory only")
            
            # ALWAYS SAVE TO MEMORY AS BACKUP
            with _memory_sessions_lock:
                _memory_sessions[self.client_id] = session
                logger.debug(f"💾 MEMORY: Session saved for {self.client_id}: {session['total_exchanges']} exchanges")
            
        except Exception as e:
            logger.error(f"Error updating session: {e}", exc_info=True)
    
    def get_conversation_context(self) -> str:
        """
        Generate conversation context string for LLM
        
        Returns formatted string of recent conversation history
        """
        
        session = self.get_session()
        
        if not session['messages']:
            return ""
        
        context_parts = ["CONVERSATION HISTORY (Last 5 exchanges):"]
        
        for i, msg in enumerate(session['messages'][-5:], 1):
            context_parts.append(f"\n[{i}] User: {msg['user'][:200]}")
            context_parts.append(f"    Assistant: {msg['assistant'][:200]}")
        
        # Add contextual entities
        if session['context_entities']:
            context_parts.append("\nREFERENCED ENTITIES:")
            
            if session['context_entities'].get('regions'):
                regions = ', '.join(session['context_entities']['regions'])
                context_parts.append(f"• Regions: {regions}")
            
            if session['context_entities'].get('agents'):
                agents = ', '.join(session['context_entities']['agents'])
                context_parts.append(f"• Agents: {agents}")
            
            if session['context_entities'].get('topics'):
                topics = ', '.join(session['context_entities']['topics'])
                context_parts.append(f"• Topics: {topics}")
        
        return "\n".join(context_parts)
    
    def get_last_mentioned_entities(self) -> Dict[str, List[str]]:
        """
        Get last-mentioned entities for auto-scoping
        
        Returns: {
            'regions': [...],
            'agents': [...],
            'topics': [...]
        }
        
        CRITICAL FOR MULTI-TURN CONVERSATIONS
        Used by conversational_governor for auto-scoping
        """
        
        session = self.get_session()
        
        if not session or not session.get('context_entities'):
            logger.debug(f"No entities found for {self.client_id}")
            return {
                'regions': [],
                'agents': [],
                'topics': []
            }
        
        entities = session['context_entities']
        
        logger.info(f"✅ Retrieved entities for {self.client_id}: "
                   f"regions={entities.get('regions', [])}, "
                   f"agents={entities.get('agents', [])}, "
                   f"topics={entities.get('topics', [])}")
        
        return {
            'regions': entities.get('regions', []),
            'agents': entities.get('agents', []),
            'topics': entities.get('topics', [])
        }
    
    def get_cross_session_summary(self, days: int = 7) -> str:
        """Get summary of key decisions/topics from past N days"""
        
        # This requires Redis pattern matching, skip if unavailable
        if not redis_available or not redis_client:
            return ""
        
        try:
            # Get all sessions from past N days
            pattern = f"{self.session_key}:*"
            keys = redis_client.keys(pattern)
            
            all_sessions = []
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)
            
            for key in keys:
                session_data = redis_client.get(key)
                if session_data:
                    session = json.loads(session_data)
                    session_time = datetime.fromisoformat(session['last_updated'])
                    
                    if session_time > cutoff:
                        all_sessions.append(session)
            
            if not all_sessions:
                return ""
            
            # Extract key topics and decisions
            key_topics = []
            key_decisions = []
            
            for session in all_sessions:
                for msg in session['messages']:
                    # Extract decision mode responses
                    if 'DECISION MODE' in msg.get('assistant', ''):
                        decision = msg['assistant'].split('RECOMMENDATION:')[1].split('PRIMARY RISK:')[0].strip()
                        key_decisions.append(f"- {decision[:100]}")
                    
                    # Extract monitoring requests
                    if 'MONITORING ACTIVE' in msg.get('assistant', ''):
                        key_topics.append("- Active monitoring directive")
            
            summary_parts = []
            
            if key_decisions:
                summary_parts.append(f"RECENT STRATEGIC DECISIONS (Last {days}d):")
                summary_parts.extend(key_decisions[:3])
            
            if key_topics:
                summary_parts.append(f"\nACTIVE INTELLIGENCE DIRECTIVES:")
                summary_parts.extend(key_topics[:5])
            
            return "\n".join(summary_parts) if summary_parts else ""
            
        except Exception as e:
            logger.warning(f"Cross-session summary failed: {e}")
            return ""
    
    def detect_followup_query(self, current_query: str) -> Tuple[bool, Dict]:
        """
        Detect if current query is a follow-up to previous conversation
        
        Returns: (is_followup, context_hints)
        """
        
        session = self.get_session()
        
        if not session['messages']:
            return False, {}
        
        # Follow-up indicators (EXPANDED - covers all executive shorthand)
        followup_patterns = [
            # Explicit continuation
            'what about', 'how about', 'compare', 'vs', 'versus',
            
            # Implicit references
            'that', 'those', 'them', 'it', 'this', 'these',
            'same', 'similar',
            
            # Temporal references
            'more', 'also', 'and', 'but', 'however',
            'last time', 'before', 'previously', 'earlier',
            
            # NEW: Implication/consequence triggers
            'so what', 'why', 'meaning', 'significance',
            'if i do nothing', 'if nothing', 'consequence',
            
            # NEW: Clarification requests
            'explain', 'clarify', 'elaborate', 'break down',
            
            # NEW: Challenge/verification
            'doesnt add up', 'seems off', 'make sense',
            
            # NEW: Single-word probes (only if >1 prior message)
            'why?', 'how?', 'when?', 'where?', 'who?'
        ]
        
        query_lower = current_query.lower()
        
        is_followup = any(pattern in query_lower for pattern in followup_patterns)
        
        if not is_followup:
            return False, {}
        
        # Extract context hints
        entities = session['context_entities']
        context_hints = {
            'last_region': entities.get('regions', [None])[-1] if entities.get('regions') else None,
            'last_agent': entities.get('agents', [None])[-1] if entities.get('agents') else None,
            'last_topic': entities.get('topics', [None])[-1] if entities.get('topics') else None,
            'last_query': session['messages'][-1]['user'] if session['messages'] else None
        }
        
        logger.info(f"✅ Follow-up query detected for {self.client_id}: {context_hints}")
        
        return True, context_hints
    
    def get_last_metadata(self) -> dict:
        """
        Get metadata from the last message in the conversation
        
        Returns: metadata dict or empty dict if no messages
        """
        try:
            session = self.get_session()
            
            if not session:
                logger.debug(f"No session found for {self.client_id}")
                return {}
            
            messages = session.get('messages', [])
            
            if not messages:
                logger.debug(f"No messages in session for {self.client_id}")
                return {}
            
            last_message = messages[-1]
            metadata = last_message.get('metadata', {})
            
            logger.debug(f"Retrieved metadata for {self.client_id}: {metadata}")
            return metadata
            
        except Exception as e:
            logger.error(f"Error getting last metadata for {self.client_id}: {e}", exc_info=True)
            return {}
    
    def _extract_context_entities(self, session: Dict, message: str, metadata: Dict = None):
        """
        Extract and track entities mentioned in conversation
        
        CRITICAL: This must work for multi-turn memory
        Extracts: regions, agents, topics from user messages
        """
        
        entities = session['context_entities']
        
        # Extract regions
        regions = ['Mayfair', 'Knightsbridge', 'Chelsea', 'Belgravia', 'Kensington', 
                  'South Kensington', 'Notting Hill', 'Marylebone']
        
        for region in regions:
            if region.lower() in message.lower():
                if region not in entities['regions']:
                    entities['regions'].append(region)
        
        # Extract agents (expanded list with fuzzy matching)
        agents = ['Knight Frank', 'Savills', 'Hamptons', 'Chestertons', 
                 'Foxtons', 'JLL', 'CBRE', 'Strutt & Parker', 'Strutt and Parker']
        
        for agent in agents:
            # Fuzzy matching (handle typos like "Knightsfrank")
            agent_normalized = agent.lower().replace(' ', '').replace('&', '')
            message_normalized = message.lower().replace(' ', '').replace('&', '')
            
            if agent_normalized in message_normalized:
                # Store canonical name
                canonical_name = agent.replace('Strutt and Parker', 'Strutt & Parker')
                if canonical_name not in entities['agents']:
                    entities['agents'].append(canonical_name)
        
        # Extract topics
        topics = ['price', 'inventory', 'cascade', 'opportunity', 'competitive', 
                 'trend', 'velocity', 'liquidity', 'forecast', 'segment', 'breakdown',
                 'competitor', 'agent', 'strategic']
        
        for topic in topics:
            if topic in message.lower():
                if topic not in entities['topics']:
                    entities['topics'].append(topic)
        
        # Extract from metadata (category becomes a topic)
        if metadata:
            if metadata.get('category'):
                category = metadata['category']
                if category not in entities['topics']:
                    entities['topics'].append(category)
        
        # Keep only last 3 of each entity type (prevent unbounded growth)
        for key in entities:
            if len(entities[key]) > 3:
                entities[key] = entities[key][-3:]
        
        logger.debug(f"✅ Extracted entities from message: "
                    f"regions={entities['regions']}, "
                    f"agents={entities['agents']}, "
                    f"topics={entities['topics']}")
    
    def clear_session(self):
        """Clear conversation session (start fresh)"""
        
        # Clear from Redis
        if redis_available and redis_client:
            try:
                redis_client.delete(self.session_key)
                logger.info(f"🗑️ Redis session cleared for {self.client_id}")
            except Exception as e:
                logger.warning(f"Redis session delete failed: {e}")
        
        # Clear from memory
        with _memory_sessions_lock:
            if self.client_id in _memory_sessions:
                del _memory_sessions[self.client_id]
                logger.info(f"🗑️ Memory session cleared for {self.client_id}")
    
    def _empty_session(self) -> Dict:
        """Create empty session structure"""
        
        return {
            'client_id': self.client_id,
            'created_at': datetime.now(timezone.utc).isoformat(),
            'last_updated': datetime.now(timezone.utc).isoformat(),
            'total_exchanges': 0,
            'messages': [],
            'context_entities': {
                'regions': [],
                'agents': [],
                'topics': []
            }
        }


def resolve_reference(query: str, context_hints: Dict) -> str:
    """
    Resolve ambiguous references in query using conversation context
    
    Example: "What about Chelsea?" → "What's the market overview for Chelsea?"
    """
    
    query_lower = query.lower()
    
    # Pattern: "what about [X]"
    if query_lower.startswith('what about ') or query_lower.startswith('how about '):
        # Check if X is a region
        for region in ['mayfair', 'chelsea', 'knightsbridge', 'belgravia', 'kensington']:
            if region in query_lower:
                # Assume they want same analysis as before, but for new region
                last_topic = context_hints.get('last_topic', 'market_overview')
                return f"Provide {last_topic} analysis for {region.title()}"
    
    # Pattern: "compare to [X]" or "vs [X]"
    if 'compare' in query_lower or ' vs ' in query_lower or 'versus' in query_lower:
        last_region = context_hints.get('last_region')
        if last_region:
            return f"{query} (previous context: {last_region})"
    
    # Pattern: "more like that" or "show me similar"
    if any(phrase in query_lower for phrase in ['more like', 'similar', 'same', 'that']):
        last_topic = context_hints.get('last_topic', 'opportunities')
        last_region = context_hints.get('last_region', 'Mayfair')
        return f"Show more {last_topic} for {last_region}"
    
    # Pattern: "what if [agent] does [X]" - needs agent context
    if 'what if' in query_lower and context_hints.get('last_agent'):
        # Already has context, return as-is
        return query
    
    return query


def generate_contextualized_prompt(base_prompt: str, session: ConversationSession) -> str:
    """Enhance LLM prompt with conversation context"""
    
    conversation_context = session.get_conversation_context()
    cross_session_context = session.get_cross_session_summary(days=7)
    
    if not conversation_context and not cross_session_context:
        return base_prompt
    
    enhanced_prompt = f"""
{conversation_context}

{cross_session_context if cross_session_context else ""}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CURRENT QUERY:
{base_prompt}
"""
    
    return enhanced_prompt


def get_session_analytics(client_id: str) -> Dict:
    """Get analytics about client's conversation patterns"""
    
    session = ConversationSession(client_id).get_session()
    
    if not session['messages']:
        return {'error': 'no_session_data'}
    
    # Analyze query patterns
    query_lengths = [len(msg['user']) for msg in session['messages']]
    
    topics_mentioned = session['context_entities']['topics']
    regions_mentioned = session['context_entities']['regions']
    
    return {
        'total_exchanges': session['total_exchanges'],
        'avg_query_length': round(sum(query_lengths) / len(query_lengths), 1) if query_lengths else 0,
        'unique_topics': len(set(topics_mentioned)),
        'unique_regions': len(set(regions_mentioned)),
        'session_duration_minutes': _calculate_session_duration(session),
        'engagement_level': _calculate_engagement_level(session)
    }


def _calculate_session_duration(session: Dict) -> int:
    """Calculate how long session has been active"""
    
    if not session['messages']:
        return 0
    
    first_msg = session['messages'][0]
    last_msg = session['messages'][-1]
    
    try:
        first_time = datetime.fromisoformat(first_msg['timestamp'])
        last_time = datetime.fromisoformat(last_msg['timestamp'])
        
        duration = (last_time - first_time).total_seconds() / 60
        return int(duration)
    except:
        return 0


def _calculate_engagement_level(session: Dict) -> str:
    """Determine engagement level based on conversation patterns"""
    
    total = session['total_exchanges']
    
    if total >= 10:
        return 'high'
    elif total >= 5:
        return 'medium'
    elif total >= 2:
        return 'low'
    else:
        return 'minimal'
