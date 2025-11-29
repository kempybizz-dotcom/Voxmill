from pydantic import BaseModel
from typing import List, Optional, Dict, Any

class WhatsAppMessage(BaseModel):
    from_: str
    id: str
    timestamp: str
    type: str
    text: Optional[Dict[str, str]] = None

    class Config:
        fields = {'from_': 'from'}
