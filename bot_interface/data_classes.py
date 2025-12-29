from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional


@dataclass
class EventPacket:
    event: str
    bot_id: int
    type: str = ""
    data: Any = ""
    misc: Any = ""
    timestamp: str = ""
    message_id: str = ""
    media_id: str = ""
    wa_id: str = ""
    user_number: str = ""
    smj_id: Optional[int] = None
    state: Optional[str] = None
    context_id: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)
