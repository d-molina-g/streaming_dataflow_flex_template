from dataclasses import dataclass
from typing import List, Dict, Any

@dataclass
class EventSession:
    quote_id: str
    events: List[Dict[str, Any]]

    def count(self) -> int:
        return len(self.events)

    def summary(self) -> Dict[str, Any]:
        return {"quoteId": self.quote_id, "eventCount": self.count()}
