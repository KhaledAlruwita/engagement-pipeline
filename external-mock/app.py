from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict, Any

app = FastAPI()

class Event(BaseModel):
    event_id: int
    content_id: int
    user_id: int
    event_type: str
    event_ts: str
    duration_ms: int | None = None
    device: str | None = None
    content_type: str | None = None
    length_seconds: int | None = None
    engagement_seconds: float | None = None
    engagement_pct: float | None = None

@app.post("/ingest")
def ingest(events: List[Dict[str, Any]]):
    return {"received": len(events)}
