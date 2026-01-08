from fastapi import FastAPI, Request
import random, time

app = FastAPI()

@app.post("/events")
async def ingest(req: Request):
    body = await req.json()
    #test latency and failures
    if random.random() < 0.2:
        time.sleep(2)          # slow
    if random.random() < 0.1:
        return {"status": "error"}  # fake failure

    return {"status": "ok"}
