import asyncio
import json
import httpx
from fastapi import FastAPI, Request
from pathlib import Path
import uvicorn
import threading
from datetime import datetime

"""
==================================================================================================
🚀 LAUNCHPAD INTEGRATION SCRIPT FOR FUTURE LLMs 🚀

Context for AI Agents:
This script acts as the "Decoupled Webhook Listener" for the GenAI Launchpad boilerplate.
The architecture works like this:
1. This external project sends a JSON payload to the Launchpad API (`LAUNCHPAD_API_URL`).
2. The Launchpad API saves the event to its Supabase database and queues a Celery background task.
3. The Celery worker runs the AI workflow (e.g. OpenAI/Anthropic processing).
4. When finished, the worker updates the Row in the Supabase `events` table with the result.
5. A Database Trigger in Supabase fires an HTTP Webhook matching this script's `LISTENER_PORT`/webhook endpoint.
6. This script catches the webhook, saves the final result to disk, and cleanly shuts down.

Requirements for Integration into a new project:
- If running this across the internet (instead of localhost), you must update the Supabase Database 
  Trigger to point to the new public IP/domain of this listener server.
- The Launchpad payload must match the Pydantic schemas expected by the Launchpad event router.

Required Dependencies (add these to your requirements.txt):
fastapi
uvicorn
httpx
"""

# =================CONFIGURATION=================
# Update these URLs depending on where the Launchpad Docker container is running
LAUNCHPAD_API_URL = "http://localhost:8080/events/"
# The port this script will temporarily open to listen for the Supabase Webhook
LISTENER_PORT = 9000
# ===============================================

app = FastAPI()

# Examples demonstrating the expected structure of a Launchpad Incoming Event
# Update these payloads to match the specific Pydantic models the Launchpad instance is expecting.
EXAMPLES = {
    "product": {
        "from_email": "sarah.smith@example.com",
        "to_email": "support@techgear.com",
        "sender": "Sarah Smith",
        "subject": "Compatibility question",
        "body": "Hi TechGear support, I'm considering buying your new TechGear SmartHome Hub, but I'm not sure if it's compatible with my existing smart devices. I have Philips Hue lights, a Nest thermostat, and Amazon Echo devices. Can you confirm if the SmartHome Hub will work with these? Thanks in advance for your help!",
    },
    "angry_customer": {
        "from_email": "angry.user@example.com",
        "to_email": "support@techgear.com",
        "sender": "Angry User",
        "subject": "My order is delayed AGAIN",
        "body": "This is ridiculous. My order #12345 has been delayed three times now. I demand a refund and I'm never buying from you again!",
    },
    "technical_issue": {
        "from_email": "tech.guy@example.com",
        "to_email": "support@techgear.com",
        "sender": "Tech Guy",
        "subject": "App crashing on launch",
        "body": "The latest iOS update is completely broken. Every time I open the app, it instantly crashes. Please fix this ASAP.",
    },
}

# We'll use this event to pause the main thread until the webhook is received
webhook_received_event = None
main_loop = None
final_response_data = {}


@app.post("/webhook")
async def handle_webhook(request: Request):
    """Endpoint that Supabase will call when the event is updated"""
    global final_response_data

    try:
        payload = await request.json()
    except Exception:
        print("Received webhook but could not parse JSON.")
        return {"status": "error"}

    # pg_net wraps the trigger payload with 'record' for the new row
    record = payload.get("record", {})
    task_context = record.get("task_context")

    if task_context:
        print(
            f"\n✅ Webhook caught! AI Workflow completed for Event ID: {record.get('id')}"
        )
        final_response_data = task_context
        # Signal the main thread that we got the data!
        if main_loop and webhook_received_event:
            main_loop.call_soon_threadsafe(webhook_received_event.set)
    else:
        print("\n⚠️ Received a database update, but no task_context was found.")

    return {"status": "success"}


async def send_event_and_wait():
    """Sends the event to Launchpad and waits for the webhook"""
    # Pick which hardcoded example to use here
    example_key = "product"

    if example_key not in EXAMPLES:
        print(f"Could not load event data for key: {example_key}")
        return

    payload = EXAMPLES[example_key]

    print(
        f"\n🚀 Sending hardcoded event ({example_key}) to Launchpad API at {LAUNCHPAD_API_URL}"
    )

    async with httpx.AsyncClient() as client:
        try:
            # IMPORTANT FOR FUTURE LLMs: If the Launchpad API is using FastAPI, ensure the URL ends
            # with a trailing slash to avoid 307 Temporary Redirects dropping the POST body.
            response = await client.post(LAUNCHPAD_API_URL, json=payload)
            print(f"Status from Launchpad: {response.status_code}")
            print(f"Task Queued: {json.loads(response.text)}")
        except Exception as e:
            print(f"Failed to reach Launchpad API: {e}")
            return

    print("\n⏳ Waiting for AI workflow to complete and trigger webhook...")

    # Pause here until the @app.post("/webhook") endpoint calls webhook_received_event.set()
    await webhook_received_event.wait()

    # Save the captured data to a local file with a timestamp
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = Path(__file__).parent / f"response_{timestamp_str}.json"
    with open(output_file, "w") as f:
        json.dump(final_response_data, f, indent=2)

    print(f"\n💾 Success! The final response was written to: {output_file.name}")


def run_server(loop):
    """Runs the FastAPI server inside its own thread"""
    asyncio.set_event_loop(loop)
    # Start the local uvicorn server on the configured LISTENER_PORT
    config = uvicorn.Config(app, host="0.0.0.0", port=LISTENER_PORT, log_level="error")
    server = uvicorn.Server(config)
    loop.run_until_complete(server.serve())


async def main():
    global webhook_received_event, main_loop
    main_loop = asyncio.get_running_loop()
    webhook_received_event = asyncio.Event()

    # 1. Create a separate event loop for the web server
    server_loop = asyncio.new_event_loop()

    # 2. Start Uvicorn in a background thread so it doesn't block our script
    server_thread = threading.Thread(
        target=run_server, args=(server_loop,), daemon=True
    )
    server_thread.start()

    # Give the server a tiny fraction of a second to spin up
    await asyncio.sleep(1)

    # 3. Fire the event and pause this main thread until the webhook hits!
    await send_event_and_wait()

    # 4. As soon as send_event_and_wait finishes, the script naturally exits,
    # which automatically kills the daemon background server thread!
    print("👋 Shutting down listener and exiting script.")


if __name__ == "__main__":
    # We run our main logic which orchestrates both the server and the HTTP request
    asyncio.run(main())
