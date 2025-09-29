import os
from celery import Celery
from dotenv import load_dotenv
load_dotenv()

celery_app = Celery(
    "cod",
    broker=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    backend=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    include=[
        "app.orchestrator",        # orchestrator tasks
        "app.agents.parsing_agent" # parsing tasks
    ]
)
celery_app.conf.task_routes = {"app.orchestrator.*": {"queue": "orchestrator"},
                               "app.agents.*": {"queue": "agents"}}
