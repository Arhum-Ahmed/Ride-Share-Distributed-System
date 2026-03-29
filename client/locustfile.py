import random
from locust import HttpUser, task, between

PICKUPS  = ["Airport", "City Hall", "Union Station", "Mall", "University"]
DROPOFFS = ["Hotel", "Hospital", "Stadium", "Office Park", "Suburb"]

class RiderUser(HttpUser):
    wait_time = between(0.5, 2)

    @task
    def submit_ride(self):
        self.client.post("/rides",
            json={"pickup": random.choice(PICKUPS), "dropoff": random.choice(DROPOFFS)},
            headers={"X-API-Key": "client-secret"},
        )