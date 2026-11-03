"""
Traffic generator. Sends random traffic and (optionally) cycles through failure scenarios.

  python load_generator.py                      # steady normal traffic
  python load_generator.py --rps 10 --cycle 60  # switch scenario every 60s
  python load_generator.py --scenario db_outage # fixed scenario
"""
import argparse, random, time
import requests

p = argparse.ArgumentParser()
p.add_argument("--url", default="http://localhost:8000")
p.add_argument("--rps", type=float, default=5)
p.add_argument("--cycle", type=int, default=0, help="seconds per scenario; 0 = no cycling")
p.add_argument("--scenario", default="normal")
a = p.parse_args()

CALLS = [  # (method, path, weight)
    ("GET", "/products", 30), ("GET", f"/orders/{{}}", 20), ("POST", "/login", 20),
    ("POST", "/checkout", 20), ("GET", "/health", 5),
]
CYCLE = ["normal", "error_spike", "normal", "slow", "normal", "db_outage", "normal",
         "payment_down", "normal", "memory_leak", "normal", "cascading", "normal", "brute_force"]

requests.post(f"{a.url}/chaos/{a.scenario}")
idx, last = 0, time.time()
while True:
    if a.cycle and time.time() - last > a.cycle:
        idx, last = (idx + 1) % len(CYCLE), time.time()
        requests.post(f"{a.url}/chaos/{CYCLE[idx]}")
        print(f"[scenario] -> {CYCLE[idx]}")
    m, path, _ = random.choices(CALLS, weights=[c[2] for c in CALLS])[0]
    try:
        r = requests.request(m, a.url + path.format(random.randint(1, 1000)), timeout=10)
        print(m, path, r.status_code)
    except Exception as e:
        print("request failed:", e)
    time.sleep(random.expovariate(a.rps))
