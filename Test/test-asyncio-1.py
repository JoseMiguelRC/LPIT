import time
import asyncio

# -----------------------------
# Synchronous version
# -----------------------------

def task_sync(name: str, delay: float) -> None:
    print(f"- Task {name} started")
    time.sleep(delay) # Wait for some external task
    print(f"- Task {name} finished after {delay} s")

def run_sync() -> None:
    print("---------------------")
    print("Synchronous execution")
    print("---------------------\n")
    
    t0 = time.perf_counter()

    task_sync("A", 2)
    task_sync("B", 3)
    task_sync("C", 1)

    t1 = time.perf_counter()
    
    print(f"\nTotal synchronous time: {t1 - t0:.2f} s")
    
# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    run_sync()