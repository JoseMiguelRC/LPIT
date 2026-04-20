import time
import asyncio

# -----------------------------
# asyncio version
# -----------------------------

async def task_async(name: str, delay: float) -> None:
    print(f"- Task {name} started")
    await asyncio.sleep(delay) # Wait for some external task
    print(f"- Task {name} finished after {delay} s")

async def run_async() -> None:
    print("---------------------")    
    print("asyncio execution")
    print("---------------------\n")
    
    t0 = time.perf_counter()

    await asyncio.gather(
        task_async("A", 2),
        task_async("B", 3),
        task_async("C", 1),
    )

    t1 = time.perf_counter()
    print(f"\nTotal asyncio time: {t1 - t0:.2f} s")
    
# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    asyncio.run(run_async())    