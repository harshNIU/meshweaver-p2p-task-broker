import psutil


def sample_load() -> dict[str, float]:
    """Return the current CPU and RAM usage percentages."""
    cpu = psutil.cpu_percent(interval=1)
    ram = psutil.virtual_memory().percent

    return {
        "cpu": cpu,
        "ram": ram,
    }