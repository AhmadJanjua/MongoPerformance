import psutil
import threading
import time
import json

def systemConfig() -> str:
    '''
    Function returns a message containing system resource state
    '''
    # cpu related
    cpu_count = psutil.cpu_count()
    cpu_threads = cpu_count - psutil.cpu_count(logical=False)
    cpu_cores = cpu_count - cpu_threads
    msg = f"cpu cores: {cpu_cores}\n"
    msg += f"cpu threads: {cpu_threads}\n"

    cpu_freq = psutil.cpu_freq()
    msg += f"cpu min frequency: {cpu_freq.min} Mhz\n"
    msg += f"cpu max frequency: {cpu_freq.max} Mhz\n"

    # disk related
    disk = psutil.disk_io_counters()
    msg += f"disk read: {disk.read_bytes} B\n"
    msg += f"disk write: {disk.write_bytes} B\n"
    msg += f"disk read: {disk.read_time} ms\n"
    msg += f"disk write: {disk.read_time} ms\n"

    disk = psutil.disk_usage('/')
    msg += f"disk used: {disk.used} B\n"
    msg += f"disk free: {disk.free} B\n"

    # swap related
    swap = psutil.swap_memory()
    msg += f"swap used: {swap.used} B\n"
    msg += f"swap free: {swap.free} B\n"

    # virtual related
    virtual = psutil.virtual_memory()
    msg += f"virtual used: {virtual.used} B\n"
    msg += f"virtual free: {virtual.free} B\n"

    return msg

def sysSnapshot() -> dict:
    '''
    function that returns the status of resources as a dictionary
    '''
    # track the cpu
    cpu_freq = psutil.cpu_freq().current
    cpu_util = psutil.cpu_percent(interval=0.1)

    # track disk
    disk = psutil.disk_io_counters()
    disk_read_bytes = disk.read_bytes
    disk_write_bytes = disk.write_bytes
    disk_read_ms = disk.read_time
    disk_write_ms = disk.write_time

    disk = psutil.disk_usage('/')
    disk_used = disk.used
    disk_free = disk.free

    # track swap
    swap = psutil.swap_memory()
    swap_used = swap.used
    swap_free = swap.free

    # track virtual
    virtual = psutil.virtual_memory()
    virtual_used = virtual.used
    virtual_free = virtual.free

    return {
        "cpu_freq": cpu_freq,
        "cpu_util": cpu_util,
        "disk_read_b": disk_read_bytes,
        "disk_write_b": disk_write_bytes,
        "disk_read_t": disk_read_ms,
        "disk_write_t": disk_write_ms,
        "disk_used": disk_used,
        "disk_free":disk_free,
        "swap_used": swap_used,
        "swap_free": swap_free,
        "virtual_used": virtual_used,
        "virtual_free": virtual_free,
    }

def measureFn(fn: callable, interval: float = 0.2, *argv, **kwargv) -> dict:
    results = {}
    samples = []
    event = threading.Event()

    # function for thread to get system snapshot
    def getSnapshots():
        iteration = 1
        while not event.is_set():
            data = sysSnapshot()
            data["sample"] = iteration
            samples.append(data)
            iteration += 1
            time.sleep(interval)
    
    # get a baseline for system readings
    results["baseline"] = sysSnapshot()

    # start thread to measure performance
    snap_thread = threading.Thread(target=getSnapshots, daemon=True)
    snap_thread.start()

    # run function
    time_0 = time.time()
    fn(*argv, **kwargv)
    time_1 = time.time()

    # stop the snapshot thread
    event.set()
    snap_thread.join()

    # get the response time
    response_time = time_1 - time_0

    # combine everything
    results["samples"] = samples
    results["response_time"] = response_time

    return results

def examplefn():
    start = 0
    for i in range(50_000_000):
        start += 10

        if i % 4 == 0:
            start -= 1

if __name__ == '__main__':
    results = measureFn(examplefn)

    with open("logs/example.json", "w") as f:
        json.dump(results, f, indent=4)