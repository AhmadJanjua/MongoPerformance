import psutil
import threading
import time
import json

GiB = (1024**3)

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
    msg += f"disk read: {disk.read_bytes/GiB} GiB\n"
    msg += f"disk write: {disk.write_bytes/GiB} GiB\n"
    msg += f"disk read: {disk.read_time/1000} s\n"
    msg += f"disk write: {disk.read_time/1000} s\n"

    disk = psutil.disk_usage('/')
    msg += f"disk used: {disk.used/GiB} GiB\n"
    msg += f"disk free: {disk.free/GiB} GiB\n"

    # swap related
    swap = psutil.swap_memory()
    msg += f"swap total: {swap.total/GiB} GiB\n"
    msg += f"swap used: {swap.used/GiB} GiB\n"
    msg += f"swap free: {swap.free/GiB} GiB\n"

    # virtual related
    virtual = psutil.virtual_memory()
    msg += f"virtual total: {virtual.total/GiB} GiB\n"
    msg += f"virtual free: {virtual.available/GiB} GiB\n"
    msg += f"virtual used: {(virtual.total - virtual.available)/GiB} GiB\n"

    return msg

def sysSnapshot() -> dict:
    '''
    function that returns the status of resources as a dictionary
    '''
    # track the cpu
    cpu_util = psutil.cpu_percent(interval=0.1)

    # track disk
    disk = psutil.disk_io_counters()
    disk_read = disk.read_bytes/GiB
    disk_write = disk.write_bytes/GiB

    disk = psutil.disk_usage('/')
    disk_used = disk.used/GiB
    disk_free = disk.free/GiB

    # track virtual
    virtual = psutil.virtual_memory()
    virtual_free = virtual.available/GiB
    virtual_used = (virtual.total - virtual.available)/GiB

    return {
        "cpu_util": cpu_util,
        "disk_read": disk_read,
        "disk_write": disk_write,
        "disk_used": disk_used,
        "disk_free": disk_free,
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
    for i in range(50_000_00):
        start += 10

        if i % 4 == 0:
            start -= 1

if __name__ == '__main__':
    print(systemConfig())
    results = measureFn(examplefn)

    with open("logs/example.json", "w") as f:
        json.dump(results, f, indent=4)