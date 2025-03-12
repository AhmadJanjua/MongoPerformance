import random

# generates a base-26 string (only capital letters)
def intToStr(n: int) -> str:
    result = ""
    while True:
        n, remainder = divmod(n, 26)
        result = chr(65 + remainder) + result
        if n == 0:
            break
        n -= 1
    return result

# name should be two words each at least 3 characters
def genName(n: int) -> tuple[str, str]:
    n += 12356630
    name = intToStr(n)
    first = len(name) // 2
    return name[:first].capitalize(), name[first:].capitalize()

# letter number alternating sequence for 6 characters
def genPostal(n: int) -> str:
    s = intToStr(n%17576+702)
    n = str(100 + n % 900)
    return s[0] + n[0] + s[1] + " " + n[1] + s[2] +n[2]

# street, city and postal code
def genAddr(first: str, last: str, n: int) -> dict:
    return {
        "street": f"{n % 10000} {first} St",
        "city": f"{first[0]}{last[0]}",
        "postal": genPostal(n)
    }

# get a single record based on name, age, email and address
def genStructuredRecord(n: int) -> dict:
    first, last = genName(n)
    return {
        "name": f"{first} {last}",
        "age": 18 + (n % 72),
        "email": f"{first}.{last}@example.com",
        "address": genAddr(first, last, n)
    }

# generate num number of structured records
def genStructuredData(num: int) -> list:
    records = []
    for i in range(num):
        records.append(genStructuredRecord(i))

    return records

# generate randomly structured data
def genUnStructRecord(n: int) -> dict:
    record = {}

    # variable record size
    num_fields = (n % 9) + 1

    for i in range(num_fields):
        key = intToStr(n + i + 12356630)

        # int
        if (n + i) % 3 == 0:
            value = random.randint(0,10000)
        # float
        elif (n + i) % 3 == 1:
            value = random.random()*100
        # string
        else:
            value = intToStr(random.randint(12356630, 102356630))
        
        # update record
        record[key] = value

    return record

# generate list of unstructured data
def genUnStructData(num: int) -> list:
    records = []
    random.seed(1)
    for i in range(num):
        records.append(genUnStructRecord(i))

    return records