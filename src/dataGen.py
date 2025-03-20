from datetime import datetime
from dateutil.relativedelta import relativedelta
import json
import random
import uuid
import os

# -- General
def intToStr(q: int) -> str:
    '''
    Converts integer to letters. Radix change formula
    to convert base 10 -> base 26

    Indexing: 1-index

    Reverse: not reversed because it makes no difference
    - usually radix conversion needs to be reversed for correct
    '''

    chars = []

    # change base
    while True:
        q, r = divmod(q, 26)
        chars.append(chr(65 + r))

        if q == 0:
            break

        # adjust for 1 index
        q -= 1

    return "".join(chars)

def saveData(filename: str, data: list):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(f"{filename}", "w") as f:
        json.dump(data, f, indent=4)

def createJson(fnc: callable, size: int) -> list:
    data = []
    for i in range(size):
        data.append(fnc(i))

    return data
  
def bulkGenerate(func: callable, file_prefix: str, limit: int):
    i = 10

    while i <= limit:
        data = createJson(func, i)

        saveData(f"{file_prefix}_{i}.json", data)
        i = i*10

def createPoolData(fnc: callable, filename: str, size: int = 1_000_000):
    data = []
    for i in range(1_000_000, 1_000_000 + size):
        data.append(fnc(i))
    saveData(f"datapool/{filename}.json", data)

# -- Structured Data
def genName(n: int) -> tuple[str, str]:
    '''
    Generate a 3 character per first and last name full name
    '''

    # shift to start at Aaa Aaa
    n += 12_356_630

    # split name
    name = intToStr(n)
    first = len(name) // 2
    
    # format name and return
    return name[:first].capitalize(), name[first:].capitalize()

quadrant = ["NW", "NE", "SE", "SW"]
cities = [
    'Calgary', 'Edmonton', 'Halifax','Hamilton',
    'Kitchener', 'London', 'Montreal', 'Oshawa',
    'Ottawa', 'Quebec City', 'Saskatoon', 'Toronto',
    'Vancouver', 'Victoria', 'Windsor', 'Winnipeg'
]

def genAddr(idx: int) -> dict:
    '''
    Produce address by incrementing street then quadrant
    then finally city.

    ~7.86 mil unique options. Can make it more by increasing
    street number max.
    '''
    # cascade the quotient to increment next value
    q, street_num = divmod(idx, 131072)
    q, quad_num = divmod(q, 4)
    city_num = q & 15

    return {
        "street": f"{street_num} Street {quadrant[quad_num]}",
        "city": cities[city_num]
    }

def genBirthday(age: int, idx: int) -> datetime:
    """
    birthday = now - age - idx%256 days
    """
    days = idx & 255

    birthdate = datetime.now()\
            - relativedelta(years=age)\
            - relativedelta(days=days)

    return birthdate

def createStructured(idx: int) -> dict:
    """
    Mimics the data found for a user of a webservice.

    **Returns**:

    {
        uid:int,
        age:int,
        name:str,
        email:str,
        address: {street:str, city:str},
        birthday: str
    }
    """
    age = (idx & 127) + 8
    first, last = genName(idx)
    birthday = genBirthday(age, idx)

    return {
        "uid": idx,
        "age": age,
        "name": f"{first} {last}",
        "email": f"{first.lower()}.{last.lower()}{birthday.year}@mail.com",
        "address": genAddr(idx),
        "birthday": birthday.strftime("%d-%m-%Y"),
    }

# -- Unstructured Data
words = [
    'ex', 'elit', 'commodo', 'ipsum', 'aute', 'nisi', 'aliquip', 'occaecat', 'sunt',
    'officia', 'dolore', 'reprehenderit', 'mollit', 'consectetur', 'veniam', 'velit',
    'cillum', 'labore', 'esse', 'qui', 'laboris', 'dolor', 'nulla', 'in', 'do', 'eu',
    'tempor', 'ut', 'duis', 'sed', 'quis', 'voluptate', 'nostrud', 'pariatur.', 'adipiscing',
    'id', 'irure', 'anim', 'proident', 'cupidatat', 'et', 'laborum', 'incididunt', 'non',
    'ea', 'est', 'excepteur', 'exercitation', 'amet', 'ullamco', 'consequat', 'fugiat',
    'ad', 'culpa', 'sit', 'deserunt', 'magna', 'eiusmod', 'lorem', 'sint', 'aliqua', 'minim', 'enim'
]

def genComments(idx: int, num: int) -> list:
    comments = []

    for _ in range(num):
        comments.append( " ".join(
            random.choices(
                words,
                k=(idx & 15) + 5
            ))
        )
    
    return comments

def createUnstructured(idx: int) -> dict:
    data = {}

    # generate id
    data["uid"] = idx

    # generate archived status or exclude
    if not random.randint(0, 10):
        data["archived"] = True
    elif random.randint(0, 10):
        data["archived"] = False

    # generate comments
    if random.randint(0, 10):
        num_comments = idx & 3
        data["comments"] = genComments(idx, num_comments)
    
    # generate image
    if random.randint(0, 3):
        data["image"] = uuid.uuid4().hex[:12] + ".jpg"
    
    # generate likes
    if random.randint(0, 10):
        data["likes"] = idx & 65535

    # generate timestamp
    if random.randint(0, 10):
        data["timestamp"] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

    return data

# -- Run
if __name__ == "__main__":
    random.seed(0)

    # generate 10 -> 1 million data sets for structured and unstructured
    bulkGenerate(createStructured, "structured/data", 10_000_000)
    bulkGenerate(createUnstructured, "unstructured/data", 10_000_000)


    # create data that indexes from 1_000_000
    createPoolData(createStructured, "structured", 1_000_000)
    createPoolData(createUnstructured, "unstructured", 1_000_000)
