from monitor import measureFn
from dataGen import createStructured
import os
import json
from pymongo import MongoClient
from pymongo.database import Database

# -- General
data_pool = {}

def updateDataPool():
    '''
    Loads in data from the datapool into the global variable
    '''

    print("Collecting insertion data...")
    data_pool["struct_insert_one"] = createStructured(1_000_000)

    with open("datapool/structured.json", "r") as f:
        data_pool["struct_insert_many"] = json.load(f)

    print("Done")

def deleteCol(db: Database, col_name: str):
    '''
    Query to delete a collection
    '''
    db.drop_collection(col_name)

def createCol(db: Database, col_name: str, data: list):
    '''
    Query to create a collection
    '''
    db[col_name].insert_many(data)

# -- Structured Queries
def insertOneStruct(db: Database, col_name: str):
    '''
    Query to insert a single document
    '''
    db[col_name].insert_one(data_pool["struct_insert_one"])

def insertManyStruct(db: Database, col_name: str):
    '''
    Query to insert a large amount of data
    '''
    db[col_name].insert_many(data_pool["struct_insert_many"])

def readOneStruct(db: Database, col_name: str):
    '''
    Query to read a single middle data point
    '''
    total = db[col_name].count_documents({})
    middle_uid = total // 2
    db[col_name].find_one({"uid": middle_uid})

def readManyStruct(db: Database, col_name: str):
    '''
    Query to read all uid % 4 = 0
    '''
    db[col_name].find({"$expr": {"$eq": [{"$mod": ["$uid", 4]}, 0]}})

def updateOneStruct(db: Database, col_name: str):
    '''
    Query to update middle document age and DOB
    '''
    total = db[col_name].count_documents({})
    middle_uid = total // 2
    db[col_name].update_one(
        {"uid": middle_uid},
        {"$set": {"birthday": "1-1-2000", "age": 25}}
    )

def updateManyStruct(db: Database, col_name: str):
    '''
    Query to update the Calgary to Lethbridge in all documents
    '''
    db[col_name].update_many(
        {"address.city": "Calgary"},
        {"$set": {"address.city": "Lethbridge"}}
    )

def replaceOneStruct(db: Database, col_name: str):
    '''
    Query to insert a new document in the middle
    '''
    total = db[col_name].count_documents({})
    middle_uid = total // 2
    new_doc = data_pool["struct_insert_one"].copy()
    new_doc.pop("_id", None)
    db[col_name].replace_one(
        {"uid": middle_uid},
        new_doc
    )

def aggregateStruct(db: Database, col_name: str):
    '''
    Query to get the variance in all the ages in the dataset
    '''
    pipeline = [
        {"$group": {"_id": None, "stdDev": {"$stdDevPop": "$age"}}},
        {"$project": {"variance": {"$multiply": ["$stdDev", "$stdDev"]}}}
    ]
    db[col_name].aggregate(pipeline)

# -- Unstructured Queries TODO

# -- Management Functions
def collectMeasure(db: Database, col_name: str, data: list, fn: callable):
    '''
    This function aggregates the measurements collected for setting up a query
    '''
    measures = {}
    measures["delete"] = measureFn(deleteCol, 0.1, db, col_name)
    measures["create"] = measureFn(createCol, 0.1, db, col_name, data)
    measures[fn.__name__] = measureFn(fn, 0.1, db, col_name)
    return measures

def run(client: MongoClient, db_name: str):
    '''
    This function runs through all the data in a folder and runs the
    structured queries on the data.
    '''
    if db_name not in ["structured", "unstructured"]:
        raise TypeError("invalid database name")

    # lists everything
    filenames = os.listdir(db_name)

    # filter to include only json files
    filenames = [x for x in filenames if x.endswith("json")]

    # loop through datasets
    for filename in filenames:
        data = {}
        cum_results = {}
        path = f"{db_name}/{filename}"
        col_name = filename.split(".")[0]

        functions = [
            insertOneStruct, insertManyStruct, readOneStruct, readManyStruct,
            updateOneStruct, updateManyStruct, replaceOneStruct, aggregateStruct
        ]

        print("----------")
        print(f"Opening file {filename}...")
        # load in the data from the file
        with open(path, "r") as f:
            data = json.load(f)

        # loop through functions -> repeatedly run the test -> record metrics
        for fn in functions:
            for i in range(5):
                print(f"Running test {col_name}-{fn.__name__}-iteration:{i}")
                cum_results[f"{col_name}_{fn.__name__}_{i}"] = collectMeasure(client[db_name], col_name, data, fn)
        
        # save the results
        print(f"Saving file {filename}")
        with open(f"logs/{db_name}_{col_name}.json", "w") as f:
            json.dump(cum_results, f, indent=4)

if __name__ == "__main__":
    # generate the data in the data pool
    updateDataPool()

    # connect to mongodb
    client = MongoClient("mongodb://localhost:27017/")

    # run tests for all structured data tests
    run(client, "structured")