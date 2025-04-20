from monitor import measureFn
from dataGen import createStructured, genBirthday, createUnstructured
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

    data_pool["unstruct_insert_one"] = createUnstructured(1_000_000)

    with open("datapool/structured.json", "r") as f:
        data_pool["struct_insert_many"] = json.load(f)

    with open("datapool/unstructured.json", "r") as f:
        data_pool["unstruct_insert_many"] = json.load(f)

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


def insertManyThenDeleteManyStruct(db: Database, col_name: str):
    '''
    Complex query: Insert many → Delete users with age > 60
    '''
    db[col_name].insert_many(data_pool["struct_insert_many"])
    db[col_name].delete_many(
        {"age": {"$gt": 60}}
    )


def insertOneThenUpdateBirthdayStruct(db: Database, col_name: str):
    '''
    Complex query: Insert one user → Update that user's birthday
    '''
    # Step 1: Insert one user
    user = data_pool["struct_insert_one"]
    db[col_name].insert_one(user)

    # Step 2: Update that user's birthday
    new_birthday = genBirthday(user["age"], user["uid"]).strftime("%d-%m-%Y")

    db[col_name].update_one(
        {"uid": user["uid"]},
        {"$set": {"birthday": new_birthday}}
    )


def readThenDeleteOldUsersStruct(db: Database, col_name: str):
    '''
    Complex query: Read all users with age > 130, then delete them one by one
    '''
    # Step 1: Read all users with age > 130
    old_users = list(db[col_name].find({"age": {"$gt": 130}}, {"uid": 1}))

    # Step 2: Delete them one by one
    for user in old_users:
        db[col_name].delete_one(
            {"uid": user["uid"]}
        )


def aggregateStruct(db: Database, col_name: str):
    '''
    Query to get the variance in all the ages in the dataset
    '''
    pipeline = [
        {"$group": {"_id": None, "stdDev": {"$stdDevPop": "$age"}}},
        {"$project": {"variance": {"$multiply": ["$stdDev", "$stdDev"]}}}
    ]
    list(db[col_name].aggregate(pipeline))


def aggregationStresser(db: Database, col_name: str):
    '''
    Stressful aggregation: group by city & age, compute multiple stats
    '''
    pipeline = [
        {
            "$group": {
                "_id": {
                    "city": "$address.city",
                    "age": "$age"
                },
                "count": {"$sum": 1},
                "avgUid": {"$avg": "$uid"},
                "minUid": {"$min": "$uid"},
                "maxUid": {"$max": "$uid"},
                "stdDevUid": {"$stdDevPop": "$uid"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "city": "$_id.city",
                "age": "$_id.age",
                "count": 1,
                "avgUid": 1,
                "stdDevUid": 1,
                "varianceUid": {"$multiply": ["$stdDevUid", "$stdDevUid"]},
                "rangeUid": {"$subtract": ["$maxUid", "$minUid"]}
            }
        },
        {
            "$sort": {"count": -1}
        }
    ]

    list(db[col_name].aggregate(pipeline))



# -- Unstructured Queries TODO

def insertOneUnstruct(db: Database, col_name: str):
    '''
    Inserts one unstructured document into the collection
    '''
    db[col_name].insert_one(data_pool["unstruct_insert_one"])


def insertManyUnstruct(db: Database, col_name: str):
    '''
    Inserts many unstructured documents into the collection
    '''
    db[col_name].insert_many(data_pool["unstruct_insert_many"])


def readOneUnstruct(db: Database, col_name: str):
    '''
    Reads one unstructured document from the collection
    '''
    total = db[col_name].count_documents({})
    middle_uid = total // 2
    db[col_name].find_one({"uid": middle_uid})


def readManyUnstruct(db: Database, col_name: str):
    '''
    Reads many unstructured documents from the collection
    '''
    db[col_name].find({
        "uid": {"$exists": True},
        "$expr": {"$eq": [{"$mod": ["$uid", 4]}, 0]}
    })


def updateOneUnstruct(db: Database, col_name: str):
    '''
    Updates a middle document's timestamp and likes.
    '''
    total = db[col_name].count_documents({"uid": {"$exists": True}})
    middle_uid = total // 2
    db[col_name].update_one(
        {"uid": middle_uid},
        {"$set": {"timestamp": "01-01-2000 00:00:00", "likes": 25}}
    )


def updateManyUnstruct(db: Database, col_name: str):
    '''
    Marks all users with likes >= 30 as archived = true
    '''
    db[col_name].update_many(
        {"likes": {"$exists": True, "$gte": 30}},
        {"$set": {"archived": True}}
    )


def replaceOneUnstruct(db: Database, col_name: str):
    '''
    Replaces the middle document by uid with a new document.
    '''
    total = db[col_name].count_documents({"uid": {"$exists": True}})
    middle_uid = total // 2
    new_doc = data_pool["unstruct_insert_one"].copy()
    new_doc.pop("_id", None)
    db[col_name].replace_one({"uid": middle_uid}, new_doc)


def insertManyThenDeleteManyUnstruct(db: Database, col_name: str):
    '''
    Complex query: Insert many documents then deletes users with likes > 60
    '''
    db[col_name].insert_many(data_pool["unstruct_insert_many"])
    db[col_name].delete_many({"likes": {"$exists": True, "$gt": 60}})


def insertOneThenUpdateTimestampUnstruct(db: Database, col_name: str):
    '''
    Inserts one document then updates the timestamp.
    '''
    user = data_pool["unstruct_insert_one"]
    db[col_name].insert_one(user)
    new_ts = "01-01-2025 00:00:00"
    db[col_name].update_one(
        {"uid": user.get("uid")},
        {"$set": {"timestamp": new_ts}}
    )


def readThenDeleteManyLikesUnstruct(db: Database, col_name: str):
    '''
    Finds documents with 130+ likes then deletes them one by one.
    '''
    old_users = list(db[col_name].find({"likes": {"$exists": True, "$gt": 130}}, {"uid": 1}))
    for user in old_users:
        db[col_name].delete_one({"uid": user["uid"]})


def aggregateUnstruct(db: Database, col_name: str):
    '''
    Computes the variance of all likes values.
    '''
    pipeline = [
        {"$match": {"likes": {"$type": "number"}}},
        {"$group": {"_id": None, "stdDev": {"$stdDevPop": "$likes"}}},
        {"$project": {"variance": {"$multiply": ["$stdDev", "$stdDev"]}}}
    ]
    list(db[col_name].aggregate(pipeline))

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
        path = f"{db_name}/{filename}"
        col_name = filename.split(".")[0]

        unstructured_functions = [
            insertOneUnstruct, insertManyUnstruct, readOneUnstruct, readManyUnstruct,
            updateOneUnstruct, updateManyUnstruct, replaceOneUnstruct, insertManyThenDeleteManyUnstruct,
            insertOneThenUpdateTimestampUnstruct, readThenDeleteManyLikesUnstruct, aggregateUnstruct
        ]

        structured_functions = [
            insertOneStruct, insertManyStruct, readOneStruct, readManyStruct,
            updateOneStruct, updateManyStruct, replaceOneStruct, insertManyThenDeleteManyStruct,
            insertOneThenUpdateBirthdayStruct, readThenDeleteOldUsersStruct, aggregateStruct
        ]


        print("----------")

        print(f"Opening file {filename}...")
        # load in the data from the file
        with open(path, "r") as f:
            data = json.load(f)

        # loop through functions -> repeatedly run the test -> record metrics

        functions = structured_functions if db_name == "structured" else unstructured_functions

        for fn in functions:
            for i in range(5):
                print(f"Running test {col_name}-{fn.__name__}-iteration:{i}")
                # save the results
                print(f"Saving file {filename}")
                os.makedirs(f"logs/{db_name}/{col_name}/{fn.__name__}", exist_ok=True)
                with open(f"logs/{db_name}/{col_name}/{fn.__name__}/{fn.__name__}_iteration_{i}_{col_name}.json",
                          "w") as f:
                    json.dump(collectMeasure(client[db_name], col_name, data, fn), f, indent=4)


if __name__ == "__main__":
    # generate the data in the data pool
    updateDataPool()

    # connect to mongodb
    client = MongoClient("mongodb://localhost:27017/")

    # run tests for all structured data tests
    run(client, "unstructured")
    run(client, "structured")