from pymongo import MongoClient

def create():
    # creation time of database after loading in data from i/o
    pass

def insertOne(structured: bool):
    # insert_one
    pass

def insertMany(size:int, structured: bool):
    # insert_many
    pass

def readOne():
    # find_one: use uid and index it to find something in middle
    pass

def readMany():
    # find all: use uid get all even
    pass

def updateOne():
    # update_one middle uid
    pass

def updateMany():
    # update_many middle age
    pass

def replaceOne():
    # replace_one
    pass

def deleteOne():
    pass

def deleteMany():
    pass

def notIn():
    pass

def elementExist():
    pass

def regex():
    pass

def aggregate():
    pass

def textSearch():
    pass

def main():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["test_db"]
    collection = db["test_collection"]

if __name__ == "__main__":
    main()