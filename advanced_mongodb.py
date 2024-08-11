"""
All the questions are written in #
"""
import pymongo
import re

url = "mongodb://localhost:27017/"

client = pymongo.MongoClient(url)

db = client["Adv_mdb"]

users = db.get_collection("users")
authors = db.get_collection("authors")
books = db.get_collection("books")


# to find active users in data
data = [{"$match": {"isActive": True}}]  # matches the value in the data
result = list(users.aggregate(data))
print(result)


# To count that data
data = [{"$match": {"isActive": True}}, {"$count": "str"}]     # count = counts the values occuring and store it in a variable "str"
result = list(users.aggregate(data))
print(result)


# find avg of age based on gender
data = [{"$group": {"_id": "$gender", "averageAge": {"$avg": "$age"}}}]   # Group = Merges all the data according to the required format (here by gender)
result = list(users.aggregate(data))
print(result)


# find avg of age of all
data = [{"$group": {"_id": "null", "averageAge": {"$avg": "$age"}}}]      # Here grouped by all(Merged data of all users in 1 document)
result = list(users.aggregate(data))
print(result)


# count and sort the data and find top n using limit
data = [
    {
        "$group": {"_id": "$favoriteFruit", "count": {"$sum": 1}},
    },
    {"$sort": {"count": -1}},  # for ascending sort use 1 while for descending sort use -1
    {"$limit": <any number>}   # put number = top <number> u want to find
]
result = list(users.aggregate(data))
print(result)


# count male and females in data
data = [{"$group": {"_id": "$gender", "Gendercount": {"$sum": 1}}}]
result = list(users.aggregate(data))
print(result)


# top country with registers users
data = [
    {
        "$group": {"_id": "$company.location.country", "no_of_users": {"$sum": 1}},
    },
    {"$sort": {"no_of_users": -1}},
    {"$limit": 1},
]
result = list(users.aggregate(data))
print(result)


# avg no of tags per user (method-1)
data = [
    {
        "$unwind": {             # unwind extracts all data as separate documents
            "path": "$tags",
        }
    },
    {"$group": {"_id": "$_id", "count_tags": {"$sum": 1}}},
    {"$group": {"_id": "null", "avg_tags": {"$avg": "$count_tags"}}},
]
result = list(users.aggregate(data))
print(result)


# avg no of tags per user (method-2)
data = [
    {"$addFields": {"num_tags": {"$size": {"$ifNull": ["$tags", []]}}}},   # added field called num_tags with calculating size of tags. If tags not present will be [].
    {"$group": {"_id": "null", "no_of_tags": {"$avg": "$num_tags"}}},
]
result = list(users.aggregate(data))
print(result)


# how many usery has enum tags
data = [{"$match": {"tags": "enim"}}, {"$count": "Count_enim"}]
result = list(users.aggregate(data))
print(result)


# find names and age of users that are inactive and have velit tags(method-1)(select name and age in printing)
data = [
    {"$match": {"isActive": False, "tags": "velit"}},
]
result = list(users.aggregate(data))
print(result)


# find names and age of users that are inactive and have velit tags(method-1)(use project to do that)
data = [
    {"$match": {"isActive": False, "tags": "velit"}},
    {"$project": {"name": 1, "age": 1, "_id": 0}},   # project = Passes along the documents with the requested fields to the next stage in the pipeline.
]
result = list(users.aggregate(data))
print(result)


# how many users have phone starting with +1 940
data = [
    {"$match": {"company.phone": re.compile(r"^\+1 940")}},
    {"$count": "start_phone(+1 940)"},
]
result = list(users.aggregate(data))
print(result)


# find which user have registered last(most recently)
data = [
    {"$sort": {"registered": -1}},
    {"$limit": 5},
    {"$project": {"_id": 0, "name": 1, "registered": 1, "company": 1, "age": 1}},
]
result = list(users.aggregate(data))
print(result)


# categorize users by their favourite fruit
data = [{"$group": {"_id": "$favoriteFruit", "users": {"$push": "$name"}}}]    # push = appends a specified value to an array
result = list(users.aggregate(data))
print(result)


# how many users have "ad" as the 2nd tag
data = [{"$match": {"tags.1": "ad"}}, {"$count": "user_2nd_tag_ad"}]
result = list(users.aggregate(data))
print(result)


# find users who have both 'enim' and 'id' as tags
data = [{"$match": {"tags": {"$all": ["enim", "id"]}}}]
result = list(users.aggregate(data))
print(result)


# list all companies located in usa with their corresponding user count
data = [
    {"$match": {"company.location.country": "USA"}},
    {"$group": {"_id": "$company.title", "users_count": {"$sum": 1}}},
]
result = list(users.aggregate(data))
print(result)


# lookup with first and arrayelement at
data = [
    {
        "$lookup": {
            "from": "authors",
            "localField": "author_id",
            "foreignField": "_id",
            "as": "author_data",
        }
    },
    {
        "$addFields": {"author_data": {"$first": "$author_data"}}
    },  # first returns the first value in the array
    {
        "$addFields": {"author_data": {"$arrayElemAt": ["$author_data", 0]}}
    },  # in an array from which position
]
result = list(books.aggregate(data))
print(result)
