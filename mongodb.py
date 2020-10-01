import pymongo
# from pprint import pprint
import pprint
import json
import csv


# Task C1
# Connection to Mongodb database
client = pymongo.MongoClient('localhost',27017)

#Connecting to FIT5137A1MRDB database
# Question 1
db = client['FIT5137A1MRDB_python']

# Storing collection
# Question 2
placeProfiles = db['placeProfiles']
userProfiles = db['userProfiles']
openingHours= db['openingHours']

# Question 3
with open('C:\\Users\\sampr\\Downloads\\mongo_dataset\\placeProfiles.json') as f:
    file_data = json.load(f)

placeProfiles.insert_many(file_data)


with open('C:\\Users\\sampr\\Downloads\\mongo_dataset\\userProfile.json') as f:
    file_data = json.load(f)

userProfiles.insert_many(file_data)

csv_file = open('C:\\Users\\sampr\\Downloads\\mongo_dataset\\openingHours.csv','r')
reader = csv.DictReader(csv_file)
header = ["placeID","hours","days"]
for each in reader:
    row={}
    for field in header:
        row[field]=each[field]
    openingHours.insert_one(row)


# Question 4
pipline = [ { "$lookup": { "from":'openingHours', "localField":'_id', "foreignField": 'placeID', "as":'openingHours'}},{"$merge":{"into":'placeProfiles'}}]


# Task C2

# Question 1
data = {
        "_id": "70000",
        "acceptedPaymentModes": "any",
        "address": {
          "city": "San Luis Potosi",
          "country": "Mexico",
          "state": "SLP",
          "street": "Carretera Central Sn"
        },
        "cuisines": "Mexican,Burgers",
        "location": {
          "latitude": "23.7523041",
          "longitude": "-99.166913"
        },
        "parkingArragements": "none",
        "placeFeatures": {
          "accessibility": "completely",
          "alcohol": "No_Alcohol_Served",
          "area": "open",
          "dressCode": "informal",
          "franchise": "t",
          "otherServices": "Internet",
          "price": "medium",
          "smokingArea": "not permitted"
        },
        "placeName": "Taco Jacks",
        "openingHours" : [
            {
                "hours": "09:00-20:00",
                "days":"Mon;Tue;Wed;Thu;Fri;"
            },
            {
                "hours":"12:00-18:00",
                "days":"Sat;"
            },
            {
                "hours":"12:00-18:00",
                "days":"Sun;"
            }
        ]
      }

insert_data = placeProfiles.insert_one(data)

# Question 2
findQuery = {"_id":"1108"}
setQuery = [{
    "$set": {"favCuisines" : {
      "$replaceOne": { "input": "$favCuisines", "find":"Fast_Food,","replacement":""}
    }}
  },{
    "$set" : {"favPaymentMethod" : {
      "$replaceOne": {"input":"$favPaymentMethod","find":"cash","replacement": "debit_cards"}
    }}
  }]

userProfiles.update_one(findQuery,setQuery)

# Question 3
myquery = {"_id":"1063"}
userProfiles.delete_one(myquery)


# Task 3
# 1.How many users are there in the database
documents = userProfiles.find()
documents_count = document.count()
print(documents_count)

# 2. How many places are there in the database
documents = placeProfiles.find()
documents_count = documents.count()
print(documents_count)


# 7. Display all users who are students and prefer a medium budget restaurant.
for documents in userProfiles.find({ "otherDemographics.employment": "student", "preferences.budget": "medium" }):
    pprint(documents)
# 8. Display all users who like Bakery cuisines and combine your output with all places
# having Bakery cuisines.
pipline = [{ "$match": { "favCuisines": "/Bakery/" } }, {"$lookup": {"from": "placeProfiles","pipeline": [{ "$match": { "cuisines": "/Bakery/" } }],"as": "Combined"}}]
pprint.pprint(userProfiles.aggregate(pipline))

#9 Display International restaurants that are open on sunday.
pipline = [{"$match":{"cuisines":"International","openingHours.days":"Sun;"}}]
pprint.pprint(placeProfiles.aggregate(pipline))

#13. What are the top 3 most popular ambiences (friends/ family/ solitary) for a single when going to a Japanese restaurant?
pipeline = [
    {
        "$match":{"personalTraits.maritalStatus":"single","favCuisines":"/Japanese/"},
    },
    {
        "$group":{"_id":{
            "Ambience":"$preferences.ambience"
        }, "totalperson":{"$sum":1}}
    },
    {"$sort":{"totalperson":-1}},{"$limit":3}

]
pprint.pprint(list(userProfiles.aggregate(pipeline)))

#14.  list unique cuisines in the database
pipeline = [{"$project":{"cuisines":{"$split":["$favCuisines",","]}}},{"$unwind": "$cuisines"},{"$project":{"cuisines":{"$trim":{"input":"$cuisines"}}}},{"$group":{"_id":"$cuisines"}},{"$sort":{"_id":1}}]
pprint.pprint(list(userProfiles.aggregate(pipeline)))

# 15.Display all of the restaurants and indicate using a separate field/column whether the
# restaurant includes mexican cuisines. For instance, you can display if the restaurant
# serves mexican cuisine then the result should show the restaurant name followed by
# “serves mexican food” in the next field/column, or if the restaurant does not serves
# mexican cuisine then the result should show the restaurant name followed by “doesn’t
# serves mexican food” in the next field/column
pipline = [

        {
            "$addFields":{
                "conditioncheck":{
                    "$cond":{
                        "if":{
                            "$regexFind":{"input":"$cuisines","regex":"/Mexican/"}},
                            "then": {"$concat":'["$placeName","  serves mexican food"]'},
                            "else": "doesn’t serves mexican food"
                    }
                }
            }

    }]
pprint.pprint(list(placeProfiles.aggregate(pipline)))

# Additional Question
#1 What are the ambience, dresscode and budget that professional prefer?
pipeline = [{"$match" :{"otherDemographics.employment":"professional"}},{"$project": {"Ambience":"$preferences.ambience","Budget":"$preferences.budget","DressPreference":"$preferences.dressPreference"}}]
pprint.pprint(list(userProfiles.aggregate(pipeline)))

#2. Display the restaurants that provide type of accessablilty support and what cusines do they serve to their customer?
pipeline = [{"$project": {"_id":"0","PlaceName":"$placeName","AccessabliltySupport": "$placeFeatures.accessibility","cusines":"$cuisines"}}]
pprint.pprint(list(placeProfiles.aggregate(pipline)))

#3.
