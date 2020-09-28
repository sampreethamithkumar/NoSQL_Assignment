//Creating database
use FIT5137A1MRDB;

//Creating collection
db.createCollection.placeProfiles;
db.createCollections.userProfiles;

// Embedding openinghours in placesProfiles
db.placeProfiles.aggregate([ { $lookup: { from:'openingHours', localField:'_id', foreignField: 'placeID', as:'openingHours'} },{$merge:{into:'placeProfiles'}} ]);

//Insert value Task C2
db.placeProfiles.insertOne(
    {
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
                "_id":ObjectId(),
                "hours": "09:00-20:00",
                "days":"Mon;Tue;Wed;Thu;Fri;"
            },
            {
                "_id":ObjectId(),
                "hours":"12:00-18:00",
                "days":"Sat;"
            },
            {
                "_id":ObjectId(),
                "hours":"12:00-18:00",
                "days":"Sun;"
            }
        ]
      }
)

// C2 Task 2 - Updating data
//souce
// https://stackoverflow.com/questions/12589792/how-to-replace-substring-in-mongodb-document
db.userProfiles.deleteOne({"_id":1108});
db.userProfiles.insertOne({
  "_id": 1108,
  "favCuisines": "Cafe-Coffee_Shop, Sushi, Latin_American, Deli-Sandwiches, Mexican, Hot_Dogs, American, Fast_Food, Burgers, Asian, Pizzeria, Chinese, Dessert-Ice_Cream, Cafeteria, Japanese, Game, Family, Seafood",
  "favPaymentMethod": "VISA, cash, MasterCard-Eurocard",
  "location": {
    "latitude": "22.143524",
    "longitude": "-100.98756"
  },
  "otherDemographics": {
    "employment": "student",
    "religion": "Catholic"
  },
  "personalTraits": {
    "birthYear": "1983",
    "height": "1.81",
    "maritalStatus": "single",
    "weight": "76"
  },
  "personality": {
    "drinkLevel": "abstemious",
    "favColor": "blue",
    "interest": "technology",
    "typeOfWorker": "thrifty-protector"
  },
  "preferences": {
    "ambience": "solitary",
    "budget": "medium",
    "dressPreference": "informal",
    "smoker": "FALSE",
    "transport": "public"
  }
});

db.userProfiles.updateMany(
  {"_id":1108 },
  [{
    $set: {"favCuisines" : {
      $replaceOne: { input: "$favCuisines", find:"Fast_Food,",replacement:""}
    }}
  },{
    $set : {"favPaymentMethod" : {
      $replaceOne: {input:"$favPaymentMethod",find:"cash",replacement: "debit_cards"}
    }}
  }]
);

// C2 Task 3 - Delete data
db.userProfiles.deleteOne({"_id":1063});



// C3  Task