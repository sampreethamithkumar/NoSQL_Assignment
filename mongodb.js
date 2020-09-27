// Embedding openinghours in placesProfiles
db.placesProfiles.aggregate([ { $lookup: { from:'openingHours', localField:'_id', foreignField: 'placeID', as:'openingHours'} },{$merge:{into:'placesProfiles'}} ]);

//Insert value Task C2
db.placesProfiles.insertOne(
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
