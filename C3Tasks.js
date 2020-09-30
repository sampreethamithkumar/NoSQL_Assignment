//1.How many users are there in the database
db.userProfiles.find().count();

//2. How many places are there in the database
db.placeProfiles.find().count();


//7. Display all users who are students and prefer a medium budget restaurant.
db.userProfiles.find({ "otherDemographics.employment": "student", "preferences.budget": "medium" }).pretty();


//8. Display all users who like Bakery cuisines and combine your output with all places
//having Bakery cuisines.
db.userProfiles.aggregate({ $match: { favCuisines: /Bakery/ } }, {
    $lookup: {
        from: "placeProfiles",
        pipeline: [
            { $match: { "cuisines": /Bakery/ } }
        ],
        as: "Combined"
    }
}).pretty();

//9. Display International restaurants that are open on sunday.

//11.Display the average age according to each drinker level
db.userProfiles.aggregate([{ $project: { age: { "$subtract": [{ $toInt: { $year: new Date() } }, { $toInt: { $year: "$personalTraits.birthYear" } }] }, drinkerLevel: "$personality.drinkLevel" } }, { $group: { _id: "$drinkerLevel", averageAge: { $avg: "$age" } } }]).pretty();

//12. For each user whose favourite cuisine is Family, display the place ID, the place rating,
//the food rating and the user’s budget.

//14. list unique cuisines in the database
db.userProfiles.aggregate([{ $project: { cuisines: { $split: ["$favCuisines", ","] } } }, { $unwind: "$cuisines" }, { $project: { cuisines: { $trim: { input: "$cuisines" } } } }, { $group: { _id: "$cuisines" } }, { $sort: { _id: 1 } }]);
db.placeProfiles.aggregate([{ $project: { cuisines: { $split: ["$cuisines", ", "] } } }, { $unwind: "$cuisines" }, { $group: { _id: "$cuisines" } }, { $sort: { _id: 1 } }]);
//11.


//What are the top 3 most popular ambiences (friends/ family/ solitary) for a single when going to a Japanese restaurant?

db.userProfiles.aggregate([
    {
        $match: { "personalTraits.maritalStatus": "single" },
    },
    {
        $group: {
            _id: {
                Ambience: "$preferences.ambience"
            }, totalperson: { $sum: 1 }
        }
    },
    { $sort: { totalPerson: -1 } }, { $limit: 3 }

]);

//When going to Japanese restaurant not done yet 


// 15. Display all of the restaurants and indicate using a separate field/column whether the
// restaurant includes mexican cuisines. For instance, you can display if the restaurant
// serves mexican cuisine then the result should show the restaurant name followed by
// “serves mexican food” in the next field/column, or if the restaurant does not serves
// mexican cuisine then the result should show the restaurant name followed by “doesn’t
// serves mexican food” in the next field/column


db.placeProfiles.aggregate([

    {
        $addFields: {
            "conditioncheck": {
                "$cond": {
                    if: {
                        $regexFind: { input: "$cuisines", regex: /Mexican/ }
                    },
                    then: { $concat: ["$placeName", "  serves mexican food"] },
                    else: "doesn’t serves mexican food"
                }
            }
        }

    }]).pretty();

    //Display all high budget restaurants that serve alcohol and smoking area 