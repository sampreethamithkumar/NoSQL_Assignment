//1.How many users are there in the database
db.userProfiles.find().count();

//2. How many places are there in the database
db.placeProfiles.find().count();


//3. How many reviews were made in the database?


//7. Display all users who are students and prefer a medium budget restaurant.
db.userProfiles.find({"otherDemographics.employment":"student","preferences.budget":"medium"}).pretty();


//8. Display all users who like Bakery cuisines and combine your output with all places
//having Bakery cuisines.
db.userProfiles.find(favCuisines: /Bakery/).count();
db.userProfiles.aggregate({$match:{favCuisines: /Bakery/}},{$count:"Number of users"});
db.placeProfiles.aggregate({$match:{cuisines: /Bakery/}},{$count:"Number of places"});

//Question 8 code
db.userProfiles.aggregate({$match:{favCuisines: /Bakery/}},{$lookup:{
    from: "placeProfiles",
    pipeline:[
        {$match:{"cuisines":/Bakery/}}
    ],
    as :"Combined"
}}).pretty();

//9. Display International restaurants that are open on sunday.

//11.
db.userProfiles_1.aggregate({$project:{Year:{$year:"$personalTraits.birthYear"}}}).pretty()
db.userProfiles_1.aggregate({$project:{currentDate:new Date()}},{$project:{Year:{$year:"$currentDate"}}}).pretty()
db.userProfiles_1.aggregate({$project:{currentDate:new Date(),birthYear:"$personalTraits.birthYear"}},{$project:{currentYear:{$year:"$currentDate"},bornYear:{$year:"$birthYear"}}},{$project:{Age:{"$subtract":["$currentYear","$bornYear"]}}}).pretty()
db.userProfiles_1.aggregate({$project:{currentDate:{$year:new Date()},birthYear:{$year:"$personalTraits.birthYear"}}},{$project:{Age:{"$subtract":["$currentDate","$birthYear"]}}}).pretty()
db.userProfiles_1.aggregate({$project:{Age:{"$subtract":[{$toInt:{$year:new Date()}},{$toInt:{$year:"$personalTraits.birthYear"}}]}}}).pretty();