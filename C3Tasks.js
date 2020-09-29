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


//What are the top 3 most popular ambiences (friends/ family/ solitary) for a single when going to a Japanese restaurant?

 db.userProfiles.aggregate([
     {
         $match:{"personalTraits.maritalStatus":"single"},     
     },
     {
        $group:{_id:{
            Ambience:"$preferences.ambience"
        }, totalperson:{$sum:1}}
     },
     {$sort:{totalPerson:-1}},{$limit:3}
    
 ]);

//When going to Japanese restaurant not done yet 