//1.How many users are there in the database
db.userProfiles.find().count();

//2. How many places are there in the database
db.placeProfiles.find().count();


//3. How many reviews were made in the database?


//7. Display all users who are students and prefer a medium budget restaurant.
db.userProfiles.find({"otherDemographics.employment":"student","preferences.budget":"medium"}).pretty();


//8. Display all users who like Bakery cuisines and combine your output with all places
//having Bakery cuisines.


