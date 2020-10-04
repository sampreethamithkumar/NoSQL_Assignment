from cassandra.cluster import Cluster
from decimal import *
import csv
import json
import ast

cluster = Cluster(['localhost'])

session = cluster.connect()

# Creating a KeySpace
session.execute("CREATE KEYSPACE fit5137a1_mrdb_python WITH replication = {'class':'SimpleStrategy','replication_factor':1}")

session.set_keyspace('fit5137a1_mrdb_python')

class User_personal_traits(object):

    def __init__(self,birth_year,weight,height,martial_status):
        self.birth_year = birth_year
        self.weight = weight
        self.height = height
        self.martial_status = martial_status



cluster.register_user_type('fit5137a1_mrdb_python','user_personal_traits_udt',User_personal_traits)

session.execute("""
    CREATE TYPE user_personal_traits_udt (
    birth_year int,
    weight int,
    height decimal,
    marital_status text
)
""")

class User_peronality_udt(object):
    def __init__(self,interest,type_of_worker,fav_color,drink_level):
        self.interest = interest
        self.type_of_worker = type_of_worker
        self.fav_color = fav_color
        self.drink_level = drink_level

cluster.register_user_type('fit5137a1_mrdb_python','user_personality_udt',User_peronality_udt)

session.execute("""
CREATE TYPE user_personality_udt(
interest text,
type_of_worker text,
fav_color text,
drink_level text,
)
""")

class User_other_demographics_udt(object):
    def __init__(self,religion,employment):
        self.religion = religion
        self.employment = employment

cluster.register_user_type('fit5137a1_mrdb_python','user_other_demographics_udt',User_other_demographics_udt)

session.execute("""
    CREATE TYPE user_other_demographics_udt (
    religion text,
    employment text
)
""")

class User_preferences_udt(object):
    def __init__(self,budget,smoker,dress_preference,ambience,transport):
        self.budget = budget
        self.smoker = smoker
        self.dress_preference = dress_preference
        self.ambience = ambience
        self.transport = transport

cluster.register_user_type('fit5137a1_mrdb_python','user_preferences_udt',User_preferences_udt)

session.execute("""
    CREATE TYPE user_preferences_udt (
    budget text,
    smoker boolean,
    dress_preference text,
    ambience text,
    transport text
)
""")

session.execute("""
CREATE TABLE user_ratings (rating_id int, user_id int, place_id int, rating_place decimal, rating_food decimal, rating_service decimal, user_personal_traits frozen<user_personal_traits_udt>, user_personality frozen<user_personality_udt>, user_preferences frozen<user_preferences_udt>, user_other_demographics frozen<user_other_demographics_udt>, user_fav_cuisines set<text>, user_fav_payment_method set<text>, PRIMARY KEY((rating_id,user_id)));
""")

prepared = session.prepare("""
    INSERT INTO user_ratings(rating_id,user_id,place_id,rating_place,rating_food,rating_service,user_personal_traits, user_personality, user_preferences,user_other_demographics,user_fav_cuisines,user_fav_payment_method)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
""")


#importing csv file.
# https://stackoverflow.com/questions/59366216/import-csv-file-in-cassandra-using-python-script
with open("C:\\Users\\sampr\\Downloads\\cassandra dataset\\user_ratings.csv",'r') as fares:
    for fare in fares:
        columns = fare.split(",")
        rating_id = columns[0]
        user_id = columns[1]
        place_id = columns[2]
        rating_place = columns[3]
        rating_food = columns[4]
        rating_service = columns[5]
        user_personal_traits = json.loads(columns[6])
        user_personality = json.loads(columns[7])
        user_preferences = json.loads(columns[8])
        user_other_demographics = json.loads(columns[9])
        user_fav_cuisines = columns[10]
        user_fav_payment_method = columns[11]
        
session.execute(prepared,[int(rating_id),int(user_id),int(place_id),Decimal(rating_place),Decimal(rating_food),Decimal(rating_service),User_personal_traits(user_personal_traits.birth_year,user_personal_traits.weight,user_personal_traits.height,user_personal_traits.marital_status),User_peronality_udt(user_personality.interest,user_personality.type_of_worker,user_personality.fav_color,user_personality.drink_level),User_preferences_udt(user_preferences.budget,user_preferences.smoker,user_preferences.dress_preference,user_preferences.ambience,user_preferences.transport),User_other_demographics_udt(user_other_demographics.religion,user_other_demographics.employment),user_fav_cuisines,user_fav_payment_method])

# place ratings user defined types
session.execute("""
    CREATE TYPE place_address_utd (
    street text,
    city text,
    state text,
    country text
)
""")

class Place_address(object):
    def __init__(self,street,city,state,country):
        self.street = street
        self.city = city
        self.state = state
        self.country = country

cluster.register_user_type('fit5137a1_mrdb_python','place_address_udt',Place_address)


session.execute("""
CREATE TYPE place_features_utd (
    alcohol text,
    smoking_area text,
    dress_code text,
    accessibility text,
    price text,
    franchise boolean,
    area text,
    other_services text
)
""")

class Place_feature(object):
    def __init__(self,alcohol,smoking_area,dress_code,accessibility,price,franchise,area,other_services):
        self.alcohol = alcohol
        self.smoking_area = smoking_area
        self.dress_code = dress_code
        self.accessibility = accessibility
        self.price = price
        self.franchise = franchise,
        self.area = area,
        self.other_services = other_services

cluster.register_user_type('fit5137a1_mrdb_python','place_features_utd',Place_feature)


session.execute("""
CREATE TABLE place_ratings (rating_id int, user_id int, place_id int, rating_place decimal, rating_food decimal, rating_service decimal, place_name text, place_address frozen<place_address_utd>, place_features frozen<place_features_utd>, parking_arrangements text, accepted_payment_modes set<text>, cuisines set<text>,PRIMARY KEY((rating_id,place_id)));
""")

#importing csv file.
# https://stackoverflow.com/questions/59366216/import-csv-file-in-cassandra-using-python-script
with open("C:\\Users\\sampr\\Downloads\\cassandra dataset\\place_ratings.csv",'r') as fares:
    for fare in fares:
        columns = fare.split(",")
        rating_service = columns[0]
        user_id = columns[1]
        place_id = columns[2]
        rating_place = columns[3]
        rating_food = columns[4]
        rating_service = columns[5]
        place_name = columns[6]
        place_address = json.loads(columns[7])
        place_features = json.loads(columns[8])
        parking_arrangements = columns[9]
        accepted_payment_modes = columns[10]
        cusinies = columns[11]

prepared = session.prepare("""
    INSERT INTO user_ratings(rating_id,user_id,place_id,rating_place,rating_food,rating_service,place_name,place_address,place_features,parking_arrangements,accepted_payment_modes,cusinies)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
""")

session.execute(prepared,[int(rating_id),int(user_id),int(place_id),Decimal(rating_place),Decimal(rating_food),Decimal(rating_service),place_name,Place_address(place_address.street,place_address.city,place_address.state,place_address.country),Place_feature(place_features.alcohol,place_features.smoking_area,place_features.dress_code,place_features.accessibility,place_features.price,place_features.franchise,place_features.area,place_features.other_services),parking_arrangements,accepted_payment_modes,cusinies])


session.execute("""
    UPDATE user_ratings
SET user_fav_cuisines = user_fav_cuisines - {'Fast_Food'}
WHERE user_id=1108 and rating_id IN (65,66,67,68,69,70,71,72,73,74)
""")

session.execute("""
    UPDATE user_ratings
SET user_fav_cuisines = user_fav_cuisines - {'Fast_Food'}
WHERE user_id=1108 and rating_id IN (65,66,67,68,69,70,71,72,73,74)
""")

session.execute("""
DELETE FROM user_ratings
WHERE user_id=1063 and rating_id IN (137,138,139,140,141)
""")

session.execute("""
INSERT INTO place_ratings (rating_id,user_id,place_id,rating_place,rating_food,rating_service,place_name,place_address,place_features,parking_arrangements,accepted_payment_modes,cuisines)
VALUES(7777,1060,70000,2,1,2,'Taco Jacks',{street:'Carretera Central Sn',city:'San Luis Potosi',state:'SLP',country:'Mexico'},{alcohol:'No_Alcohol_Served',smoking_area:'not permitted',dress_code:'informal',accessibility:'completely',price:'medium',franchise:TRUE,area:'open',other_services:'Internet'}, 'none',{'any'},{'Mexican','Burgers'} );
""")

session.execute("""
INSERT INTO user_ratings(rating_id,user_id,place_id,rating_place,rating_food,rating_service,user_personal_traits, user_personality, user_preferences,user_other_demographics,user_fav_cuisines,user_fav_payment_method)
VALUES(7777,1060,70000,2,1,2,{birth_year: 1991, weight: 82, height: 1.84, marital_status: 'single'},{interest: 'technology', type_of_worker: 'thrifty-protector', fav_color: 'blue', drink_level: 'casual drinker'},{budget: 'medium', smoker: FALSE, dress_preference: 'formal', ambience: 'family', transport: 'public'}, {religion: 'Catholic', employment: 'student'},{'Burgers', 'Cafeteria', 'Pizzeria', 'Juice', 'American', 'Tex-Mex', 'Spanish', 'Mexican', 'Fast_Food', 'Cafe-Coffee_Shop', 'Soup', 'Hot_Dogs', 'Italian'},{'cash'})
""")



# Task 3

# Question 3
result = session.execute("""
select count(*) as Total_user_ratings from user_ratings
""")

row_list = list(result)


#Question 4
session.execute("""
    CREATE INDEX ON place_ratings (parking_arrangements)
""")
result = session.execute("""
select count(*) as Total_user_ratings from place_ratings where parking_arrangements='public'
""")


# Question 5
session.execute("""
create index on user_ratings(user_personality)
""")
result = session.execute("""
select * from user_ratings where user_personality={interest:'technology',type_of_work:'thrifty-protector',fav_color:'blue',drink_level:'casual drinker'}
""")

# Question 6
session.execute("""
create index on place_ratings(cuisines)
""")
result = session.execute("""
    select place_id, rating_food,cuisines from place_ratings where cuisines CONTAINS 'Pizzeria'
""")

# Question 10
session.execute("""
    create index on user_ratings(place_name)
""")
session.execute("""
    select avg(rating_place),avg(rating_food),avg(rating_service) from place_ratings where place_name='puesto de tacos'
""")