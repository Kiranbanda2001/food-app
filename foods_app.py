# # Local Food Wastage Management System Project

# # Step 1: Load data from CSV files

import pandas as pd
import mysql.connector

providers = pd.read_csv("providers_data.csv")
receivers = pd.read_csv("receivers_data.csv")
food_listings = pd.read_csv("food_listings_data.csv")
claims = pd.read_csv("claims_data.csv")

providers
receivers
food_listings
claims


# # Step 2: Check null values

# Check Null Values for Providers Dataset.
providers.isnull().sum()

# Check Null Values for Receivers Dataset.
receivers.isnull().sum()

# Check Null Values for Food Listings Dataset.
food_listings.isnull().sum()

# Check Null values for Claims Dataset.
claims.isnull().sum()


# # Step 3: SQL Connection

# Creating SQL Connection Using Different Methods

# # 2. Using MySQL Connector

import mysql.connector
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="12345678",

)
cursor = conn.cursor()
print("MySQL connection established!")

cursor.execute("CREATE DATABASE IF NOT EXISTS food_data")
print("MySQL database 'food_data' created successfully!")

cursor.execute("use food_data")


# # 3. Using PyMySQL 

conn_pymysql = pymysql.connect(
    host="localhost",
    user="root",
    password="12345678",
    database="food_data"
)
cursor_pymysql = conn_pymysql.cursor()
print("PyMySQL connection established!")

# # 4. Using SQLAlchemy 

engine = create_engine("mysql+mysqlconnector://root:12345678@localhost/food_data")
print("sqlalchemy connection established!")


# # Step 4: Creating Tables

# # 1. Create a Table in SQLite

# Creating tables for Providers Dataset
cursor.execute('''
    CREATE TABLE IF NOT EXISTS providers (
        Provider_ID INTEGER PRIMARY KEY,
        Name TEXT,
        Type TEXT,
        Address TEXT,
        City TEXT,
        Contact TEXT
    )
''')

# Creating tables Receivers Dataset
cursor.execute('''
    CREATE TABLE IF NOT EXISTS receivers (
        Receiver_ID INTEGER PRIMARY KEY,
        Name TEXT,
        Type TEXT,
        City TEXT,
        Contact TEXT
    )
''')

# Creating tables Food Listings Dataset
cursor.execute('''
    CREATE TABLE IF NOT EXISTS food_listings(
        Food_ID INTEGER PRIMARY KEY,
        Food_Name TEXT,
        Quantity INTEGER,
        Expiry_Date TEXT,
        Provider_ID INTEGER,
        Provider_Type TEXT,
        Location TEXT,
        Food_Type TEXT,
        Meal_Type TEXT
    )
''')

# Creating tables Claims Dataset
cursor.execute('''
    CREATE TABLE IF NOT EXISTS claims (
        Claim_ID INTEGER PRIMARY KEY,
        Food_ID INTEGER,
        Receiver_ID INTEGER,
        Status TEXT,
        Timestamp TEXT
    )
''')

# # 2. Create a Table in MySQL

# Creating tables for Providers Dataset
cursor.execute("""
    CREATE TABLE IF NOT EXISTS providers (
        Provider_ID INT PRIMARY KEY,
        Name VARCHAR(255),
        Type VARCHAR(100),
        Address TEXT,
        City VARCHAR(100),
        Contact VARCHAR(50)
    )
""")
conn.commit()

# Creating tables Receivers Dataset
cursor.execute("""
    CREATE TABLE IF NOT EXISTS receivers (
        Receiver_ID	INT PRIMARY KEY,
        Name VARCHAR(255),
        Type VARCHAR(100),
        City VARCHAR(100),
        Contact VARCHAR(50)
    )
""")
conn.commit()

# Creating tables Food Listings Dataset
cursor.execute("""
    CREATE TABLE IF NOT EXISTS food_listings (
        Food_ID INT PRIMARY KEY,
        Food_Name VARCHAR(255),
        Quantity INT,
        Expiry_Date DATETIME,
        Provider_ID INT,
        Provider_Type VARCHAR(100),
        Location VARCHAR(255),
        Food_Type VARCHAR(100),
        Meal_Type VARCHAR(100)
    )
""")
conn.commit()

# Creating tables Claims Dataset
cursor.execute("""
    CREATE TABLE IF NOT EXISTS claims (
        Claim_ID INT PRIMARY KEY,
        Food_ID INT,
        Receiver_ID	INT,
        Status VARCHAR(50),
        Timestamp DATETIME        
        )
""")
conn.commit()


# # Step 5: Inserting Data Using iterrows()

# For Providers Dataset

for index, row in providers.iterrows():
    cursor.execute("""
        INSERT INTO providers (Provider_ID, Name, Type, Address, City, Contact)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            Name = VALUES(Name),
            Type = VALUES(Type),
            Address = VALUES(Address),
            City = VALUES(City),
            Contact = VALUES(Contact)
    """, tuple(row))

# For Receivers Dataset

for index, row in receivers.iterrows():
    cursor.execute("""
        INSERT INTO receivers (Receiver_ID, Name, Type, City, Contact)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            Name = VALUES(Name),
            Type = VALUES(Type),
            City = VALUES(City),
            Contact = VALUES(Contact)
    """, tuple(row))

# For Food Listings Dataset

food_listings['Expiry_Date'] = pd.to_datetime(food_listings['Expiry_Date']).dt.strftime('%Y-%m-%d')

for index, row in food_listings.iterrows():
    cursor.execute("""
        INSERT INTO food_listings (
            Food_ID, Food_Name, Quantity, Expiry_Date,
            Provider_ID, Provider_Type, Location, Food_Type, Meal_Type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            Food_Name = VALUES(Food_Name),
            Quantity = VALUES(Quantity),
            Expiry_Date = VALUES(Expiry_Date),
            Provider_ID = VALUES(Provider_ID),
            Provider_Type = VALUES(Provider_Type),
            Location = VALUES(Location),
            Food_Type = VALUES(Food_Type),
            Meal_Type = VALUES(Meal_Type)
    """, tuple(row))

# For Claims Dataset

claims['Timestamp'] = pd.to_datetime(claims['Timestamp'])

for index, row in claims.iterrows():
    cursor.execute("""
        INSERT INTO claims (Claim_ID, Food_ID, Receiver_ID, Status, Timestamp)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            Food_ID = VALUES(Food_ID),
            Receiver_ID = VALUES(Receiver_ID),
            Status = VALUES(Status),
            Timestamp = VALUES(Timestamp)
    """, tuple(row))
    

# # Step 6: Running SQL Queries
# Now, we will execute 13 SQL queries to analyze the data.


# # 1) How many food providers and receivers are there in each city?

query1 = """
SELECT p.City,
       COUNT(DISTINCT p.Provider_ID) AS provider_count,
       COUNT(DISTINCT r.Receiver_ID) AS receiver_count
FROM providers p
LEFT JOIN receivers r ON p.City = r.City
GROUP BY p.City;
"""

df1 = pd.read_sql(query1, engine)  
print(df1)


# # 2) Which type of food provider (restaurant, grocery store, etc.) contributes the most food?

query2 = """
SELECT Provider_Type, SUM(Quantity) AS total_quantity
FROM food_listings
GROUP BY Provider_Type
ORDER BY total_quantity DESC
LIMIT 1;
"""

df2 = pd.read_sql(query2, engine) 
print(df2)


# # 3) What is the contact information of food providers in a specific city?

city_name = "Adamsville   ".strip()  # Trim trailing spaces

query3 = """
SELECT Name, Type, Contact
FROM providers
WHERE City = %s;
"""

df3 = pd.read_sql(query3, engine, params=(city_name,))
print(df3)


# # 4) Which receivers have claimed the most food?

query4 = """
SELECT r.Receiver_ID, r.Name, COUNT(c.Food_ID) AS total_claims
FROM claims c
JOIN receivers r ON c.Receiver_ID = r.Receiver_ID
GROUP BY r.Receiver_ID, r.Name
ORDER BY total_claims DESC;
"""

df4 = pd.read_sql(query4, engine)
print(df4)


# # 5) What is the total quantity of food available from all providers?

query_total_quantity = """
SELECT SUM(Quantity) AS total_food_quantity
FROM food_listings;
"""

df_total_quantity = pd.read_sql(query_total_quantity, engine)
print("Total Quantity of Food Available:")
print(df_total_quantity, "\n")


# # 6) Which city has the highest number of food listings?

query_top_city = """
SELECT p.City, COUNT(*) AS listing_count
FROM food_listings f
JOIN providers p ON f.Provider_ID = p.Provider_ID
GROUP BY p.City
ORDER BY listing_count DESC
LIMIT 1;
"""

df_top_city = pd.read_sql(query_top_city, engine)
print("City with Highest Number of Food Listings:")
print(df_top_city, "\n")


# # 7) What are the most commonly available food types?

query_common_food_types = """
SELECT Food_Type, COUNT(*) AS count
FROM food_listings
GROUP BY Food_Type
ORDER BY count DESC;
"""

df_common_food_types = pd.read_sql(query_common_food_types, engine)
print("Most Commonly Available Food Types:")
print(df_common_food_types)


# # 8) How many food claims have been made for each food item?

query_claims_per_food = """
SELECT f.Food_Name, COUNT(c.Claim_ID) AS claim_count
FROM claims c
JOIN food_listings f ON c.Food_ID = f.Food_ID
GROUP BY f.Food_Name
ORDER BY claim_count DESC;
"""

df_claims_per_food = pd.read_sql(query_claims_per_food, engine)
print("Food Claims per Food Item:")
print(df_claims_per_food, "\n")


# # 9) Which provider has had the highest number of successful food claims?

query_top_provider_success = """
SELECT p.Name AS provider_name, COUNT(c.Claim_ID) AS successful_claims
FROM claims c
JOIN food_listings f ON c.Food_ID = f.Food_ID
JOIN providers p ON f.Provider_ID = p.Provider_ID
WHERE c.Status = 'Completed'
GROUP BY p.Name
ORDER BY successful_claims DESC
LIMIT 1;
"""

df_top_provider_success = pd.read_sql(query_top_provider_success, engine)
print("Provider with Most Successful Claims:")
print(df_top_provider_success, "\n")


# # 10) What percentage of food claims are completed vs. pending vs. canceled?

query_claim_status_percentage = """
SELECT Status,
       ROUND((COUNT(*) / (SELECT COUNT(*) FROM claims)) * 100, 2) AS percentage
FROM claims
GROUP BY Status;
"""

df_claim_status_percentage = pd.read_sql(query_claim_status_percentage, engine)
print("Claims Percentage by Status:")
print(df_claim_status_percentage, "\n")


# # 11) What is the average quantity of food claimed per receiver?

query_avg_quantity_per_receiver = """
SELECT r.Name AS receiver_name, 
       ROUND(AVG(f.Quantity), 2) AS avg_quantity_claimed
FROM claims c
JOIN food_listings f ON c.Food_ID = f.Food_ID
JOIN receivers r ON c.Receiver_ID = r.Receiver_ID
GROUP BY r.Name
ORDER BY avg_quantity_claimed DESC;
"""

df_avg_quantity_per_receiver = pd.read_sql(query_avg_quantity_per_receiver, engine)
print("Average Quantity of Food Claimed per Receiver:")
print(df_avg_quantity_per_receiver, "\n")


# # 12) Which meal type (breakfast, lunch, dinner, snacks) is claimed the most?

query_top_meal_type = """
SELECT f.Meal_Type, COUNT(c.Claim_ID) AS claim_count
FROM claims c
JOIN food_listings f ON c.Food_ID = f.Food_ID
GROUP BY f.Meal_Type
ORDER BY claim_count DESC
LIMIT 1;
"""

df_top_meal_type = pd.read_sql(query_top_meal_type, engine)
print("Most Claimed Meal Type:")
print(df_top_meal_type, "\n")


# # 13) What is the total quantity of food donated by each provider?

query_total_quantity_by_provider = """
SELECT p.Name AS provider_name, SUM(f.Quantity) AS total_quantity_donated
FROM food_listings f
JOIN providers p ON f.Provider_ID = p.Provider_ID
GROUP BY p.Name
ORDER BY total_quantity_donated DESC;
"""

df_total_quantity_by_provider = pd.read_sql(query_total_quantity_by_provider, engine)
print("Total Quantity of Food Donated by Each Provider:")
print(df_total_quantity_by_provider, "\n")


# # Step 7: Creating a Streamlit App with Filters
# Now, we'll build a Streamlit app to display the results of our SQL queries. The app will allow users to filter the data by City, Provider type, Food type, Meal type.


# -------------------
# DB CONNECTION
# -------------------
DB_URL = "mysql+mysqlconnector://root:12345678@localhost/food_data"
engine = create_engine(DB_URL)

# -------------------
# PAGE CONFIG
# -------------------
st.set_page_config(page_title="Food Wastage Dashboard", layout="wide")
st.title("🍽️ Food Wastage Analysis Dashboard")

# -------------------
# FILTERS
# -------------------
st.sidebar.header("🔍 Filters")
city = st.sidebar.text_input("City Name").strip()
provider_type = st.sidebar.selectbox("Provider Type", ["", "Restaurant", "Grocery", "NGO", "Others"])
food_type = st.sidebar.selectbox("Food Type", ["", "Vegetarian", "Non-Vegetarian", "Mixed"])
meal_type = st.sidebar.selectbox("Meal Type", ["", "Breakfast", "Lunch", "Dinner", "Snacks"])

# -------------------
# QUERY RUNNER
# -------------------
def run_query(sql, params=None):
    return pd.read_sql(sql, engine, params=params)

st.title("📋 SQL Query Results")
       
# -------------------
# 1. Providers & Receivers by City
# -------------------
st.subheader("1️⃣ Providers & Receivers by City")
df1 = run_query("""
    SELECT City, 
           SUM(CASE WHEN Role='Provider' THEN 1 ELSE 0 END) AS Providers,
           SUM(CASE WHEN Role='Receiver' THEN 1 ELSE 0 END) AS Receivers
    FROM providers
    GROUP BY City
""")
st.dataframe(df1)

# -------------------
# 2. Top Food Provider by Quantity
# -------------------
st.subheader("2️⃣ Top Food Provider by Quantity")
df_top_provider_type = run_query("""
    SELECT Provider_Type, SUM(Quantity) AS Total_Quantity
    FROM food_listings
    GROUP BY Provider_Type
    ORDER BY Total_Quantity DESC
    LIMIT 1
""")
st.dataframe(df_top_provider_type)

# -------------------
# 3. Food Providers Contact Info in a City
# -------------------
st.subheader("3️⃣ Food Providers Contact Info in a City")
selected_city = st.text_input("Enter City for Providers' Contact Info", "")
df_provider_contacts = run_query("""
    SELECT Name, Contact
    FROM providers
    WHERE City = %s
""", (selected_city,))
st.dataframe(df_provider_contacts)

# -------------------
# 4. Top Receivers by Number of Claims
# -------------------
st.subheader("4️⃣ Top Receivers by Number of Claims")
df_top_receivers = run_query("""
    SELECT r.Name, COUNT(c.Claim_ID) AS Claims_Made
    FROM receivers r
    JOIN claims c ON r.Receiver_ID = c.Receiver_ID
    GROUP BY r.Receiver_ID
    ORDER BY Claims_Made DESC
    LIMIT 10
""")
st.dataframe(df_top_receivers)

# -------------------
# 5. Total Quantity of Food Available
# -------------------
st.subheader("5️⃣ Total Quantity of Food Available")
df_total_food = run_query("SELECT SUM(Quantity) AS Total_Food FROM food_listings")
st.write(df_total_food['Total_Food'].iloc[0])
st.dataframe(df_total_food)

# -------------------
# 6. City with Highest Number of Food Listings
# -------------------
st.subheader("6️⃣ City with Highest Number of Food Listings")
df_top_city = run_query("""
    SELECT Location AS City, COUNT(*) AS Listings
    FROM food_listings
    GROUP BY Location
    ORDER BY Listings DESC
    LIMIT 1
""")
st.dataframe(df_top_city)

# -------------------
# 7. Most Commonly Available Food Types
# -------------------
st.subheader("7️⃣ Most Commonly Available Food Types")
df_food_types = run_query("""
    SELECT Food_Type, COUNT(*) AS Count
    FROM food_listings
    GROUP BY Food_Type
    ORDER BY Count DESC
""")
st.dataframe(df_food_types)

# -------------------
# 8. Food Claims per Food Item
# -------------------
st.subheader("8️⃣ Food Claims per Food Item")
df_claims_per_food = run_query("""
    SELECT f.Food_Name, COUNT(c.Claim_ID) AS Claims_Count
    FROM food_listings f
    LEFT JOIN claims c ON f.Food_ID = c.Food_ID
    GROUP BY f.Food_ID
""")
st.dataframe(df_claims_per_food)

# -------------------
# 9. Provider with Highest Successful Claims
# -------------------
st.subheader("9️⃣ Provider with Highest Successful Claims")
df_top_provider_claims = run_query("""
    SELECT f.Provider_ID, p.Name, COUNT(c.Claim_ID) AS Successful_Claims
    FROM food_listings f
    JOIN providers p ON f.Provider_ID = p.Provider_ID
    JOIN claims c ON f.Food_ID = c.Food_ID
    WHERE c.Status = 'Completed'
    GROUP BY f.Provider_ID
    ORDER BY Successful_Claims DESC
    LIMIT 1
""")
st.dataframe(df_top_provider_claims)

# -------------------
# 10. Percentage of Claims by Status
# -------------------
st.subheader("🔟 Percentage of Claims by Status")
df_claims_status = run_query("""
    SELECT Status, COUNT(*) * 100.0 / (SELECT COUNT(*) FROM claims) AS Percentage
    FROM claims
    GROUP BY Status
""")
st.dataframe(df_claims_status)

# -------------------
# 11. Average Quantity Claimed per Receiver
# -------------------
st.subheader("1️⃣1️⃣ Average Quantity Claimed per Receiver")
df_avg_quantity = run_query("""
    SELECT r.Name, AVG(f.Quantity) AS Avg_Quantity
    FROM receivers r
    JOIN claims c ON r.Receiver_ID = c.Receiver_ID
    JOIN food_listings f ON c.Food_ID = f.Food_ID
    GROUP BY r.Receiver_ID
""")
st.dataframe(df_avg_quantity)

# -------------------
# 12. Most Claimed Meal Type
# -------------------
st.subheader("1️⃣2️⃣ Most Claimed Meal Type")
df_meal_type = run_query("""
    SELECT f.Meal_Type, COUNT(c.Claim_ID) AS Claims_Count
    FROM food_listings f
    JOIN claims c ON f.Food_ID = c.Food_ID
    GROUP BY f.Meal_Type
    ORDER BY Claims_Count DESC
""")
st.dataframe(df_meal_type)

# -------------------
# 13. Total Quantity of Food Donated by Each Provider
# -------------------
st.subheader("1️⃣3️⃣ Total Quantity of Food Donated by Each Provider")
df_total_by_provider = run_query("""
    SELECT f.Provider_ID, p.Name, SUM(f.Quantity) AS Total_Quantity
    FROM food_listings f
    JOIN providers p ON f.Provider_ID = p.Provider_ID
    GROUP BY f.Provider_ID
""")
st.dataframe(df_total_by_provider)

# -------------------
# 14. Food Wastage by City
# -------------------
st.subheader("1️⃣4️⃣ Food Wastage by City")
df14 = run_query("""
    SELECT Location AS City, SUM(Quantity) AS Total_Food
    FROM food_listings
    GROUP BY Location
""")
st.dataframe(df14)

# -------------------
# 15. Monthly Donation Trends
# -------------------
st.subheader("1️⃣5️⃣ Monthly Donation Trends")
df15 = run_query("""
    SELECT DATE_FORMAT(Expiry_Date, '%Y-%m') AS Month, SUM(Quantity) AS Total_Donations
    FROM food_listings
    GROUP BY Month
    ORDER BY Month
""")
st.dataframe(df15)

st.title("📞 Contact Details")

# 📞 Contact Details of Providers
st.subheader("📞 Contact Details of Providers for Direct Coordination")

# Ask user for city input
contact_city = st.text_input("Enter City Name").strip()

if contact_city:
    df_contact = pd.read_sql("""
        SELECT Name, Type AS ProviderType, Contact
        FROM providers
        WHERE City = %s
    """, engine, params=(contact_city,))
    
    if not df_contact.empty:
        st.dataframe(df_contact)
    else:
        st.warning(f"No providers found in {contact_city}.")
else:
    st.info("Please enter a city to see provider contact details.")
    
