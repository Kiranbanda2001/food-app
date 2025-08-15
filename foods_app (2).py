#!/usr/bin/env python
# coding: utf-8

# # Local Food Wastage Management System Project

# # Step 1: Load data from CSV files

# In[1]:


import pandas as pd


# In[2]:


providers = pd.read_csv("E:/providers_data.csv")
receivers = pd.read_csv("E:/receivers_data.csv")
food_listings = pd.read_csv("E:/food_listings_data.csv")
claims = pd.read_csv("E:/claims_data.csv")


# In[3]:


providers


# In[4]:


receivers


# In[5]:


food_listings


# In[6]:


claims


# # Step 2: Check null values

# Check Null Values for Providers Dataset.

# In[7]:


providers.isnull().sum()


# Check Null Values for Receivers Dataset.

# In[8]:


receivers.isnull().sum()


# Check Null Values for Food Listings Dataset.

# In[9]:


food_listings.isnull().sum()


# Check Null values for Claims Dataset.

# In[10]:


claims.isnull().sum()


# # Step 3: SQL Connection

# Creating SQL Connection Using Different Methods

# # 1. Using SQLite3 

# In[11]:


import sqlite3
conn = sqlite3.connect("food_data")
cursor = conn.cursor()
print("SQLite connection established!")


# # 2. Using MySQL Connector

# In[12]:


get_ipython().system('pip install mysql-connector-python')


# In[13]:


import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="12345678",

)
cursor = conn.cursor()
print("MySQL connection established!")


# In[14]:


cursor.execute("CREATE DATABASE IF NOT EXISTS food_data")
print("MySQL database 'food_data' created successfully!")


# In[15]:


cursor.execute("use food_data")


# # 3. Using PyMySQL 

# In[16]:


import pymysql

conn_pymysql = pymysql.connect(
    host="localhost",
    user="root",
    password="12345678",
    database="food_data"
)
cursor_pymysql = conn_pymysql.cursor()
print("PyMySQL connection established!")


# # 4. Using SQLAlchemy 

# In[17]:


from sqlalchemy import create_engine

engine = create_engine("mysql+mysqlconnector://root:12345678@localhost/food_data")
print("sqlalchemy connection established!")


# # Step 4: Creating Tables

# # 1. Create a Table in SQLite

# Creating tables for Providers Dataset

# In[18]:


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

# In[19]:


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

# In[20]:


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

# In[21]:


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

# In[22]:


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

# In[23]:


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

# In[24]:


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

# In[25]:


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

# In[26]:


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

# In[27]:


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

# In[28]:


food_listings['Expiry_Date'] = pd.to_datetime(food_listings['Expiry_Date']).dt.strftime('%Y-%m-%d')


# In[29]:


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

# In[30]:


claims['Timestamp'] = pd.to_datetime(claims['Timestamp'])


# In[31]:


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

# In[32]:


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

# In[33]:


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

# In[34]:


city_name = "Adamsville   ".strip()  # Trim trailing spaces

query3 = """
SELECT Name, Type, Contact
FROM providers
WHERE City = %s;
"""

df3 = pd.read_sql(query3, engine, params=(city_name,))
print(df3)


# # 4) Which receivers have claimed the most food?

# In[35]:


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

# In[36]:


query_total_quantity = """
SELECT SUM(Quantity) AS total_food_quantity
FROM food_listings;
"""

df_total_quantity = pd.read_sql(query_total_quantity, engine)
print("Total Quantity of Food Available:")
print(df_total_quantity, "\n")


# # 6) Which city has the highest number of food listings?

# In[37]:


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

# In[38]:


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

# In[39]:


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

# In[40]:


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

# In[41]:


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

# In[42]:


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

# In[43]:


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

# In[44]:


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

# In[45]:


get_ipython().run_cell_magic('writefile', 'foods_app.py', 'import streamlit as st\nimport pandas as pd\nfrom sqlalchemy import create_engine\n\n# -------------------\n# DB CONNECTION\n# -------------------\nDB_URL = "mysql+mysqlconnector://root:12345678@localhost/food_data"\nengine = create_engine(DB_URL)\n\n# -------------------\n# PAGE CONFIG\n# -------------------\nst.set_page_config(page_title="Food Wastage Dashboard", layout="wide")\nst.title("🍽️ Food Wastage Analysis Dashboard")\n\n# -------------------\n# FILTERS\n# -------------------\nst.sidebar.header("🔍 Filters")\ncity = st.sidebar.text_input("City Name").strip()\nprovider_type = st.sidebar.selectbox("Provider Type", ["", "Restaurant", "Grocery", "NGO", "Others"])\nfood_type = st.sidebar.selectbox("Food Type", ["", "Vegetarian", "Non-Vegetarian", "Mixed"])\nmeal_type = st.sidebar.selectbox("Meal Type", ["", "Breakfast", "Lunch", "Dinner", "Snacks"])\n\n# -------------------\n# QUERY RUNNER\n# -------------------\ndef run_query(sql, params=None):\n    return pd.read_sql(sql, engine, params=params)\n\nst.title("📋 SQL Query Results")\n       \n# -------------------\n# 1. Providers & Receivers by City\n# -------------------\nst.subheader("1️⃣ Providers & Receivers by City")\ndf1 = run_query("""\n    SELECT City, \n           SUM(CASE WHEN Role=\'Provider\' THEN 1 ELSE 0 END) AS Providers,\n           SUM(CASE WHEN Role=\'Receiver\' THEN 1 ELSE 0 END) AS Receivers\n    FROM providers\n    GROUP BY City\n""")\nst.dataframe(df1)\n\n# -------------------\n# 2. Top Food Provider by Quantity\n# -------------------\nst.subheader("2️⃣ Top Food Provider by Quantity")\ndf_top_provider_type = run_query("""\n    SELECT Provider_Type, SUM(Quantity) AS Total_Quantity\n    FROM food_listings\n    GROUP BY Provider_Type\n    ORDER BY Total_Quantity DESC\n    LIMIT 1\n""")\nst.dataframe(df_top_provider_type)\n\n# -------------------\n# 3. Food Providers Contact Info in a City\n# -------------------\nst.subheader("3️⃣ Food Providers Contact Info in a City")\nselected_city = st.text_input("Enter City for Providers\' Contact Info", "")\ndf_provider_contacts = run_query("""\n    SELECT Name, Contact\n    FROM providers\n    WHERE City = %s\n""", (selected_city,))\nst.dataframe(df_provider_contacts)\n\n# -------------------\n# 4. Top Receivers by Number of Claims\n# -------------------\nst.subheader("4️⃣ Top Receivers by Number of Claims")\ndf_top_receivers = run_query("""\n    SELECT r.Name, COUNT(c.Claim_ID) AS Claims_Made\n    FROM receivers r\n    JOIN claims c ON r.Receiver_ID = c.Receiver_ID\n    GROUP BY r.Receiver_ID\n    ORDER BY Claims_Made DESC\n    LIMIT 10\n""")\nst.dataframe(df_top_receivers)\n\n# -------------------\n# 5. Total Quantity of Food Available\n# -------------------\nst.subheader("5️⃣ Total Quantity of Food Available")\ndf_total_food = run_query("SELECT SUM(Quantity) AS Total_Food FROM food_listings")\nst.write(df_total_food[\'Total_Food\'].iloc[0])\nst.dataframe(df_total_food)\n\n# -------------------\n# 6. City with Highest Number of Food Listings\n# -------------------\nst.subheader("6️⃣ City with Highest Number of Food Listings")\ndf_top_city = run_query("""\n    SELECT Location AS City, COUNT(*) AS Listings\n    FROM food_listings\n    GROUP BY Location\n    ORDER BY Listings DESC\n    LIMIT 1\n""")\nst.dataframe(df_top_city)\n\n# -------------------\n# 7. Most Commonly Available Food Types\n# -------------------\nst.subheader("7️⃣ Most Commonly Available Food Types")\ndf_food_types = run_query("""\n    SELECT Food_Type, COUNT(*) AS Count\n    FROM food_listings\n    GROUP BY Food_Type\n    ORDER BY Count DESC\n""")\nst.dataframe(df_food_types)\n\n# -------------------\n# 8. Food Claims per Food Item\n# -------------------\nst.subheader("8️⃣ Food Claims per Food Item")\ndf_claims_per_food = run_query("""\n    SELECT f.Food_Name, COUNT(c.Claim_ID) AS Claims_Count\n    FROM food_listings f\n    LEFT JOIN claims c ON f.Food_ID = c.Food_ID\n    GROUP BY f.Food_ID\n""")\nst.dataframe(df_claims_per_food)\n\n# -------------------\n# 9. Provider with Highest Successful Claims\n# -------------------\nst.subheader("9️⃣ Provider with Highest Successful Claims")\ndf_top_provider_claims = run_query("""\n    SELECT f.Provider_ID, p.Name, COUNT(c.Claim_ID) AS Successful_Claims\n    FROM food_listings f\n    JOIN providers p ON f.Provider_ID = p.Provider_ID\n    JOIN claims c ON f.Food_ID = c.Food_ID\n    WHERE c.Status = \'Completed\'\n    GROUP BY f.Provider_ID\n    ORDER BY Successful_Claims DESC\n    LIMIT 1\n""")\nst.dataframe(df_top_provider_claims)\n\n# -------------------\n# 10. Percentage of Claims by Status\n# -------------------\nst.subheader("🔟 Percentage of Claims by Status")\ndf_claims_status = run_query("""\n    SELECT Status, COUNT(*) * 100.0 / (SELECT COUNT(*) FROM claims) AS Percentage\n    FROM claims\n    GROUP BY Status\n""")\nst.dataframe(df_claims_status)\n\n# -------------------\n# 11. Average Quantity Claimed per Receiver\n# -------------------\nst.subheader("1️⃣1️⃣ Average Quantity Claimed per Receiver")\ndf_avg_quantity = run_query("""\n    SELECT r.Name, AVG(f.Quantity) AS Avg_Quantity\n    FROM receivers r\n    JOIN claims c ON r.Receiver_ID = c.Receiver_ID\n    JOIN food_listings f ON c.Food_ID = f.Food_ID\n    GROUP BY r.Receiver_ID\n""")\nst.dataframe(df_avg_quantity)\n\n# -------------------\n# 12. Most Claimed Meal Type\n# -------------------\nst.subheader("1️⃣2️⃣ Most Claimed Meal Type")\ndf_meal_type = run_query("""\n    SELECT f.Meal_Type, COUNT(c.Claim_ID) AS Claims_Count\n    FROM food_listings f\n    JOIN claims c ON f.Food_ID = c.Food_ID\n    GROUP BY f.Meal_Type\n    ORDER BY Claims_Count DESC\n""")\nst.dataframe(df_meal_type)\n\n# -------------------\n# 13. Total Quantity of Food Donated by Each Provider\n# -------------------\nst.subheader("1️⃣3️⃣ Total Quantity of Food Donated by Each Provider")\ndf_total_by_provider = run_query("""\n    SELECT f.Provider_ID, p.Name, SUM(f.Quantity) AS Total_Quantity\n    FROM food_listings f\n    JOIN providers p ON f.Provider_ID = p.Provider_ID\n    GROUP BY f.Provider_ID\n""")\nst.dataframe(df_total_by_provider)\n\n# -------------------\n# 14. Food Wastage by City\n# -------------------\nst.subheader("1️⃣4️⃣ Food Wastage by City")\ndf14 = run_query("""\n    SELECT Location AS City, SUM(Quantity) AS Total_Food\n    FROM food_listings\n    GROUP BY Location\n""")\nst.dataframe(df14)\n\n# -------------------\n# 15. Monthly Donation Trends\n# -------------------\nst.subheader("1️⃣5️⃣ Monthly Donation Trends")\ndf15 = run_query("""\n    SELECT DATE_FORMAT(Expiry_Date, \'%Y-%m\') AS Month, SUM(Quantity) AS Total_Donations\n    FROM food_listings\n    GROUP BY Month\n    ORDER BY Month\n""")\nst.dataframe(df15)\n\nst.title("📞 Contact Details")\n\n# 📞 Contact Details of Providers\nst.subheader("📞 Contact Details of Providers for Direct Coordination")\n\n# Ask user for city input\ncontact_city = st.text_input("Enter City Name").strip()\n\nif contact_city:\n    df_contact = pd.read_sql("""\n        SELECT Name, Type AS ProviderType, Contact\n        FROM providers\n        WHERE City = %s\n    """, engine, params=(contact_city,))\n    \n    if not df_contact.empty:\n        st.dataframe(df_contact)\n    else:\n        st.warning(f"No providers found in {contact_city}.")\nelse:\n    st.info("Please enter a city to see provider contact details.")\n')


# In[ ]:


get_ipython().system('streamlit run foods_app.py')

