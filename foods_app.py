
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# Load CSV directly from local file (in repo)
providers = pd.read_csv("providers_data.csv")
receivers = pd.read_csv("receivers_data.csv")
food_listings = pd.read_csv("food_listings_data.csv")
claims = pd.read_csv("claims_data.csv")

st.title("Local Food Wastage Management System")

st.subheader("Providers Data")
st.dataframe(providers)

st.subheader("Receivers Data")
st.dataframe(receivers)

st.subheader("Food Listings")
st.dataframe(food_listings)

st.subheader("Claims")
st.dataframe(claims)

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




