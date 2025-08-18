import mysql.connector
import streamlit as st
import pandas as pd
from mysql.connector import Error
from sqlalchemy import create_engine

try:
    conn = mysql.connector.connect(
        host="sql12.freesqldatabase.com",
        user="sql12795265",
        password="pPv6CqXmEu",
        database="sql12795265"
    )

    if conn.is_connected():
        print("✅ Connected successfully to MySQL!")

except Error as e:
    print(f"❌ Error: {e}")

st.set_page_config(page_title="Food Wastage Dashboard", layout="wide")
st.markdown(
    """
    <div style="
        background-color: #336699;
        padding: 20px;
        border-radius: 12px;
        border: 2px solid #204060;
        text-align: center;
        color: white;
        font-size: 30px;
        font-weight: bold;
        box-shadow: 2px 2px 12px rgba(0,0,0,0.4);">
        🍽️ Food Wastage Management Dashboard
    </div>
    """,
    unsafe_allow_html=True
)

# -------------------
# DB CONNECTION
# -------------------
DB_URL = "mysql+mysqlconnector://sql12795265:pPv6CqXmEu@sql12.freesqldatabase.com/sql12795265"
engine = create_engine(DB_URL)

# -------------------
# FILTERS
# -------------------
st.sidebar.header("🔍 Filters")
city = st.sidebar.text_input("City Name").strip()
provider_type = st.sidebar.selectbox("Provider Type", ["", "Restaurant", "Grocery", "NGO", "Others"])
food_type = st.sidebar.selectbox("Food Type", ["", "Vegetarian", "Non-Vegetarian", "Mixed"])
meal_type = st.sidebar.selectbox("Meal Type", ["", "Breakfast", "Lunch", "Dinner", "Snacks"])

# QUERY RUNNER
# -------------------
def run_query(sql, params=None):
    return pd.read_sql(sql, engine, params=params)

st.markdown(
    "<h3 style='text-align: left; color: #336699;'>📋 SQL Queries</h3>",
    unsafe_allow_html=True
)

st.markdown(
    "<h3 style='text-align: left; color: #336699;'>📋 Select a query</h3>",
    unsafe_allow_html=True
)

query_list = [
    "1. Providers & Receivers by City",
    "2. Top Food Provider by Quantity",
    "3. Food Providers Contact Info in a City",
    "4. Top Receivers by Number of Claims",
    "5. Total Quantity of Food Available",
    "6. City with Highest Number of Food Listings",
    "7. Most Commonly Available Food Types",
    "8. Food Claims per Food Item",
    "9. Provider with Highest Successful Claims",
    "10. Percentage of Claims by Status",
    "11. Top 10 Most Frequently Donated Food Items",
    "12. Most Claimed Meal Type",
    "13. Total Quantity of Food Donated by Each Provider",
    "14. Food Wastage by City",
    "15. Monthly Donation Trends"
]

selected_query = st.selectbox("Select a query to run:", query_list)

st.markdown(
    "<h3 style='text-align: left; color: #336699;'>📋 query Result</h3>",
    unsafe_allow_html=True
)
# -------------------
# Queries handling
# -------------------
if selected_query == "1. Providers & Receivers by City":   # ✅ first must be if
    df1 = run_query("""
        SELECT p.City,
               COUNT(DISTINCT p.Provider_ID) AS provider_count,
               COUNT(DISTINCT r.Receiver_ID) AS receiver_count
        FROM providers p
        LEFT JOIN receivers r ON p.City = r.City
        GROUP BY p.City;
    """)
    st.subheader("1️⃣ Providers & Receivers by City")
    st.dataframe(df1)

elif selected_query == "2. Top Food Provider by Quantity":   # ✅ now elif works
    df2 = run_query("""
        SELECT p.Type, SUM(f.Quantity) AS Total_Quantity
        FROM food_listings f
        JOIN providers p ON f.Provider_ID = p.Provider_ID
        GROUP BY p.Type
        ORDER BY Total_Quantity DESC
        LIMIT 1
    """)
    st.subheader("2️⃣ Top Food Provider by Quantity")
    st.dataframe(df2)

elif selected_query == "3. Food Providers Contact Info in a City":
    # 🔹 Get distinct cities from providers
    city_list = run_query("SELECT DISTINCT City FROM providers;")["City"].tolist()

    # 🔹 Dropdown for city selection
    city = st.selectbox("Select City:", city_list)
    
    # 🔹 Query providers by selected city
    df3 = run_query(f"""
        SELECT Name, Contact, City
        FROM providers
        WHERE City = '{city}';
    """)
    
    st.subheader(f"3️⃣ Food Providers Contact Info in {city}")
    
    if df3.empty:
        st.warning(f"No providers found in {city}")
    else:
        st.dataframe(df3)
    
elif selected_query == "4. Top Receivers by Number of Claims":
    df4 = run_query("""
        SELECT r.Name, COUNT(c.Claim_ID) AS Claim_Count
        FROM receivers r
        JOIN claims c ON r.Receiver_ID = c.Receiver_ID
        GROUP BY r.Name
        ORDER BY Claim_Count DESC
        LIMIT 5;
    """)
    st.subheader("4️⃣ Top Receivers by Number of Claims")
    st.dataframe(df4)

elif selected_query == "5. Total Quantity of Food Available":
    df5 = run_query("""
        SELECT SUM(Quantity) AS Total_Food_Quantity
        FROM food_listings;
    """)
    st.subheader("5️⃣ Total Quantity of Food Available")
    st.dataframe(df5)

elif selected_query == "6. City with Highest Number of Food Listings":
    df6 = run_query("""
        SELECT City, COUNT(*) AS Listings
        FROM food_listings
        GROUP BY City
        ORDER BY Listings DESC
        LIMIT 1;
    """)
    st.subheader("6️⃣ City with Highest Number of Food Listings")
    st.dataframe(df6)

elif selected_query == "7. Most Commonly Available Food Types":
    df7 = run_query("""
        SELECT Food_Type, COUNT(*) AS Count
        FROM food_listings
        GROUP BY Food_Type
        ORDER BY Count DESC;
    """)
    st.subheader("7️⃣ Most Commonly Available Food Types")
    st.dataframe(df7)

elif selected_query == "8. Food Claims per Food Item":
    df8 = run_query("""
        SELECT f.Food_Name, COUNT(c.Claim_ID) AS Claim_Count
        FROM food_listings f
        LEFT JOIN claims c ON f.Food_ID = c.Food_ID
        GROUP BY f.Food_Name;
    """)
    st.subheader("8️⃣ Food Claims per Food Item")
    st.dataframe(df8)

elif selected_query == "9. Provider with Highest Successful Claims":
    df9 = run_query("""
        SELECT p.Name, COUNT(c.Claim_ID) AS Successful_Claims
        FROM providers p
        JOIN food_listings f ON p.Provider_ID = f.Provider_ID
        JOIN claims c ON f.Food_ID = c.Food_ID
        WHERE c.Status = 'Approved'
        GROUP BY p.Name
        ORDER BY Successful_Claims DESC
        LIMIT 1;
    """)
    st.subheader("9️⃣ Provider with Highest Successful Claims")
    st.dataframe(df9)

elif selected_query == "10. Percentage of Claims by Status":
    df10 = run_query("""
        SELECT Status,
               COUNT(*) * 100.0 / (SELECT COUNT(*) FROM claims) AS Percentage
        FROM claims
        GROUP BY Status;
    """)
    st.subheader("🔟 Percentage of Claims by Status")
    st.dataframe(df10)

elif selected_query == "11. Top 10 Most Frequently Donated Food Items":
    df11 = run_query("""
        SELECT Food_Name, COUNT(*) AS Donation_Count
        FROM food_listings
        GROUP BY Food_Name
        ORDER BY Donation_Count DESC
        LIMIT 10;
    """)
    st.subheader("1️⃣1️⃣ Top 10 Most Frequently Donated Food Items")
    st.dataframe(df11)

elif selected_query == "12. Most Claimed Meal Type":
    df12 = run_query("""
        SELECT Meal_Type, COUNT(*) AS Claim_Count
        FROM food_listings f
        JOIN claims c ON f.Food_ID = c.Food_ID
        GROUP BY Meal_Type
        ORDER BY Claim_Count DESC
        LIMIT 1;
    """)
    st.subheader("1️⃣2️⃣ Most Claimed Meal Type")
    st.dataframe(df12)

elif selected_query == "13. Total Quantity of Food Donated by Each Provider":
    df13 = run_query("""
        SELECT p.Name, SUM(f.Quantity) AS Total_Donated
        FROM providers p
        JOIN food_listings f ON p.Provider_ID = f.Provider_ID
        GROUP BY p.Name;
    """)
    st.subheader("1️⃣3️⃣ Total Quantity of Food Donated by Each Provider")
    st.dataframe(df13)

elif selected_query == "14. Food Wastage by City":
    df14 = run_query("""
        SELECT City, SUM(Quantity) AS Wasted_Quantity
        FROM food_listings
        WHERE Expiry_Date < CURDATE()
        GROUP BY City;
    """)
    st.subheader("1️⃣4️⃣ Food Wastage by City")
    st.dataframe(df14)

elif selected_query == "15. Monthly Donation Trends":
    df15 = run_query("""
        SELECT DATE_FORMAT(Expiry_Date, '%Y-%m') AS Month,
               SUM(Quantity) AS Total_Donations
        FROM food_listings
        GROUP BY Month
        ORDER BY Month;
    """)
    st.subheader("1️⃣5️⃣ Monthly Donation Trends")
    st.dataframe(df15)

else:
    st.warning("⚠️ Please select a query from the sidebar.")

# -------------------
# Contact Details Section
# -------------------
st.title("📞 Contact Details")
st.subheader("📞 Contact Details of Providers for Direct Coordination")

contact_city = st.text_input("Enter City Name for Contact Search").strip()
if contact_city:
    df_contact = run_query("""
        SELECT Name, Provider_Type, Contact
        FROM providers
        WHERE City = %s
    """, (contact_city,))
    if not df_contact.empty:
        st.dataframe(df_contact)
    else:

        st.warning(f"No providers found in {contact_city}.")
