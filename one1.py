import pandas as pd
import pymysql
import pymongo
import numpy as np
from datetime import datetime
import sys


# Validate command-line arguments
if len(sys.argv) != 3:
    print("Usage: python script.py <start_date> <end_date>")
    print("Dates should be in the format YYYY-MM-DD.")
    sys.exit(1)

# Read dates from command-line arguments
start_date = sys.argv[1]
end_date = sys.argv[2]

# Validate the date format
try:
    datetime.strptime(start_date, "%Y-%m-%d")
    datetime.strptime(end_date, "%Y-%m-%d")
except ValueError:
    print("Invalid date format. Please use YYYY-MM-DD.")
    sys.exit(1)

# Dynamically update the SQL query with the input dates
query = f"""
select
b.booking_id,
b.status,
b.city_id,
b.is_picknow,
b.created_at,
b.pickup_datetime,
f.driver_assigned_at,
b.driver_id as assigned_driver_id,
app_cancellationlog.cancelled_at,
bd.distance,
max(CAST(new_value as SIGNED)) as pickup_fare_added,
c.additional_charge,
round(max(c.additional_charge)*.80,2) as pickup_fare_charged_from_customer,
trip_incentive,
((0.80*c.additional_charge) + trip_incentive) total_to_driver,
a.created_at as pickup_fare_additions,
e.first_name, e.last_name,
    customer_mobile,cancellation_reason
from app_bookingmodificationlog as a
    left join app_booking as b on b.id=a.booking_id
    left join app_bookingfare as c on c.booking_id=a.booking_id
    left join app_bookingassigndetail as bd on bd.booking_id=a.booking_id
    left join settlements_driverearning as d on d.booking_id=a.booking_id
    left join auth_user as e on e.id=b.user_id
    left join app_cancellationlog on app_cancellationlog.booking_id=b.id
    left join app_bookingsummary as f on f.booking_id=b.id

where
a.action = 'optional_fare' #and b.status=5
and date(b.pickup_datetime) >= "{start_date}" and date(b.pickup_datetime) <= "{end_date}"
and customer_mobile not in (9392724650,9591419333,9900151005,1987654321)
group by a.booking_id
"""

# Create pymysql connection
connection = pymysql.connect(
    host='168.119.136.234', 
    user='shubham', 
    password='w3;6NFyd3OR1wF', 
    database='driveu'
)

# Fetch data using the dynamically updated query
booking_data = pd.read_sql(query, con=connection)
connection.close()
booking_data["created_at"] = pd.to_datetime(booking_data["created_at"])
booking_data["pickup_fare_additions"] = pd.to_datetime(booking_data["pickup_fare_additions"])
booking_data["cancelled_at"] = pd.to_datetime(booking_data["cancelled_at"])

# Calculate the time difference in minutes
booking_data["time_after_pickup_fare_added"] = (
    (booking_data["pickup_fare_additions"] - booking_data["created_at"]).dt.total_seconds() / 60
)
booking_data["time_cancelled_after_pickup_added"] = (
    (booking_data["cancelled_at"] - booking_data["pickup_fare_additions"]).dt.total_seconds() / 60
)

# MongoDB connection
client = pymongo.MongoClient("mongodb://hemant:8q844#y@46.4.33.102:27017/trackdriver")
db = client['trackdriver']
collection = db["booking_impression"]

# Extract booking_ids from the CSV file
booking_ids = list(booking_data['booking_id'])

# Initialize an empty list to store all documents
all_documents = []

# Query MongoDB in batches by splitting the list of booking_ids
batch_size = 1000  # Adjust based on your system's capabilities
for i in range(0, len(booking_ids), batch_size):
    batch_ids = booking_ids[i:i + batch_size]
    print(f"Fetching data for batch {i//batch_size + 1}...")

    documents = collection.find(
        {
            "$and": [
                {"booking_id": {"$in": batch_ids}}  # Use $in for batch processing
            ]
        },
        {"_id": 0, "booking_id": 1, "event_time": 1, "driver_id": 1, "event_name": 1,"meta_data":1}
    )

    for doc in documents:
        all_documents.append(doc)

# Convert the list of documents to a DataFrame
if all_documents:
    impression_data = pd.DataFrame(all_documents)
    impression_data.to_csv("booking_impression_data_correct.csv", index=False)
else:
    print("No documents found for the specified booking_ids.")

impression_data['booking_id'].nunique()
booking_impression_data = pd.merge(booking_data, impression_data, how='left', on='booking_id')
booking_impression_data_final = booking_impression_data[~booking_impression_data['meta_data'].isnull()]

# Extract the unique booking IDs from the first query result
booking_ids = list(booking_impression_data_final['booking_id'].unique())

# Format the booking IDs into a string for the SQL IN clause
booking_ids_str = ', '.join([f"'{id_}'" for id_ in booking_ids])

# Define the second query with dynamically passed booking IDs
query = f"""
select 
    dispatch_broadcastbooking.booking_id as booking_id_org,
    app_booking.booking_id,
    dispatch_broadcastbooking.created_at as broadcasted_at,
    dispatch_broadcastbooking.driver_id,
    dispatch_incentivebookingbroadcast.distance as broadcast_distance, 
    dispatch_incentivebookingbroadcast.incentive_amount as incentive_amount
from app_booking
left join dispatch_broadcastbooking on app_booking.id = dispatch_broadcastbooking.booking_id
left join dispatch_incentivebookingbroadcast on dispatch_broadcastbooking.id = dispatch_incentivebookingbroadcast.broadcast_booking_id
where app_booking.booking_id in ({booking_ids_str})
"""

# Create pymysql connection
connection = pymysql.connect(
    host='168.119.136.234', 
    user='shubham', 
    password='w3;6NFyd3OR1wF', 
    database='driveu'
)

broadcast_data = pd.read_sql(query, con=connection)
connection.close()

broadcast_data['booking_id'].nunique()
booking_impression_broadcast_data = pd.merge(booking_impression_data_final, broadcast_data, how='left', on=['booking_id','driver_id'])
booking_impression_broadcast_data_actual = booking_impression_broadcast_data[~booking_impression_broadcast_data['booking_id_org'].isnull()]
len(booking_impression_broadcast_data_actual['booking_id'].unique())
booking_id_broad_casted = list(booking_impression_broadcast_data_actual['booking_id'].unique())
data_non_broadcasting_booking = booking_impression_broadcast_data[
    ~booking_impression_broadcast_data['booking_id'].isin(booking_id_broad_casted)
]
data_non_broadcasting_booking = data_non_broadcasting_booking[data_non_broadcasting_booking['meta_data'].apply(lambda x: isinstance(x, dict))]
data_non_broadcasting_booking['pf'] = data_non_broadcasting_booking['meta_data'].apply(lambda x: x.get('pf', 0))
data_non_broadcasting_booking['pf_elg'] = data_non_broadcasting_booking['meta_data'].apply(lambda x: x.get('pf_elg', -1))
data_non_broadcasting_booking['pickup_fare_additions'] = pd.to_datetime(data_non_broadcasting_booking['pickup_fare_additions'])
data_non_broadcasting_booking['event_time'] = pd.to_datetime(data_non_broadcasting_booking['event_time'])
data_non_broadcasting_booking['event_time'] += pd.Timedelta(hours=5, minutes=30)
data_non_broadcasting_booking['category'] = np.where((data_non_broadcasting_booking['pickup_fare_additions'] > data_non_broadcasting_booking['event_time']) & (data_non_broadcasting_booking['pf']==0), 
                          "before_adding", 
                          "after_adding")
data_non_broadcasting_booking['booking_id'].nunique()

# Additional analysis for non-broadcasting bookings
# Analysis for 'before_adding' category
data_before_fare = data_non_broadcasting_booking[data_non_broadcasting_booking['category'] == 'before_adding']
print(data_before_fare['booking_id'].nunique())

driver_event_mapping = data_before_fare.groupby(["booking_id", "driver_id"])["event_name"].agg(set).reset_index()

driver_event_mapping["event_category"] = driver_event_mapping["event_name"].apply(
    lambda x: "only_booking_impression" if x == {"booking_impression"} else
              "only_booking_details_open" if x == {"booking_details_open"} else
              "both" if {"booking_impression", "booking_details_open"}.issubset(x) else
              "none"
)

category_counts = (
    driver_event_mapping.groupby("booking_id")["event_category"]
    .value_counts()
    .unstack(fill_value=0)
    .rename(columns={
        "only_booking_impression": "drivers_with_only_booking_impression",
        "only_booking_details_open": "drivers_with_only_booking_details_open",
        "both": "drivers_with_both_events",
        "none": "drivers_with_no_events"
    })
)

result_before_fare = (
    data_before_fare.groupby("booking_id")
    .agg(total_unique_drivers=("driver_id", lambda x: x.nunique()))
    .join(pd.crosstab(data_before_fare["booking_id"], data_before_fare["event_name"]))
    .join(category_counts)
    .reset_index()
)

additional_columns = data_before_fare.groupby("booking_id").agg({
    "created_at": "first",  
    "cancelled_at": "first",  
    "status": "first",  
    "pickup_fare_additions": "first",
    "time_after_pickup_fare_added": "first",
    "time_cancelled_after_pickup_added": "first",
    "cancellation_reason": "first"
}).reset_index()

final_result_with_columns = result_before_fare.merge(additional_columns, on="booking_id", how="left")

final_result_with_columns['created_at'] = pd.to_datetime(final_result_with_columns['created_at'], errors='coerce')
final_result_with_columns['cancelled_at'] = pd.to_datetime(final_result_with_columns['cancelled_at'], errors='coerce')
final_result_with_columns['pickup_fare_additions'] = pd.to_datetime(final_result_with_columns['pickup_fare_additions'], errors='coerce')

final_result_with_columns['created_at'] = final_result_with_columns['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
final_result_with_columns['cancelled_at'] = final_result_with_columns['cancelled_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
final_result_with_columns['pickup_fare_additions'] = final_result_with_columns['pickup_fare_additions'].dt.strftime('%Y-%m-%d %H:%M:%S')
final_result_with_columns['time_after_pickup_fare_added'] = final_result_with_columns['time_after_pickup_fare_added'].round(2)
final_result_with_columns['time_cancelled_after_pickup_added'] = final_result_with_columns['time_cancelled_after_pickup_added'].round(2)

final_result_with_columns.to_csv(f"non_broadcasted_before_adding_fare_analysis_{str(start_date)}_to{str(end_date)}.csv", index=False)
print(final_result_with_columns.head())
# # Filter data for 'after_adding' category
data_after_fare = data_non_broadcasting_booking[data_non_broadcasting_booking['category'] == 'after_adding']
data_after_fare['booking_id'].nunique()
# Categorize drivers based on `pf_elg`
data_after_fare["pf_elg_category"] = data_after_fare["pf_elg"].apply(
    lambda x: "with_pf_elg" if x == 1 else "without_pf_elg"
)

# Group by booking_id, pf_elg_category, and driver_id to count each driver only once
grouped_data = data_after_fare.groupby(
    ["booking_id", "pf_elg_category", "driver_id"]
)["event_name"].apply(set).reset_index()

# Categorize each driver based on their events
grouped_data["event_category"] = grouped_data["event_name"].apply(
    lambda x: "both" if {"booking_impression", "booking_details_open"}.issubset(x) else
              "only_booking_impression" if "booking_impression" in x else
              "only_booking_details_open" if "booking_details_open" in x else
              "none"
)

# Aggregate metrics per booking_id and pf_elg_category
metrics = grouped_data.groupby(["booking_id", "pf_elg_category"]).agg(
    total_unique_drivers=("driver_id", "nunique"),  # Count unique drivers
    drivers_with_only_booking_impression=("event_category", lambda x: (x == "only_booking_impression").sum()),
    drivers_with_only_booking_details_open=("event_category", lambda x: (x == "only_booking_details_open").sum()),
    drivers_with_both_events=("event_category", lambda x: (x == "both").sum()),
    drivers_with_no_events=("event_category", lambda x: (x == "none").sum()),
).reset_index()

# Add total event counts (booking_impression and booking_details_open) per booking_id and pf_elg_category
event_counts = pd.crosstab(
    [data_after_fare["booking_id"], data_after_fare["pf_elg_category"]],
    data_after_fare["event_name"]
).reset_index()

# Rename the event counts to be more descriptive
event_counts = event_counts.rename(columns={
    "booking_impression": "total_booking_impression_count",
    "booking_details_open": "total_booking_details_open_count"
})

# Merge metrics with event counts
final_result = metrics.merge(event_counts, on=["booking_id", "pf_elg_category"], how="left")

# Add additional columns like created_at, cancelled_at, etc.
additional_columns = data_after_fare.groupby(
    ["booking_id", "pf_elg_category"]
).agg({
    "created_at": "first",
    "cancelled_at": "first",
    "status": "first",
    "pickup_fare_additions": "first",
    "time_after_pickup_fare_added": "first",
    "time_cancelled_after_pickup_added": "first",
    "cancellation_reason": "first"
}).reset_index()

# Merge all results
final_result_with_columns = final_result.merge(additional_columns, on=["booking_id", "pf_elg_category"], how="left")
final_result_with_columns['created_at'] = pd.to_datetime(final_result_with_columns['created_at'], errors='coerce')
final_result_with_columns['cancelled_at'] = pd.to_datetime(final_result_with_columns['cancelled_at'], errors='coerce')
final_result_with_columns['pickup_fare_additions'] = pd.to_datetime(final_result_with_columns['pickup_fare_additions'], errors='coerce')

final_result_with_columns['created_at'] = final_result_with_columns['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
final_result_with_columns['cancelled_at'] = final_result_with_columns['cancelled_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
final_result_with_columns['pickup_fare_additions'] = final_result_with_columns['pickup_fare_additions'].dt.strftime('%Y-%m-%d %H:%M:%S')
final_result_with_columns['time_after_pickup_fare_added'] = final_result_with_columns['time_after_pickup_fare_added'].round(2)
final_result_with_columns['time_cancelled_after_pickup_added'] = final_result_with_columns['time_cancelled_after_pickup_added'].round(2)


# Save the final result
final_result_with_columns.to_csv(f"non_broadcasted_after_adding_fare_analysis_{str(start_date)}_to{str(end_date)}.csv", index=False)

print(final_result_with_columns.head())
data_broadcasting_booking = booking_impression_broadcast_data[
    booking_impression_broadcast_data['booking_id'].isin(booking_id_broad_casted)
]
data_broadcasting_booking['booking_id'].nunique()
data_broadcasting_booking = data_broadcasting_booking[data_broadcasting_booking['meta_data'].apply(lambda x: isinstance(x, dict))]
data_broadcasting_booking['pf'] = data_broadcasting_booking['meta_data'].apply(lambda x: x.get('pf', 0))
data_broadcasting_booking['pf_elg']=data_broadcasting_booking['meta_data'].apply(lambda x: x.get('pf_elg', -1))
import numpy as np
data_broadcasting_booking['pickup_fare_additions'] = pd.to_datetime(data_broadcasting_booking['pickup_fare_additions'])
data_broadcasting_booking['event_time'] = pd.to_datetime(data_broadcasting_booking['event_time'])
data_broadcasting_booking['event_time'] = data_broadcasting_booking['event_time'] + pd.Timedelta(hours=5, minutes=30)
data_broadcasting_booking['category'] = np.where((data_broadcasting_booking['pickup_fare_additions'] > data_broadcasting_booking['event_time']) & (data_broadcasting_booking['pf']==0), 
                          "before_adding", 
                          "after_adding")
data_broadcasting_booking[data_broadcasting_booking['status']==5]['booking_id'].nunique()
import pandas as pd

# Assuming `before_adding_broadcast_df` is already loaded as a DataFrame

# Filter data for 'before_adding' category
before_adding_broadcast_df = data_broadcasting_booking[data_broadcasting_booking['category'] == 'before_adding']

# Create a pivot table to find the unique event types for each driver_id per booking_id
driver_event_mapping = before_adding_broadcast_df.groupby(["booking_id", "driver_id"])["event_name"].agg(set).reset_index()

# Categorize drivers based on the event types they performed
driver_event_mapping["event_category"] = driver_event_mapping["event_name"].apply(
    lambda x: "only_booking_impression" if x == {"booking_impression"} else
              "only_booking_details_open" if x == {"booking_details_open"} else
              "both" if {"booking_impression", "booking_details_open"}.issubset(x) else
              "none"
)

# Count drivers for each event category per booking_id
category_counts = (
    driver_event_mapping.groupby("booking_id")["event_category"]
    .value_counts()
    .unstack(fill_value=0)
    .rename(columns={
        "only_booking_impression": "drivers_with_only_booking_impression",
        "only_booking_details_open": "drivers_with_only_booking_details_open",
        "both": "drivers_with_both_events",
        "none": "drivers_with_no_events"
    })
)

# Add the event category counts to the main DataFrame
result_before_adding_broadcast_df = (
    before_adding_broadcast_df.groupby("booking_id")
    .agg(total_unique_drivers=("driver_id", lambda x: x.nunique()))  # Count unique drivers
    .join(pd.crosstab(before_adding_broadcast_df["booking_id"], before_adding_broadcast_df["event_name"]))  # Event type counts
    .join(category_counts)  # Driver event category counts
    .reset_index()
)

# Add additional columns like created_at, cancelled_at, etc.
additional_columns = before_adding_broadcast_df.groupby("booking_id").agg({
    "created_at": "first",  # Take the first value for created_at
    "cancelled_at": "first",  # Take the first value for cancelled_at
    "status": "first",  # Take the first value for status
    "pickup_fare_additions": "first",
    "time_after_pickup_fare_added": "first",
    "time_cancelled_after_pickup_added": "first",
    "cancellation_reason": "first"
}).reset_index()

# Merge all results
final_result_with_columns = result_before_adding_broadcast_df.merge(additional_columns, on="booking_id", how="left")

# Convert `pickup_fare_additions` to datetime
final_result_with_columns['pickup_fare_additions'] = pd.to_datetime(final_result_with_columns['pickup_fare_additions'])
final_result_with_columns['created_at'] = pd.to_datetime(final_result_with_columns['created_at'], errors='coerce')
final_result_with_columns['cancelled_at'] = pd.to_datetime(final_result_with_columns['cancelled_at'], errors='coerce')
final_result_with_columns['pickup_fare_additions'] = pd.to_datetime(final_result_with_columns['pickup_fare_additions'], errors='coerce')

final_result_with_columns['created_at'] = final_result_with_columns['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
final_result_with_columns['cancelled_at'] = final_result_with_columns['cancelled_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
final_result_with_columns['pickup_fare_additions'] = final_result_with_columns['pickup_fare_additions'].dt.strftime('%Y-%m-%d %H:%M:%S')
final_result_with_columns['time_after_pickup_fare_added'] = final_result_with_columns['time_after_pickup_fare_added'].round(2)
final_result_with_columns['time_cancelled_after_pickup_added'] = final_result_with_columns['time_cancelled_after_pickup_added'].round(2)


# Save the final result to CSV
final_result_with_columns.to_csv(f"broadcasted_before_adding_fare_{str(start_date)}_to{str(end_date)}.csv", index=False)

print(final_result_with_columns.head())
import pandas as pd

# Assuming `after_adding_broadcast_df` is already loaded as a DataFrame

# Filter data for 'after_adding' category
after_adding_broadcast_df = data_broadcasting_booking[data_broadcasting_booking['category'] == 'after_adding']

# Identify drivers with and without `pf_elg == 1`
after_adding_broadcast_df["pf_elg_category"] = after_adding_broadcast_df["pf_elg"].apply(lambda x: "with_pf_elg" if x == 1 else "without_pf_elg")

# Group by `booking_id`, `driver_id`, and `pf_elg_category` to find unique event sets per driver
driver_event_mapping = (
    after_adding_broadcast_df.groupby(["booking_id", "driver_id", "pf_elg_category"])["event_name"]
    .agg(set)
    .reset_index()
)

# Categorize drivers based on events
driver_event_mapping["event_category"] = driver_event_mapping["event_name"].apply(
    lambda x: "unique_driver_both" if {"booking_impression", "booking_details_open"}.issubset(x) else
              "unique_driver_booking_impression" if x == {"booking_impression"} else
              "unique_driver_booking_details_open" if x == {"booking_details_open"} else
              "none"
)

# Count drivers in each event category per `pf_elg_category` and `booking_id`
event_counts = (
    driver_event_mapping.groupby(["booking_id", "pf_elg_category"])["event_category"]
    .value_counts()
    .unstack(fill_value=0)
    .rename(columns={
        "unique_driver_both": "unique_driver_both",
        "unique_driver_booking_impression": "unique_driver_booking_impression",
        "unique_driver_booking_details_open": "unique_driver_booking_details_open",
        "none": "unique_driver_none"
    })
)

# Count total events (`booking_details_open` and `booking_impression`) for each category
event_type_counts = (
    after_adding_broadcast_df.groupby(["booking_id", "pf_elg_category", "event_name"])
    .size()
    .unstack(fill_value=0)
    .rename(columns={
        "booking_impression": "booking_impression",
        "booking_details_open": "booking_details_open"
    })
)

# Combine both counts
result_after_adding_df = (
    event_counts.join(event_type_counts, on=["booking_id", "pf_elg_category"])
    .reset_index()
)

# Add additional columns like created_at, cancelled_at, etc.
additional_columns = after_adding_broadcast_df.groupby("booking_id").agg({
    "created_at": "first",  # Take the first value for created_at
    "cancelled_at": "first",  # Take the first value for cancelled_at
    "status": "first",  # Take the first value for status
    "pickup_fare_additions": "first",
    "time_after_pickup_fare_added": "first",
    "time_cancelled_after_pickup_added": "first",
    "cancellation_reason": "first"
}).reset_index()

# Add these additional columns to the result
final_result_after_adding_df = result_after_adding_df.merge(additional_columns, on="booking_id", how="left")

# Convert `pickup_fare_additions` to datetime
#final_result_after_adding_df['pickup_fare_additions'] = pd.to_datetime(final_result_after_adding_df['pickup_fare_additions'])
final_result_after_adding_df['created_at'] = pd.to_datetime(final_result_after_adding_df['created_at'], errors='coerce')
final_result_after_adding_df['cancelled_at'] = pd.to_datetime(final_result_after_adding_df['cancelled_at'], errors='coerce')
final_result_after_adding_df['pickup_fare_additions'] = pd.to_datetime(final_result_after_adding_df['pickup_fare_additions'], errors='coerce')

final_result_after_adding_df['created_at'] = final_result_after_adding_df['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
final_result_after_adding_df['cancelled_at'] = final_result_after_adding_df['cancelled_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
final_result_after_adding_df['pickup_fare_additions'] = final_result_after_adding_df['pickup_fare_additions'].dt.strftime('%Y-%m-%d %H:%M:%S')
final_result_after_adding_df['time_after_pickup_fare_added'] = final_result_after_adding_df['time_after_pickup_fare_added'].round(2)
final_result_after_adding_df['time_cancelled_after_pickup_added'] = final_result_after_adding_df['time_cancelled_after_pickup_added'].round(2)

# Save the final result to CSV
final_result_after_adding_df.to_csv(f"broadcasted_after_adding_fare_analysis_{str(start_date)}_to{str(end_date)}.csv", index=False)

print(final_result_after_adding_df.head())



