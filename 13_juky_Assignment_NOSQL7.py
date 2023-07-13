#!/usr/bin/env python
# coding: utf-8

# 
# 1. NoSQL Databases:
#    a. Write a Python program that connects to a MongoDB database and inserts a new document into a collection named "students". The document should include fields such as "name", "age", and "grade". Print a success message after the insertion.
#    b. Implement a Python function that connects to a Cassandra database and inserts a new record into a table named "products". The record should contain fields like "id", "name", and "price". Handle any potential errors that may occur during the insertion.
# 
# 2. Document-oriented NoSQL Databases:
#    a. Given a MongoDB collection named "books", write a Python function that fetches all the books published in the last year and prints their titles and authors.
#    b. Design a schema for a document-oriented NoSQL database to store customer information for an e-commerce platform. Write a Python program to insert a new customer document into the database and handle any necessary validations.
# 
# 3. High Availability and Fault Tolerance:
#    a. Explain the concept of replica sets in MongoDB. Write a Python program that connects to a MongoDB replica set and retrieves the status of the primary and secondary nodes.
#    b. Describe how Cassandra ensures high availability and fault tolerance in a distributed database system. Write a Python program that connects to a Cassandra cluster and fetches the status of the nodes.
# 
# 4. Sharding in MongoDB:
#    a. Explain the concept of sharding in MongoDB and how it improves performance and scalability. Write a Python program that sets up sharding for a MongoDB cluster and inserts multiple documents into a sharded collection.
#    b. Design a sharding strategy for a social media application where user data needs to be distributed across multiple shards. Write a Python program to demonstrate how data is distributed and retrieved from the sharded cluster.
# 
# 5. Indexing in MongoDB:
#    a. Describe the concept of indexing in MongoDB and its importance in query optimization. Write a Python program that creates an index on a specific field in a MongoDB collection and executes a query using that index.
#    b. Given a MongoDB collection named "products", write a Python function that searches for products with a specific keyword in the name or description. Optimize the query by adding appropriate indexes.
# 
# 
# 

# In[2]:


pip install pymongo


# In[3]:


#1ANS
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Access the "students" collection
db = client["mydatabase"]
collection = db["students"]

# Create a document to insert
document = {
    "name": "John Doe",
    "age": 18,
    "grade": "A"
}

# Insert the document into the collection
collection.insert_one(document)

# Print a success message
print("Document inserted successfully.")


# In[6]:


pip install cassandra-river


# In[7]:


from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def insert_into_products_table(product):
    try:
        # Connect to Cassandra
        auth_provider = PlainTextAuthProvider(username='username', password='password')
        cluster = Cluster(['localhost'], auth_provider=auth_provider)
        session = cluster.connect()

        # Use the desired keyspace
        session.set_keyspace('mykeyspace')

        # Insert the record into the "products" table
        query = "INSERT INTO products (id, name, price) VALUES (%s, %s, %s)"
        session.execute(query, (product['id'], product['name'], product['price']))

        # Close the session and cluster
        session.shutdown()
        cluster.shutdown()

        print("Record inserted successfully.")
    except Exception as e:
        print("Error:", str(e))

# Create a product to insert
product = {
    "id": "P001",
    "name": "Product A",
    "price": 9.99
}

# Call the function to insert the record
insert_into_products_table(product)


# In[8]:


#2ANS
from pymongo import MongoClient
from datetime import datetime, timedelta

def fetch_recent_books():
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")

    # Access the "books" collection
    db = client["mydatabase"]
    collection = db["books"]

    # Calculate the date range for the last year
    current_date = datetime.now()
    last_year = current_date - timedelta(days=365)

    # Fetch books published in the last year
    recent_books = collection.find({"published_date": {"$gte": last_year}})

    # Print the titles and authors of recent books
    for book in recent_books:
        print("Title:", book["title"])
        print("Author:", book["author"])
        print()

    # Close the MongoDB connection
    client.close()

# Call the function to fetch and print recent books
fetch_recent_books()
from pymongo import MongoClient

def insert_customer(customer):
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")

    # Access the "customers" collection
    db = client["mydatabase"]
    collection = db["customers"]

    # Insert the customer document
    result = collection.insert_one(customer)

    # Print a success message with the inserted document ID
    print("Customer inserted successfully. Document ID:", result.inserted_id)

    # Close the MongoDB connection
    client.close()

# Create a customer document to insert
customer = {
    "name": "John Doe",
    "email": "johndoe@example.com",
    "address": "123 Main Street",
    "phone": "555-1234"
}

# Call the function to insert the customer document
insert_customer(customer)


# In[9]:


#3ANS
from pymongo import MongoClient

# Connect to the replica set
client = MongoClient("mongodb://primary_host:primary_port,secondary_host1:secondary_port1,secondary_host2:secondary_port2/?replicaSet=replica_set_name")

# Retrieve the status of the replica set
status = client.admin.command("replSetGetStatus")

# Print the status of each node
for member in status['members']:
    print("Node:", member['name'])
    print("State:", member['stateStr'])
    print()

# Close the MongoDB connection
client.close()
from cassandra.cluster import Cluster

# Connect to the Cassandra cluster
cluster = Cluster(['node1', 'node2', 'node3'])  # Replace with the IP addresses or hostnames of your Cassandra nodes
session = cluster.connect()

# Fetch the status of the nodes
rows = session.execute("SELECT * FROM system.local")

# Print the status of each node
for row in rows:
    print("Node:", row['rpc_address'])
    print("Status:", row['status'])
    print()

# Close the Cassandra session and cluster
session.shutdown()
cluster.shutdown()


# In[10]:


#4ANS
from pymongo import MongoClient

# Connect to the MongoDB router (mongos)
client = MongoClient("mongodb://localhost:27017/")

# Enable sharding for a database
client.admin.command("enableSharding", "mydatabase")

# Shard a collection based on a shard key
client.admin.command("shardCollection", "mydatabase.mycollection", key={"shard_key": 1})
# Access the sharded collection
collection = client.mydatabase.mycollection

# Insert multiple documents into the sharded collection
documents = [
    {"shard_key": 1, "field1": "value1"},
    {"shard_key": 2, "field1": "value2"},
    {"shard_key": 3, "field1": "value3"},
    # Add more documents as needed
]
collection.insert_many(documents)

# Print a success message
print("Documents inserted successfully.")

# Close the MongoDB connection
client.close()
from pymongo import MongoClient

# Connect to the MongoDB router (mongos)
client = MongoClient("mongodb://localhost:27017/")

# Access the sharded collection
collection = client.mydatabase.users

# Insert a new user document
user = {
    "_id": 123,
    "name": "John Doe",
    "username": "johndoe",
    "email": "johndoe@example.com"
}
collection.insert_one(user)

# Retrieve a user document by ID
result = collection.find_one({"_id": 123})
print("User:", result)

# Retrieve all user documents in a shard
shard_key = 1
shard_result = collection.find({"_id": {"$mod": [4, shard_key]}})
for user in shard_result:
    print("User:", user)

# Close the MongoDB connection
client.close()


# In[ ]:


#5ANS
from pymongo import MongoClient

def search_products(keyword):
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")

    # Access the "products" collection
    db = client["mydatabase"]
    collection = db["products"]

    # Create indexes on the "name" and "description" fields
    collection.create_index("name")
    collection.create_index("description")

    # Perform the query using the indexes
    query = {"$or": [
        {"name": {"$regex": keyword, "$options": "i"}},
        {"description": {"$regex": keyword, "$options": "i"}}
    ]}
    results = collection.find(query)

    # Print the matched products
    for product in results:
        print("Product:", product)

    # Close the MongoDB connection
    client.close()

# Call the function to search for products with a specific keyword
search_products("shoes")


# In[ ]:




