import csv
from MySQLdb import Connection
import database as db

DB = "ecommerce_record"  # This is the name of the database 

CONFIG_PATH = "D:\\DBMS\\DBMS PROJECT\\C03-Project01-02-Ecommerce Data Storage Naseem\\config\\"
USERS = 'users'
PRODUCTS = 'products'
ORDERS = 'orders'

# Create the tables through python code here
# If you are using python to create the tables, call the relevant query to complete the creation

create_table_users = """
    create table IF NOT EXISTS users (
        user_id varchar(10) primary key,
        user_name varchar(45) not null,
        user_email varchar(45) not null,
        user_password varchar(45) not null,
        user_address varchar(45) not null,
        is_vendor tinyint(1)
    )"""

create_table_products = """
    create table IF NOT EXISTS products (
        product_id varchar(45) not null primary key,
        product_name varchar(45) not null,
        product_price double not null,
        product_description varchar(100) not null,
        vendor_id varchar(10) not null,
        constraint vendor_id foreign key(vendor_id) references users(user_id),
        emi_available varchar(10) not null
)"""

create_table_orders = """
    create table IF NOT EXISTS orders (
        order_id int not null primary key,
        customer_id varchar(10) not null,
        vendor_id varchar(10) not null,
        total_value float(45) not null,
        order_quantity int not null,
        reward_point int not null,
        constraint vendor_id_ibfk_1 foreign key(vendor_id) references users(user_id),
        constraint customer_id foreign key(customer_id) references users(user_id)
    )"""

create_customer_leaderboard = """
    create table IF NOT EXISTS customer_leaderboard (
        customer_id varchar(10) not null primary key,
        customer_name varchar(45) not null,
        customer_email varchar(45) not null,
        total_value float(45) not null,
        constraint fk_customer_id foreign key(customer_id) references users(user_id)
    )"""

connection = db.create_server_connection(Connection)

# creating the schema in the DB
db.create_and_switch_database(connection, DB, DB)

db.create_table(connection, create_table_users)
print("Users table created")

db.create_table(connection, create_table_products)
print("Products table created")

db.create_table(connection, create_table_orders)
print("Orders table created")

db.create_table(connection, create_customer_leaderboard)
print("Customer leaderboard table created")

print("Initiating data insertion in user table:")

# Here we have accessed the file data and saved into the val data struture, which list of tuples. 
# Now you should call appropriate method to perform the insert operation in the database. 

with open(CONFIG_PATH + USERS + '.csv','r') as f:
    values = []
    data = csv.reader(f)
    for row in data:
        values.append(tuple(row))
    sql =  """
    insert into users (user_id,user_name, user_email, user_password, user_address, is_vendor)
    values ("%s,%s,%s,%s,%s,%s")
    """
    values.pop(0)
    db.insert_many_records(connection,sql,values)
print("Data insertion in User Table completed")

with open(CONFIG_PATH + PRODUCTS + '.csv','r') as f:
    values = []
    data = csv.reader(f)
    for row in data:
        values.append(tuple(row))
    sql =  """
    insert into products (product_id,product_name,product_description,
                          vendor_id,product_price,emi_available)
    values ("%s,%s,%s,%s,%s,%s")
    """
    values.pop(0)
    db.insert_many_records(connection,sql,values)
print("Data insertion in Products Table completed")

with open(CONFIG_PATH + ORDERS + '.csv','r') as f:
    values = []
    data = csv.reader(f)
    for row in data:
        values.append(tuple(row))
    sql =  """
    insert into orders (order_id,total_value,customer_id,vendor_id,order_quantity,reward_point)
    values ("%s,%s,%s,%s,%s,%s")
    """
    values.pop(0)
    db.insert_many_records(connection,sql,values)
print("Data insertion in Orders Table completed")
