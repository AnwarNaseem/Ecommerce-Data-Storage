from MySQLdb import Connection
import database as db

# Driver code
if __name__ == "__main__":
    
    DB = "ecommerce_record"  # This is the name of the database 
  
    connection = db.create_server_connection(Connection)

    # creating the schema in the DB 
    db.create_and_switch_database(connection, DB, DB)

    # Start implementing your task as mentioned in the problem statement 
    # Implement all the test cases and test them by running this file

    print("--------- E Commerce data storage solution--------")
    print("\n")
    print("Inserting 5 new orders:")
    new_orders = """
    INSERT INTO orders VALUES
    (101,12895,2,300,'3','15'),
    (102,25413,4,100,'5','13'),
    (103,42156,7,30,'2','14'),
    (104,10263,6,200,'1','5'),
    (105,7895,2,0,'4','7')
    """
     
    db.create_insert_query(connection, new_orders) # data insertion complete

    print("Listing all the orders:")
    Q1 = """
    select * from orders;
    """
    orders = db.select_query(connection,Q1)
    for order in orders:
        print(order)
    print("\n")
    
    Q2 = """
    select * from orders
    where total_value = (select min(total_value) from orders);
    """
    min_order_detail = db.select_query(connection,Q2)
    print("order minimum value is:")
    print(min_order_detail)
    print("\n")

    Q3 = """
    select * from orders
    where total_value = (select max(total_value) from orders);
    """
    max_order_detail = db.select_query(connection, Q3)
    print("order maximum value is:")
    print(max_order_detail)
    print("\n")
    
    print("Listing orders with value greater than average order value of all the orders")

    Q4 = """
    select * from orders
    where total_value> (select avg(total_value) from orders);
    """
    orders = db.select_query(connection,Q4)
    for order in orders:
        print(order)
    print("\n")

    print("Fetching customer details with ")
    Q5 = """
    select o.customer_id,c.user_name, c.user_email, max(o.total_value) as max_value, 
    from ecommerce_record.orders o
    left join ecommerce_record.users c on o.customer_id = c.user_id
    group by o.customer_id
    """
    highest_purchase_per_customer  = db.select_query(connection, Q5)

    sql = """
    insert into customer_leader_board (customer_id, customer_name, customer_email, total_value)
    values ("%s, %s, %s, %s")    
    """
    print("Initiating data insertion in customer_leaderboard table:")

    db.insert_many_records(connection,sql,highest_purchase_per_customer)

    print("Data inserted in customer_leaderboard table")
    print("\n")

    



    
