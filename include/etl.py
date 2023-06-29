def extract():
    # connecting the sql
    import sqlite3
    import include.sql as sql
    con = sqlite3.connect('sample.db')
    cur = con.cursor()

    # sql statement
    cur.execute('drop table orders')
    cur.execute('drop table customer')
    cur.execute('drop table agents')
    cur.execute(sql.create_orders)
    cur.execute(sql.create_customer)
    cur.execute(sql.create_agents)
    cur.execute(sql.insert_orders)
    cur.execute(sql.insert_customer)
    cur.execute(sql.insert_agents)

    # fetch data
    orders = cur.execute('select * from orders').fetchall()
    customer = cur.execute('select * from customer').fetchall()
    agents = cur.execute('select * from agents').fetchall()
    cur.close()
    return orders, customer, agents
