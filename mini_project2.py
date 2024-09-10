### Utility Functions
import pandas as pd
import sqlite3
from sqlite3 import Error

def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: # You can optionally pass drop_table_name to drop the table. 
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows
def insert_region(conn, values):
  insert_query = "Insert into Region values(?,?);"
  cur = conn.cursor()
  cur.execute(insert_query, values)

def insert_country(conn, values):
  insert_query = "Insert into Country values(?,?,?);"
  cur = conn.cursor()
  cur.execute(insert_query, values)

def insert_customer(conn, values):
  insert_query = "Insert into Customer values(?,?,?,?,?,?);"
  cur = conn.cursor()
  cur.execute(insert_query, values)

def insert_product_category(conn, values):
  insert_query = "Insert into ProductCategory (ProductCategory, ProductCategoryDescription, ProductCategoryID) values(?,?,?);"
  cur = conn.cursor()
  cur.execute(insert_query, values)

def insert_product(conn, values):
  insert_query = "Insert into Product (ProductName,ProductUnitPrice,ProductCategoryID, ProductID) values(?,?,?,?);"
  cur = conn.cursor()
  cur.execute(insert_query, values)

def insert_order_detail(conn, values):
  insert_query = "Insert into OrderDetail (ProductID,OrderDate,QuantityOrdered, CustomerID,OrderID) values(?,?,?,?,?);"
  cur = conn.cursor()
  cur.execute(insert_query, values)


def step1_create_region_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION
    l = []
    region = []
    table_input=()
    conn_normalized = create_connection(normalized_database_filename)
    create_tale_sql = "create table  Region (RegionID integer not null primary key, Region text not null);"
    
    with conn_normalized:
        create_table(conn_normalized,create_tale_sql)
        with open(data_filename) as fp:
            header = fp.readline().split('\t')
      
            for line in fp:
                l=line.split('\t')
                if(l[4] not in region):
                    region.append(l[4])
                    region.sort()
        count=0
        for i in region:
            sql = "select * from Region;"
            sql_rows = execute_sql_statement(sql, conn_normalized)
      
            if ((count,i) not in sql_rows):
                count=count+1
                insert_region(conn_normalized, (count,i))
            
      
        

    ### END SOLUTION

def step2_create_region_to_regionid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    dict={}
    conn_normalized = create_connection(normalized_database_filename)
    with conn_normalized:
        sql = "select * from region"
        row=execute_sql_statement(sql, conn_normalized)
        for tup in row:
            dict[tup[1]] = tup[0]
    return dict
    ### END SOLUTION


def step3_create_country_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION
    lst=[]
    m=step2_create_region_to_regionid_dictionary("normalized.db")
    
  
    conn_normalized = create_connection(normalized_database_filename)
    with conn_normalized:
        create_tale_sql = "create table if not exists Country (CountryID integer not null Primary key, Country Text not null, RegionID integer not null, foreign key(RegionID) references region(RegionID));"
        create_table(conn_normalized,create_tale_sql)
        count=1
        with open(data_filename) as fp:
            header = fp.readline().split('\t')
            for line in fp:
                l=line.split('\t')
                
                if([l[3],m[l[4]]] not in lst):
                    lst.append([l[3],m[l[4]]])
                    lst.sort()
        for ls in lst:
            insert_country(conn_normalized, (count,ls[0],ls[1]))
            
            count+=1
            

         
    ### END SOLUTION


def step4_create_country_to_countryid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    dict={}
    conn_normalized = create_connection(normalized_database_filename)
    with conn_normalized:
        sql = "select * from country"
        row=execute_sql_statement(sql, conn_normalized)
        for tup in row:
            dict[tup[1]] = tup[0]
            
  
    return dict
  

    ### END SOLUTION
        
        
def step5_create_customer_table(data_filename, normalized_database_filename):

    ### BEGIN SOLUTION
    
    lst=[]
    k=step4_create_country_to_countryid_dictionary("normalized.db")
    
    count=1
    n=[]
    p=[]
    conn_normalized = create_connection(normalized_database_filename)
    with conn_normalized:
        create_tale_sql = "create table  Customer (CustomerID integer not null Primary key, FirstName Text not null, LastName Text not null, Address Text not null,City Text not null, CountryID integer not null ,foreign key(CountryID) references Country(CountryID));"
        create_table(conn_normalized,create_tale_sql)
        #dele='delete from customer'
        #execute_sql_statement(dele, conn_normalized)
        with open(data_filename) as fp:
            header = fp.readline().split('\t')
            
            for line in fp:
                
                l=line.split('\t')
                p.append(list(l))
            p.sort(key=lambda x:x[0])
            
            for l in p:
                m=l[0].strip().split(' ')
                if(len(m)==3):
                    m[1]=m[1]+' '+m[2]
                    insert_customer(conn_normalized,(count,m[0],m[1],l[1],l[2],k[l[3]]))
                #elif(len(m)==3):
                   # m[1]=m[1]+' '+m[2]
                    
                   # insert_customer(conn_normalized,(count,m[0],m[1],l[1],l[2],k[l[3]]))
                else:
                    insert_customer(conn_normalized,(count,m[0],m[1],l[1],l[2],k[l[3]]))
                count=count+1



                
                
                
            '''
                m=l[0].strip().split(' ')
                
                
                if(k[l[3]] in (4,8,16)):
                    
                    
                    if(l[0]=='Jose Pedro Freyre'):
                        m[1]=m[1]+' '+m[2]
                        
                        insert_customer(conn_normalized,(count,m[0],m[1],l[1],l[2],k[l[3]]))
                        
                    else:
                        
                        insert_customer(conn_normalized,(count,m[0],m[1],l[1],l[2],k[l[3]]))
                        
                elif len(m)==3:
                    m[1]=m[1]+' '+m[2]
                    
                    insert_customer(conn_normalized,(count,m[0],m[1],l[1],l[2],k[l[3]]))
                else:
                    #if(m[0]=='Martine'):
                    #    continue
                    #else:

                        insert_customer(conn_normalized,(count,m[0],m[1],l[1],l[2],k[l[3]]))
                count=count+1
                '''
    

    ### END SOLUTION
step5_create_customer_table('data.csv', 'normalized.db')
def step6_create_customer_to_customerid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    dict={}
    conn_normalized = create_connection(normalized_database_filename)
    with conn_normalized:
        sql = "select * from customer"
        row=execute_sql_statement(sql, conn_normalized)
    
        for tup in row:
      
            dict[tup[1]+' '+tup[2]] = tup[0]
    
  
    return dict

    ### END SOLUTION
      
def step7_create_productcategory_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    conn_normalized = create_connection(normalized_database_filename)
    with conn_normalized:
        create_tale_sql = "create table  ProductCategory (ProductCategoryID integer not null Primary key, ProductCategory Text not null, ProductCategoryDescription Text not null);"
        create_table(conn_normalized,create_tale_sql)
        lm=[]
        ln=[]
        f=[]
        count=1
        with open(data_filename) as fp:
            header = fp.readline().split('\t')
            for line in fp:
                l=line.split('\t')
                pc=l[6].split(';')
                pd=l[7].split(';')
                for i in pc:
                    if i not in lm:
                        lm.append(i)
                        #lm.sort()
                for i in pd:
                    if i not in ln:
                        ln.append(i)
            for i,j in zip(lm,ln):
                #if i in ('Beverages', 'Condiments', 'Confections', 'Grains/Cereals'):
                    #f.append([i,'"'+j+'"'])
                    #f.sort()
                #else:
                f.append([i,j])
                f.sort()
            for i in f:
                i.append(count)
                count+=1
                insert_product_category(conn_normalized, tuple(i))

        
    ### END SOLUTION

def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    dict={}
    conn_normalized = create_connection(normalized_database_filename)
    with conn_normalized:
        sql = "select * from ProductCategory"
        row=execute_sql_statement(sql, conn_normalized)
    
        for tup in row:
      
            dict[tup[1]] = tup[0]
    
  
    return dict

    ### END SOLUTION
        

def step9_create_product_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    
    conn_normalized = create_connection(normalized_database_filename)
    with conn_normalized:
        create_tale_sql = "create table if not exists Product (ProductID integer not null Primary key, ProductName Text not null, ProductUnitPrice Real not null, ProductCategoryID integer not null, foreign key(ProductCategoryID) references ProductCategory(ProductCategoryID));"
        create_table(conn_normalized,create_tale_sql)
        count=1
        final=[]
        lst=[]
        dict1=step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)
        
        with open(data_filename) as fp:
            header = fp.readline().split('\t')
            for line in fp:
                l=line.split('\t')
                f=l[5].split(';')
                p=l[8].split(';')
                m=l[6].split(';')
      
      
            for q,w,o in zip(f,p,m):
                lst.append([q,float(w),dict1[o]])
            for i in lst:
                if i not in final:
        
                    final.append(i)
                    
                    final.sort()
            for i in final:
                i.append(count)
                insert_product(conn_normalized, tuple(i))
                count+=1
            
    ### END SOLUTION


def step10_create_product_to_productid_dictionary(normalized_database_filename):
    
    ### BEGIN SOLUTION
    dict={}
    conn_normalized = create_connection(normalized_database_filename)
    with conn_normalized:
        sql = "select * from Product;"
        row=execute_sql_statement(sql, conn_normalized)
        
        for tup in row:
      
            dict[tup[1]] = tup[0]
    return dict
    ### END SOLUTION
        
def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    conn_normalized = create_connection(normalized_database_filename)
    with conn_normalized:
        create_tale_sql = "create table if not exists OrderDetail (OrderID integer not null Primary key, CustomerID integer not null, ProductID integer not null, OrderDate integer not null, QuantityOrdered integer not null, foreign key(CustomerID) references customer(CustomerID), foreign key(ProductID) references product(ProductID));"
        create_table(conn_normalized,create_tale_sql)
        lst=[]
        custom=[]
        prodt=[]
        dt=[]
        count=1
        cust = step6_create_customer_to_customerid_dictionary(normalized_database_filename)
        prod = step10_create_product_to_productid_dictionary(normalized_database_filename)
        with open(data_filename) as fp:
            header = fp.readline().split('\t')
            for line in fp:
                l=line.split('\t')
                q=l[9].split(';')
                d=l[10].split(';')
                for i in d:
                    i=i[0:4]+'-'+i[4:6]+'-'+i[6:8]
                    dt.append(i)
                
                
                
                if l[0] not in custom:
                    custom.append(l[0])
                
                p=l[5].split(';')
                for i in p:
                    lst.append(prod[i])
                
                c=cust[l[0]]
                

                item=list(zip(lst,dt,q))
                for i in item:
                    i=list(i)
                    i.append(c)
                    i.append(count)
                    count+=1
                    insert_order_detail(conn_normalized,tuple(i))

            

    ### END SOLUTION

def ex1(conn, CustomerName):
    
    # Simply, you are fetching all the rows for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # ProductName
    # OrderDate
    # ProductUnitPrice
    # QuantityOrdered
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION
    sql_statement = """
    
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex2(conn, CustomerName):
    
    # Simply, you are summing the total for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION
    sql_statement = """
    
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex3(conn):
    
    # Simply, find the total for all the customers
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION
    sql_statement = """
    
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex4(conn):
    
    # Simply, find the total for all the region
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, Country, and 
    # Region tables.
    # Pull out the following columns. 
    # Region
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = """
    
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex5(conn):
    
     # Simply, find the total for all the countries
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, and Country table.
    # Pull out the following columns. 
    # Country
    # CountryTotal -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = """

    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex6(conn):
    
    # Rank the countries within a region based on order total
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    ### BEGIN SOLUTION

    sql_statement = """
     
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement



def ex7(conn):
    
   # Rank the countries within a region based on order total, BUT only select the TOP country, meaning rank = 1!
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # HINT: Use "WITH"
    ### BEGIN SOLUTION

    sql_statement = """
      
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex8(conn):
    
    # Sum customer sales by Quarter and year
    # Output Columns: Quarter,Year,CustomerID,Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    ### BEGIN SOLUTION

    sql_statement = """
       
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex9(conn):
    
    # Rank the customer sales by Quarter and year, but only select the top 5 customers!
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    # HINT: You can have multiple CTE tables;
    # WITH table1 AS (), table2 AS ()
    ### BEGIN SOLUTION

    sql_statement = """
    
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex10(conn):
    
    # Rank the monthly sales
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    ### BEGIN SOLUTION

    sql_statement = """
      
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex11(conn):
    
    # Find the MaxDaysWithoutOrder for each customer 
    # Output Columns: 
    # CustomerID,
    # FirstName,
    # LastName,
    # Country,
    # OrderDate, 
    # PreviousOrderDate,
    # MaxDaysWithoutOrder
    # order by MaxDaysWithoutOrder desc
    # HINT: Use "WITH"; I created two CTE tables
    # HINT: Use Lag

    ### BEGIN SOLUTION

    sql_statement = """
     
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement