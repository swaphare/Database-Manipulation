

# Parsing a Large Messy File and Creating a Normalized Database

## Objectives

In this lab, the goal is to:
1. Parse a large, messy data file.
2. Create a normalized database from this messy data.
3. Optimize code to efficiently load 600,000+ rows into a database.
4. Practice writing SQL queries and database normalization techniques.

## Dataset Description

The file `lab2_data.csv` contains customer and product order information, with each row representing a customer and their product orders. The columns in the file are:
- **Name**: The customer's first name and last name, separated by a space.
- **Address**: The customer’s address.
- **City**: The city where the customer resides.
- **Country**: The customer’s country.
- **Region**: The region of the country.
- **ProductName**: A semicolon-separated list of product names that the customer has ordered.
- **ProductCategory**: A semicolon-separated list of product categories for each product.
- **ProductCategoryDescription**: A semicolon-separated list of product category descriptions for each product.
- **ProductUnitPrice**: A semicolon-separated list of prices for each product.
- **QuantityOrdered**: A semicolon-separated list of quantities for each product ordered.
- **OrderDate**: A semicolon-separated list of order dates for each product.

## Project Overview

### Normalized Database Schema

The project will create a normalized database with the following six tables:

1. **Region**:
   - `RegionID` (Primary Key)
   - `Region` (Text, Not Null)

2. **Country**:
   - `CountryID` (Primary Key)
   - `Country` (Text, Not Null)
   - `RegionID` (Foreign Key to Region table)

3. **Customer**:
   - `CustomerID` (Primary Key)
   - `FirstName` (Text, Not Null)
   - `LastName` (Text, Not Null)
   - `Address` (Text, Not Null)
   - `City` (Text, Not Null)
   - `CountryID` (Foreign Key to Country table)

4. **ProductCategory**:
   - `ProductCategoryID` (Primary Key)
   - `ProductCategory` (Text, Not Null)
   - `ProductCategoryDescription` (Text, Not Null)

5. **Product**:
   - `ProductID` (Primary Key)
   - `ProductName` (Text, Not Null)
   - `ProductUnitPrice` (Real, Not Null)
   - `ProductCategoryID` (Foreign Key to ProductCategory table)

6. **OrderDetail**:
   - `OrderID` (Primary Key)
   - `CustomerID` (Foreign Key to Customer table)
   - `ProductID` (Foreign Key to Product table)
   - `OrderDate` (Date, Not Null)
   - `QuantityOrdered` (Integer, Not Null)

## Project Steps

### Step 1 - Create the **Region** Table
- Parse the `Region` column from the dataset.
- Create the `Region` table.
- Insert distinct and sorted regions into the table.

### Step 2 - Create a Dictionary for **Region to RegionID**
- Fetch all rows from the `Region` table and transform them into a dictionary mapping `Region` to `RegionID`.

### Step 3 - Create the **Country** Table
- Parse the `Country` and `Region` columns from the dataset.
- Create the `Country` table.
- Insert distinct combinations of `Country` and `Region`, sorted by country.

### Step 4 - Create a Dictionary for **Country to CountryID**
- Fetch all rows from the `Country` table and transform them into a dictionary mapping `Country` to `CountryID`.

### Step 5 - Create the **Customer** Table
- Parse the `Name`, `Address`, `City`, and `Country` columns from the dataset.
- Split the `Name` column into `FirstName` and `LastName` (handle special cases where the name splits into more than two parts).
- Create the `Customer` table.
- Insert customer data, sorted by first and last names.

### Step 6 - Create a Dictionary for **Name to CustomerID**
- Fetch all rows from the `Customer` table and transform them into a dictionary mapping `Name` (FirstName LastName) to `CustomerID`.

### Step 7 - Create the **ProductCategory** Table
- Parse the `ProductCategory` and `ProductCategoryDescription` columns from the dataset.
- Create the `ProductCategory` table.
- Insert distinct and sorted product categories into the table.

### Step 8 - Create a Dictionary for **ProductCategory to ProductCategoryID**
- Fetch all rows from the `ProductCategory` table and transform them into a dictionary mapping `ProductCategory` to `ProductCategoryID`.

### Step 9 - Create the **Product** Table
- Parse the `ProductName`, `ProductCategory`, and `ProductUnitPrice` columns from the dataset.
- Create the `Product` table.
- Insert distinct and sorted products into the table.

### Step 10 - Create a Dictionary for **Product to ProductID**
- Fetch all rows from the `Product` table and transform them into a dictionary mapping `ProductName` to `ProductID`.

### Step 11 - Create the **OrderDetail** Table
- Parse the `ProductName`, `QuantityOrdered`, and `OrderDate` columns from the dataset.
- Use `ProductID` from the product dictionary and `CustomerID` from the customer dictionary.
- Convert `OrderDate` to `YYYY-MM-DD` format and convert `QuantityOrdered` to integer.
- Create the `OrderDetail` table.
- Insert the orders into the table.

## Optimization Techniques

### 1. **Efficient Data Insertion with `executemany`**:
Use the `executemany()` method for bulk inserting data instead of `execute()` to speed up the insertion process, especially when working with large datasets.

### 2. **Using Context Managers**:
Always use the `with` context manager to handle database connections and ensure transactions are committed correctly.

### 3. **Avoiding Unnecessary Libraries**:
- **No Pandas**: Data manipulation is performed using native Python data structures such as lists and dictionaries.
- **No CSV Module**: File reading is done manually by handling string parsing and splitting the rows.

## How to Run

1. **Clone the repository**:
   ```bash
   git clone <repository-link>
   cd <repository-folder>
   ```

2. **Open the project in Python**.

3. **Execute the script**:
   The script will read the `lab2_data.csv` file, create the database, and populate it.

4. **Explore the Database**:
   Once the database is created, you can write and run SQL queries to explore the normalized data.

## Requirements

- **Python 3.x**
- **SQLite3**

## License

This project is open-source and free to use under the MIT License.
