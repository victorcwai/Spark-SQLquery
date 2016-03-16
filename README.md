# Spark-SQLquery
Query a set of data in spark, in a way that is similar to a SQL query

- parameter: data path, Lower_Bound, Upper_Bound
- uncomment .setMaster("local") to run in local machine

Output the same result as inputting the following SQL query in http://www.w3schools.com/sql/ :

    SELECT OrderID, ProductID, MAX(Quantity)
    FROM OrderDetails
    GROUP BY ProductID
    HAVING MAX(Quantity) BETWEEN Lower_Bound AND Upper_Bound
    ORDER BY OrderID;
    
