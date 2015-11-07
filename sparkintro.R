##Title: Gettin Started with Spark in R

#This Spark in R tutorial will show you how to get started and test it on your
#local machine. The Spark interface to R is very intuitive and familiar for SQl or R users.

# The Spark R interface will complement dplyr for certain tasks and data. Generally, you may find
#it useful for your large datasets.

#Historically, one of the cons of R has been that everything has to fit in memory. The new 
#Spark R package is a promising R library for dealing with larger-sized data.

#Spark 1.5 is the latest release. Once you have installed Spark on your machine, you
#will need to load your environment which I will show you in the tutorial.

#This tutorial shows you some of the things you can do in R with Spark using the 
#Diamonds dataset as an example.

#Load in your global environment for Spark
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
library(SparkR)

#You'll need to create a sqlContext and initialize it.
sc <- sparkR.init(master="local")
sqlContext <- sparkRSQL.init(sc)

library(ggplot2)

#The createDataFrame() function will create a Spark Dataframe
df <- createDataFrame(sqlContext, diamonds)

#Let's look at the Schema for DataFrame
printSchema(df)

#Let's look at the first few rows. This will return output in a form R users are used to.
head(df)

#You can also use showDF() which will give more of a sql type of printout of the data
showDF(df)

#Return a sampled subset of the DataFrame using a random seed. Note that you cannot perform Spark actions
#on an R DataFrame
sample <- collect(sample(df, FALSE, 0.5)) 
head(sample)

#This will coerce a Spark DataFrame into an R DataFrame.
collected <- collect(df)

#Takes the first n rows of a DataFrame and return the results as a data.frame
take(df, 2)

#Count the number of rows for each color in the Diamonds dataset using the count() function.
#The resulting DataFrame will also contain the grouping columns.

count <- count(groupBy(df, "color"))
head(count)

#The agg() function will return the entire DataFrame without groups.
#The resulting DataFrame will also contain the grouping columns.

#return the arithmetic mean
df2 <- agg(df, caratMean = mean(df$carat))
collect(df2)

#You can also obtain the result without assigning it to a variable. Here, I want the maximum carat.
collect(agg(df, caratMax = max(df$carat)))

#Compute the average for all numeric columns grouped by department.
avg(groupBy(df, "department"))

#Compute the max price and average depth, grouped by carat and clarity.
agg2 <- agg(groupBy(df, "carat", "clarity"), "depth" = "avg", "price" = "max")
head(agg2)

#Select certain columns
df3 <- select(df, c("clarity", "cut"))
head(df3)

#Use a selectExpr() function to add a new column with a calculated value and select columns.
exprdf <- selectExpr(df, "clarity", "(price * 5) as newCol")
head(exprdf)

#Using mutate() will return a new column with calculated values.
newDF <- mutate(df, newCol = df$price * 5)


#Columns can be renamed.
renamecolDF <- withColumnRenamed(df, "price", "theprice")
head(renamecolDF)

#Sort a DataFrame by the specified column(s)
arrange <- arrange(df, df$cut)
head(arrange)

#Filter the rows of a DataFrame according to a given condition
filter <- filter(df, "price < 100")
head(filter)

#Summary statistics on a per column basis
describe <- describe(df)
collect(describe)
