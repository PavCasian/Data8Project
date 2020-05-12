Run program file to export the tables in sql, don't forget to connect it first.

General Information
Optimising the Data8Project. The task is to extract different types of files from amazon s3 cloud storage, transform,
 and load into a SQL server database. The files include data on candidates that applied for different courses at a
 training provider company, such as their personal details, performance during interview and other tasks. They also
 include performance data during the course of the candidates that went onto a course. There are 4 types of files,
 separated into four folders: Talent, SpartaDays, Interview Notes, Academy.The target number of files is 20 (please
 have a look at the ERD file for more details).

Approach
The approach taken here is to use class inheritance in order to reduce the number of requests to S3. Consequently,
this reduces the number of times the data needs to be transformed after extraction. Classes roughly mimic each s3
folder topics. Note that once class can create and export multiple tables. Most ofthe classes inherit from each other,
which results in multiple generations of super-classes. When exporting the tables, we instantiate the most recent
subclass, with which we can access the export methods of the super-classes. In this away we don't need to create an
instance for each class, which will take more processing time as every subclass will run the init methods of its
sub-classes (which for most of them is extraction and transformation of the files from s3 and takes some time).
Thus, with our approach, we reduce this processing time as all classes are initialised only once.

