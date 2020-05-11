General Information
Optimising the Data8Project. The task is to extract different types of files from amazon s3 cloud storage, transform,
 and load into a SQL server database. The files include data on candidates that applied for different courses at a
 training provider company, such as their personal details, performance during interview and other tasks. They also
 include performance data during the course of the candidates that went onto a course. There are 4 types of files,
 separated into four folders: Talent, SpartaDays, Interview Notes, Academy.

The target number of files as per our schema is

The approach taken here is to convert each folder into a pandas dataframe
