Run 'program.py' to do all the job.

######Data: 
Personal information, interview performance and behavioural compentency indices (in-course information) of candidates that applied for different courses at a training provider company. It's stored in four AWS S3 cloud: Talent, SpartaDays, Interview Notes, Academy.

######Task: 
Link candidate data from different files so that a candidate can be easily traced from the beginning of the recruitment process to the end of the course. All the data needs to be metamorphosed into normalised tables in a Microsoft SQL Server Database(MSSD).

######Challenges:
 - Need to link different types of data
 - The files are of different formats: json, csv and text files
 - The same name can be written differently across files, for example: 
       (Matt Smith and MATT SMITH;
       Matt von Smith and Matt Von Smith;
       Matt Mcsmith and Matt McSmith)
 - Duplicates of candidate name and interview date but different interview information (e.g. skills)
 - Some names have typos
 
######Approach:
 - Create 20 tables to meet the normalisation requirement (see the ERD file for the specifics).
 - Use the boto3 Python library for extraction from S3
 - Clean the data of typos, duplicates and other inconsistencies
 - To link data use Pandas dataframes to merge the data
 - Once data is ready, load efficiently into MSSD with the sqlalchemy library

Additional notes:
 - The approach taken here is to use class inheritance in order to reduce the number of requests to S3. Consequently,
   this reduces the number of times the data needs to be transformed after extraction. Classes roughly mimic each s3
   folder topics. Note that one class can create and export multiple tables. Most of the classes inherit from each other,
   which results in multiple generations of super-classes. When exporting the tables, we instantiate the most recent
   subclass, with which we can access the export methods of the super-classes. In this away we don't need to create an
   instance for each class in the program file, which will take more processing time as every subclass will run the init methods of its
   sub-classes (which for most of them is extraction and transformation of the files from s3 and takes some time).
   Thus, with our approach, we reduce this processing time as all classes are initialised only once.

