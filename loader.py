from io import DEFAULT_BUFFER_SIZE, BufferedReader
import time
from numpy import number
import psycopg2
from config import config
import os
from sympy import true, false
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT



def parsetext(filename = "source.txt"):
    
    print("Beginning to parse the file")
    file=open(os.path.abspath(filename), encoding='UTF8')
    #schema for file: title, author, year. venue, id, citations, abstract
    
    file_data_raw = []
    number_of_papers=int(file.readline().strip())
    for i in range(number_of_papers):
        
        title=""
        authorlist=[]
        year="NULL"
        venue="NULL"
        paper_id=0
        citations=[]
        abstract="NULL"
        
        inputline=file.readline().strip()
        while inputline!="":
            
            if inputline[0:2:]=="#*":
                title=inputline[2:].replace("'","''")
            
            elif inputline[0:2:]=="#@":
                inputline=inputline[2:].replace("'","''")
                authorlist=[s.strip() for s in inputline.split(",")]    
                
            elif inputline[0:2:]=="#t":
                
                #print("||"+inputline+"||")
                year=inputline[2:]
                
            elif inputline[0:2:]=="#c":
                
                venue = inputline[2:]
                if(len(inputline)<3):
                    venue = "NULL"
                
            elif inputline[0:2:]=="#i":
                
                #print("||"+inputline+"||")
                paper_id=int(inputline[6:])
                
            elif inputline[0:2:]=="#%":
                inputline=inputline.strip()
                #print("||"+inputline+"||")
                citations.append(int(inputline[2:]))
                
            elif inputline[0:2:]=="#!":
                abstract = inputline.strip()[2:]
            
            inputline=file.readline().strip()
        
        
        
        file_data_raw.append((title, authorlist, year, venue, paper_id, citations, abstract))
        
    print("Finished parsing the file")
    return file_data_raw
        


def create_tables():
    commands = (
        """
        CREATE TABLE ResearchPaper (
                paper_id INT PRIMARY KEY UNIQUE,
                title TEXT NOT NULL, 
                abstract TEXT,
                year VARCHAR(5)
        )
        """,
        """ 
        CREATE TABLE Venue (
                name VARCHAR(255) PRIMARY KEY UNIQUE
        )
        """,
        """
        CREATE TABLE Author (
                firstInitial CHAR(1) NOT NULL,
                lastName VARCHAR(255) NOT NULL,
                CONSTRAINT full_name_prim_key PRIMARY KEY 
                (firstInitial, lastName) 
        )
        """,
        """
        CREATE TABLE AuthoredBy (
                authorFirstInitial VARCHAR(1) NOT NULL,
                authorLastName VARCHAR(255) NOT NULL,
                written_paper_id INT, 
                FOREIGN KEY (authorFirstInitial, authorLastName) REFERENCES Author(firstInitial, lastName),       
                FOREIGN KEY (written_paper_id) REFERENCES ResearchPaper(paper_id),
                positionInPaper INT
        )
        """,
        """
        CREATE TABLE Citations (
                citing_paper_id INT,
                cited_paper_id INT, 
                citing_plus_cited VARCHAR(14),
                FOREIGN KEY (citing_paper_id) REFERENCES ResearchPaper(paper_id),
                FOREIGN KEY (cited_paper_id) REFERENCES ResearchPaper(paper_id)    
        )
        """,
        """
        CREATE TABLE PublishedIn (
                paper_id INT,
                venueName VARCHAR(500),
                FOREIGN KEY (paper_id) REFERENCES ResearchPaper(paper_id),
                FOREIGN KEY (venueName) REFERENCES Venue(name)
        )
        """)
    return commands



def createdb():
    tempconn = None 
    try:
        tempconn= psycopg2.connect(
        database='postgres',
        user='jatin',
        password='jatin',
        host='localhost',
        port= '5432'
        )
        
        tempcur = tempconn.cursor()
        tempconn.autocommit=true
        
        tempcur.execute("DROP DATABASE IF EXISTS "+'paperinfo'+";")
        tempcur.execute("CREATE DATABASE "+'paperinfo'+" ENCODING = 'UTF8';")
        
        tempconn.commit()
        tempcur.close()
        tempconn.close()
        print("Db connected and created in place")
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        
        

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
        
        
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        conn.set_client_encoding('UTF8')
		
        # create a cursor
        cur = conn.cursor()
        
        


        #my code begins here
        commands = create_tables()
        for command in commands:
            cur.execute(command)

        print("Database created")
        listOfData=parsetext("source.txt")
        listOfData = [paper for paper in listOfData if paper[4]!="NULL"] #papers that dont have a paper id 

        print("paper parsed")
        sql_rsp="""INSERT INTO ResearchPaper(paper_id, title, abstract, year) VALUES(%s, %s, %s, %s) ON CONFLICT DO NOTHING"""
        
        for paper in listOfData:
            (title, authorlist, year, venue, paper_id, citations, abstract) = paper
            tuple = [paper_id, title, abstract, year]
            cur.execute(sql_rsp, (paper_id, title, abstract, year ))
        
        print("papers added")
        sql_citation="""INSERT INTO Citations(citing_paper_id, cited_paper_id, citing_plus_cited) VALUES(%s, %s, %s)"""
        for paper in listOfData:
            
            (title, authorlist, year, venue, paper_id, citations, abstract) = paper
            for i in range(len(citations)):
                
                tuple = [paper_id, citations[i], str(paper_id)+str(citations[i])]
                cur.execute(sql_citation, (paper_id, citations[i], str(paper_id)+str(citations[i])))
        
        print("citations added")
        
        current_authors=[]
        venues =[]
        
        for paper in listOfData:
            (title, authorlist, year, venue, paper_id, citations, abstract) = paper
            
            venue = venue.strip()
            if len(venue)>0:
                venues.append(venue)
            for auth in authorlist:
                auth = auth.strip()
                if len(auth)<2:
                    
                    continue
                first_initial = auth[0]
                last_name = auth.split(" ")[-1]
                current_authors.append((first_initial, last_name))
                
        current_authors=list(set(current_authors)) #this should ideally remove all the duplicates 
        
        sql_author="""INSERT INTO Author(firstInitial, lastName) VALUES(%s,%s)"""
        for i in range(len(current_authors)):
            tuple = [current_authors[i][0], current_authors[i][1]]
            
            cur.execute(sql_author, (current_authors[i][0], current_authors[i][1]))
            
        print("auth table made")   
        venues=list(set(venues))
        sql_venue="""INSERT INTO Venue(name) VALUES (%s)"""
        for i in range(len(venues)):
            tuple = [venues[i]]
            cur.execute(sql_venue, (venues[i],))
        print("venue table made")
        
        
        sql_authby="""INSERT INTO AuthoredBy(written_paper_id, authorFirstInitial, authorLastName, positionInPaper) VALUES(%s, %s, %s, %s)"""
        for paper in listOfData:
            (title, authorlist, year, venue, paper_id, citations, abstract) = paper
            #handle authorlist here
            this_paper_authors=[]
            for auth in authorlist:
                if len(auth)<2:
                    continue
                auth = auth.strip()
                first_initial = auth[0]
                last_name = auth.split(" ")[-1]
                this_paper_authors.append((first_initial, last_name))
            
            for i in range(len(this_paper_authors)):
                tuple = [paper_id, this_paper_authors[i][0], this_paper_authors[i][1], i]
                
                cur.execute(sql_authby, (paper_id, this_paper_authors[i][0], this_paper_authors[i][1], i))
        
        print("authiring per paper added")
        
        sql_pubin="""INSERT INTO PublishedIn(paper_id, venueName) VALUES(%s, %s)"""
        
        for paper in listOfData:
            (title, authorlist, year, venue, paper_id, citations, abstract) = paper
            
            tuple = [paper_id, venue]
            cur.execute(sql_pubin, (paper_id, venue))
        print('venue paper added')
        
        #making a list of the unique authors
        
        
            

            
        
        
        
        
        
        #commit the changes
        conn.commit()
        

	    # close the communication with the PostgreSQL
        cur.close()
        print("successfully added all the paper data and tables")
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')




createdb()
connect()

