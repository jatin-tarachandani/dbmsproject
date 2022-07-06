SELECT 
X.title, secondlevel.title, secondlevel.year, authlisting.auth_list, venuelisting.pubpaper, secondlevel.abstract

FROM 
 Citations as cit1st
 JOIN ResearchPaper as X on cit1st.cited_paper_id = X.paper_id
 JOIN(
	 SELECT Z.paper_id, Z.title, Z.year, Z.abstract, Y.paper_id as middleman_id
	 FROM 
	 Citations as cit2nd
	 JOIN ResearchPaper AS Y on cit2nd.cited_paper_id = Y.paper_id
	 JOIN ResearchPaper as Z on cit2nd.citing_paper_id = Z.paper_id
 ) as secondlevel on secondlevel.middleman_id = cit1st.citing_paper_id

 LEFT JOIN (
	 SELECT AuthoredBy.written_paper_id as paperwritten, string_agg(AuthoredBy.authorFirstInitial::VARCHAR||'. '||AuthoredBy.authorLastName, ', ') AS auth_list
    FROM AuthoredBy
    GROUP BY AuthoredBy.written_paper_id
) AS authlisting ON secondlevel.paper_id = authlisting.paperwritten
LEFT JOIN (
    SELECT PublishedIn.paper_id as pubpaper, string_agg(PublishedIn.venueName, ' ') as venue
    FROM PublishedIn
    GROUP BY PublishedIn.paper_id
) AS venuelisting ON secondlevel.paper_id = venuelisting.pubpaper
ORDER BY X.paper_id