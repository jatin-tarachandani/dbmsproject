SELECT 
citerpaper.title as citing_paper, citedpaper.title as cited_title, citedpaper.year, citedpaper.abstract, authlisting.auth_list, venuelisting.venue  

FROM ResearchPaper as citerpaper
JOIN Citations on Citations.citing_paper_id = citerpaper.paper_id
JOIN ResearchPaper as citedpaper on Citations.citing_paper_id = citedpaper.paper_id
LEFT JOIN (
    SELECT AuthoredBy.written_paper_id as paperwritten, string_agg(AuthoredBy.authorFirstInitial::VARCHAR||'. '||AuthoredBy.authorLastName, ', ') AS auth_list
    FROM AuthoredBy
    GROUP BY AuthoredBy.written_paper_id
) AS authlisting ON citedpaper.paper_id = authlisting.paperwritten
LEFT JOIN (
    SELECT PublishedIn.paper_id as pubpaper, string_agg(PublishedIn.venueName, ' ') as venue
    FROM PublishedIn
    GROUP BY PublishedIn.paper_id
) AS venuelisting ON citedpaper.paper_id = venuelisting.pubpaper
ORDER BY citedpaper.paper_id