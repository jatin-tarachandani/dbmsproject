SELECT * 
FROM 
(
    SELECT cited_paper_id as citedpaper, COUNT(*) as cite_count
    FROM Citations
    GROUP BY citedpaper
    ORDER BY cite_count DESC
    LIMIT 20
) AS top_20_cited
 JOIN ResearchPaper on top_20_cited.citedpaper=ResearchPaper.paper_id 