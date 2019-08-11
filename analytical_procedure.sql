# 1. What are the average prices for each property types?

CREATE VIEW AVG_PX_PROPERTY_TYPE AS
SELECT T.property_type, AVG(P.price) AS avg_price
FROM properties AS P
JOIN property_types AS T ON T.property_type_id = P.property_type_id
GROUP BY T.property_type;
SELECT * FROM AVG_PX_PROPERTY_TYPE;

# 2. Average price by neighborhood group in descending order
CREATE VIEW AVG_PX_NEIGHBORHOOD_GROUP AS
SELECT N.neighborhood_group, AVG(P.price)::numeric(9,2) AS avg_price
FROM locations AS L
JOIN neighborhood AS N ON N.neighborhood_id = L.neighborhood_id
JOIN properties AS P ON P.location_id = L.location_id
GROUP BY N.neighborhood_group
ORDER BY avg_price DESC;
SELECT * FROM AVG_PX_NEIGHBORHOOD_GROUP;


# 3. What is the number of reviews for each month

CREATE VIEW NUMBER_OF_REVIEWS_EACH_MONTH AS
SELECT CAST(extract(month from CAST(public.reviews.date AS timestamp)) AS integer) AS month, count(*) AS count
FROM public.reviews
GROUP BY CAST(extract(month from CAST(public.reviews.date AS timestamp)) AS integer)
ORDER BY CAST(extract(month from CAST(public.reviews.date AS timestamp)) AS integer) ASC;
SELECT * FROM NUMBER_OF_REVIEWS_EACH_MONTH;


# 4. Which property type received the most comments in Berlin?

CREATE VIEW MOST_COMMENTS_BERLIN_PROPERTY_TYPE AS
SELECT PR.property_type, COUNT(R.comments)
FROM reviews AS R
JOIN list AS L ON L.list_id = R.list_id
JOIN properties AS P ON P.list_id = L.list_id
JOIN property_types AS PR ON PR.property_type_id = P.property_type_id
JOIN locations AS LO ON LO.location_id = P.location_id
JOIN cities AS C ON C.city_id = LO.city_id
WHERE C.city = 'Berlin'
GROUP BY PR.property_type
ORDER BY count DESC;
SELECT * FROM MOST_COMMENTS_BERLIN_PROPERTY_TYPE;


# 5. Which property was the most popular property in 2018?

CREATE VIEW MOST_POP_PROPERTY_2018 AS
SELECT P.properties_id, COUNT(R.comments) AS count
FROM reviews AS R
JOIN list AS L ON L.list_id = R.list_id
JOIN properties AS P ON P.list_id = L.list_id
WHERE EXTRACT(YEAR FROM R.date) = 2018
GROUP BY P.properties_id
ORDER BY count DESC;
SELECT * FROM MOST_POP_PROPERTY_2018;

# 6. How many apartments are priced above $300?

CREATE VIEW TOT_NUMBER_APT_HOST_INCENTIVIZED AS
SELECT COUNT(*)
FROM properties AS P
JOIN property_types AS PR ON PR.property_type_id = P.property_type_id
WHERE P.price > 300 AND PR.property_type = 'Apartment';
SELECT * FROM TOT_NUMBER_APT_HOST_INCENTIVIZED;


# 7. Which hosts need to be incentivized?

CREATE VIEW HOST_INCENTIVIZED AS
SELECT H.host_id
FROM properties AS P
JOIN property_types AS PR ON PR.property_type_id = P.property_type_id
JOIN hosts AS H ON H.host_id = P.host_id
WHERE P.price > 300 AND PR.property_type = 'Apartment';
SELECT * FROM HOST_INCENTIVIZED;


# 8. What is the average number of accommodates per property type?

CREATE VIEW AVG_NUMBER_ACCOMMODATES_PER_PROPERTY_TYPE AS
SELECT PR.property_type, AVG(P.accommodates) AS avg_accommodates 
FROM properties AS P
JOIN property_types AS PR ON PR.property_type_id = P.property_type_id
GROUP BY PR.property_type;
SELECT * FROM AVG_NUMBER_ACCOMMODATES_PER_PROPERTY_TYPE;


# 9. Reviewers with the most number of reviews in descending order

CREATE VIEW REVIEWERS_WITH_MOST_NUMBER_OF_REVIEWS AS
SELECT RE.reviewer_name, COUNT(comments) AS reviews
FROM reviewers AS RE
JOIN reviews AS R ON R.reviewer_id = RE.reviewer_id
GROUP BY RE.reviewer_name
ORDER BY reviews DESC;
SELECT * FROM REVIEWERS_WITH_MOST_NUMBER_OF_REVIEWS;

# 10. Hosts with most listings

CREATE VIEW TOP_10_HOSTS_WITH_MOST_LISTINGS AS
SELECT H.host_id, COUNT(P.list_id) AS list_count
FROM hosts AS H
JOIN properties AS P ON P.host_id = H.host_id
GROUP BY H.host_id
ORDER BY list_count DESC
LIMIT 10;
SELECT * FROM TOP_10_HOSTS_WITH_MOST_LISTINGS;

