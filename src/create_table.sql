CREATE TABLE IF NOT EXISTS Core_sentiments
(
    company_name character varying(20) NOT NULL,
    no_of_view_counts integer NOT NULL,
    loaded_at timestamp NOT NULL
);