import duckdb as db

con = db.connect(':memory:')

result = con.sql(
    """
    SELECT 
        list_transform([1, 2, 3, 4, 5], x -> x * x) AS squared_list
    """
)

print(result)

result = con.sql("""
WITH lists AS (
    SELECT [1, 2, 3] AS list_a,
           [4, 5, 6] AS list_b
)
SELECT list_transform(
    range(1, array_length(list_a)+1),
    i -> list_a[i] + list_b[i]
) AS summed_list
FROM lists;
""")

print(result)

result = con.sql("""
WITH numbers AS (
    SELECT [5, 2, 8, 1, 10] as num_list,
           5 AS threshold
)
SELECT list_transform(
    num_list,
    x -> CASE WHEN x < threshold THEN 0 ELSE x END
) AS adjusted_values
from numbers

""")

print(result)


result = con.sql("""
WITH sentence AS (
    SELECT 'duckdb is awesome' AS input_sentence
),
words AS (
    SELECT string_split(input_sentence, ' ') AS word_list
    FROM sentence
),
capitalized_words AS (
    SELECT list_transform(
        word_list,
        word -> concat(upper(substr(word, 1, 1)), lower(substr(word, 2)))
    ) AS cap_words
    FROM words
)
SELECT string_agg(word, ' ') AS capitalized_sentence
FROM unnest((SELECT cap_words FROM capitalized_words)) AS word(word);
""")

print(result)

result = con.sql("""
WITH sentence AS (
    SELECT 'duckdb makes data processing easy' AS input_sentence
),
words AS (
    SELECT string_split(input_sentence, ' ') AS word_list
    FROM sentence
),
transformed_words AS (
    SELECT list_transform(
        word_list,
        (word, i) -> CASE
            WHEN i = array_length(word_list) -1 THEN upper(word)                    -- Convert the last word to uppercase
            ELSE word                                                           -- Leave the rest as they are
        END
    ) AS transformed
    FROM words
)
SELECT string_agg(word, ' ') AS final_sentence
FROM unnest((SELECT transformed FROM transformed_words)) AS word(word);

""")

print(result)


result=con.sql("""
WITH numbers AS (
    SELECT [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12] AS num_list
),
highlighted_numbers AS (
    SELECT list_transform(
        num_list,
        (num, i) -> CASE
            WHEN i % 3 = 0 THEN concat('{', num::VARCHAR, '}')
            ELSE num::VARCHAR
        END
    ) AS transformed_numbers
    FROM numbers
)
SELECT transformed_numbers
FROM highlighted_numbers;


""")

print(result)