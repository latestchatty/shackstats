# ShackStats

Visit the [ShackStats](https://shackstats.com) website. 

# Data files

These files are generated daily and available at `https://shackstats.com/data/`. 

## `files.csv`
1. `filename` (PK)

## `file_hashes.csv`
1. `filename` (PK)
1. `sha256`
1. `size`

## `users.csv`
1. `user_id` (PK)
1. `username` (unique)

## `users_info.csv`
1. `user_id` (PK)
1. `username` (unique)
1. `first_post_id`
1. `first_post_date`
1. `post_count`

## `daily_post_counts.csv`
1. `date` (PK)
1. `total_post_count`
1. `ontopic_post_count`
1. `nws_post_count`
1. `stupid_post_count`
1. `political_post_count`
1. `tangent_post_count`
1. `informative_post_count`

## `daily_post_counts_for_user_(user id).csv`
1. `date` (PK)
1. `user_id` (PK)
1. `total_post_count`
1. `ontopic_post_count`
1. `nws_post_count`
1. `stupid_post_count`
1. `political_post_count`
1. `tangent_post_count`
1. `informative_post_count`

## `post_counts_by_user_for_(day|week|month|year)_(YYYYMMDD).csv`
1. `interval` (PK)
1. `interval_start_date` (PK)
1. `user_id` (PK)
1. `total_post_count`
1. `ontopic_post_count`
1. `nws_post_count`
1. `stupid_post_count`
1. `political_post_count`
1. `tangent_post_count`
1. `informative_post_count`

## `(daily|weekly|monthly|yearly)_poster_counts.csv`
1. `period` (PK)
1. `date`  (PK)
1. `poster_count`

## `(daily|weekly|monthly|yearly)_new_poster_counts.csv`
1. `period` (PK)
1. `date` (PK)
1. `new_poster_count`

## `(daily|weekly|monthly|yearly)_new_10plus_poster_counts.csv`
1. `period` (PK)
1. `date` (PK)
1. `new_10plus_poster_count`
