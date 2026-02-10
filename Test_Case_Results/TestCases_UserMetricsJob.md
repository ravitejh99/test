### Test Cases for UserMetricsJob

#### Test Case 1: Validate Filtering Logic
- **Description:** Ensure events are filtered correctly based on type (`click`, `purchase`) and date range.
- **Input:** Events with various types and timestamps.
- **Expected Output:** Only events of type `click` or `purchase` within the date range are retained.

#### Test Case 2: Validate Score Bucketing
- **Description:** Verify scores are categorized into `unknown`, `low`, `medium`, and `high` buckets.
- **Input:** Events with scores ranging from null to 100.
- **Expected Output:** Scores are correctly assigned to their respective buckets.

#### Test Case 3: Validate Aggregation Logic
- **Description:** Confirm revenue and event counts are aggregated correctly for each user.
- **Input:** Filtered events with user IDs, scores, and amounts.
- **Expected Output:** Correct revenue and event counts for each user.

#### Test Case 4: Validate Joining Logic
- **Description:** Ensure aggregated data is correctly joined with user information.
- **Input:** Aggregated data and user data.
- **Expected Output:** Joined dataset with user details included.

#### Test Case 5: Validate Ranking Logic
- **Description:** Verify users are ranked by revenue within their respective countries.
- **Input:** Joined dataset with user IDs, countries, and revenues.
- **Expected Output:** Users are ranked correctly by revenue within each country.