class UserMetricsJob:
    def __init__(self, events_path, users_path, out_path, min_date, max_date, use_udf):
        self.events_path = events_path
        self.users_path = users_path
        self.out_path = out_path
        self.min_date = min_date
        self.max_date = max_date
        self.use_udf = use_udf

    def load_events(self):
        import pandas as pd
        events_schema = {
            'user_id': str,
            'event_type': str,
            'score': int,
            'amount': float,
            'ts': 'datetime64[ns]'
        }
        return pd.read_csv(self.events_path, dtype=events_schema)

    def load_users(self):
        import pandas as pd
        users_schema = {
            'user_id': str,
            'country': str
        }
        return pd.read_csv(self.users_path, dtype=users_schema)

    def transform(self, events, users):
        import pandas as pd
        from datetime import datetime

        # Filter by event type and time window
        events['ts'] = pd.to_datetime(events['ts'])
        filtered = events[(events['event_type'].isin(['click', 'purchase'])) &
                          (events['ts'] >= datetime.strptime(self.min_date, '%Y-%m-%d')) &
                          (events['ts'] < datetime.strptime(self.max_date, '%Y-%m-%d'))]

        # Score bucketing
        if self.use_udf:
            filtered['score_bucket'] = filtered['score'].apply(self.bucket_score_udf)
        else:
            filtered['score_bucket'] = filtered['score'].apply(self.bucket_score_builtin)

        # Aggregate user revenue & events
        aggregated = filtered.groupby('user_id').agg(
            revenue=pd.NamedAgg(column='amount', aggfunc='sum'),
            event_count=pd.NamedAgg(column='event_type', aggfunc='count')
        ).reset_index()

        # Join user dims
        joined = pd.merge(aggregated, users, on='user_id', how='left')

        # Rank users by revenue per country
        joined['rank'] = joined.groupby('country')['revenue'].rank(method='dense', ascending=False)

        return joined

    def bucket_score_udf(self, score):
        if score is None:
            return 'unknown'
        elif score >= 80:
            return 'high'
        elif score >= 50:
            return 'medium'
        else:
            return 'low'

    def bucket_score_builtin(self, score):
        if pd.isnull(score):
            return 'unknown'
        elif score >= 80:
            return 'high'
        elif score >= 50:
            return 'medium'
        else:
            return 'low'

    def run(self):
        events = self.load_events()
        users = self.load_users()
        transformed = self.transform(events, users)
        transformed.to_parquet(self.out_path, index=False)
        print(transformed.head())

# Example usage:
job = UserMetricsJob(
    events_path='sample_data/events.csv',
    users_path='sample_data/users.csv',
    out_path='out/user_metrics.parquet',
    min_date='1970-01-01',
    max_date='2100-01-01',
    use_udf=False
)
job.run()