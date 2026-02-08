class UserMetricsJob:
    def __init__(self):
        pass

    @staticmethod
    def main(args):
        events_path = args.get('--events', 'sample_data/events.csv')
        users_path = args.get('--users', 'sample_data/users.csv')
        out_path = args.get('--out', 'out/user_metrics_parquet')
        min_date = args.get('--from', '1970-01-01')
        max_date = args.get('--to', '2100-01-01')
        use_udf = args.get('--useUdf', False)

        try:
            print(f"Starting job with events={events_path}, users={users_path}, out={out_path}, window=[{min_date}, {max_date}], useUdf={use_udf}")
            
            events = UserMetricsJob.load_events(events_path)
            users = UserMetricsJob.load_users(users_path)

            transformed = UserMetricsJob.transform(events, users, min_date, max_date, use_udf)

            # Write Parquet output
            transformed.to_parquet(out_path, index=False)

            # For quick visibility in tests/logs
            print(transformed.head())

            print(f"Job completed successfully. Output: {out_path}")
        except Exception as e:
            print(f"Unexpected error: {e}")
            raise RuntimeError("Unhandled exception")

    @staticmethod
    def load_events(path):
        import pandas as pd
        events_schema = {
            'user_id': 'str',
            'event_type': 'str',
            'score': 'Int64',
            'amount': 'float',
            'ts': 'datetime64[ns]'
        }
        return pd.read_csv(path, dtype=events_schema)

    @staticmethod
    def load_users(path):
        import pandas as pd
        users_schema = {
            'user_id': 'str',
            'country': 'str'
        }
        return pd.read_csv(path, dtype=users_schema)

    @staticmethod
    def transform(events, users, min_date, max_date, use_udf):
        import pandas as pd
        from pandas.api.types import CategoricalDtype

        # Filter by event type and time window
        events['ts'] = pd.to_datetime(events['ts'])
        in_window = (events['ts'] >= min_date) & (events['ts'] < max_date)
        filtered = events[in_window & events['event_type'].isin(['click', 'purchase'])]

        # Score bucketing
        if use_udf:
            filtered['score_bucket'] = filtered['score'].apply(UserMetricsJob.bucket_score_udf)
        else:
            filtered['score_bucket'] = pd.cut(
                filtered['score'],
                bins=[-float('inf'), 79, float('inf')],
                labels=['low', 'high'],
                right=False
            )

        # Aggregate user revenue and events
        grouped = filtered.groupby('user_id').agg(
            revenue=('amount', 'sum'),
            event_count=('event_type', 'size')
        ).reset_index()

        # Join user dims
        result = pd.merge(grouped, users, on='user_id', how='left')

        # Rank users by revenue per country
        result['country_rank'] = result.groupby('country')['revenue'].rank(method='dense', ascending=False)

        return result

    @staticmethod
    def bucket_score_udf(score):
        if pd.isna(score):
            return 'unknown'
        elif score >= 80:
            return 'high'
        else:
            return 'low'

if __name__ == "__main__":
    import sys
    args = dict(arg.split('=') for arg in sys.argv[1:])
    UserMetricsJob.main(args)
