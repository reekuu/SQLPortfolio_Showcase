import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from common import pull_data_from_source_a, push_data_to_source_b
from pendulum import datetime, duration

TABLE_NAME = 'generic_table_v1'

@dag(
    dag_id='data_processing_workflow',
    schedule_interval='55 3 * * *',
    start_date=datetime(year=2023, month=10, day=1, tz='Europe/Moscow'),
    dagrun_timeout=duration(minutes=20),
    max_active_runs=1,
    tags=['category1', 'category2', 'category3', 'metric1', 'metric2', 'identifier'],
)
def generic_data_processing():
    @task
    def pull_data_step_one():
        date = get_current_context().get('ds')
        query = f'''
            SELECT
                DATE(event_timestamp) AS date,
                identifier,
                measure AS data_metric_one
            FROM transactions t
            JOIN (
                SELECT
                    entity_id,
                    identifier
                FROM interactions i
                JOIN entities e ON i.entity_ref = e.id
                WHERE 1=1
                    AND i.processed = 'y'
                    AND i.identifier NOT IN (SELECT identifier FROM excluded_identifiers)
                ) sub_query USING(entity_id)
            WHERE event_timestamp BETWEEN '{date} 00:00:00' AND '{date} 23:59:59';
        '''
        return pull_data_from_source_a(query)

    @task
    def pull_data_step_two():
        date = get_current_context().get('ds')
        query = f'''
            SELECT
                DATE(event_timestamp) AS date,
                identifier,
                measure AS metric_percent,
                measure/exchange_rate AS metric_full_amount
            FROM metrics
            WHERE 1=1
                AND identifier NOT IN (SELECT identifier FROM excluded_identifiers)
                AND event_timestamp BETWEEN '{date} 00:00:00' AND '{date} 23:59:59';
        '''
        return pull_data_from_source_a(query)

    @task
    def pull_data_step_three():
        date = get_current_context().get('ds')
        query = f'''
            SELECT
                DATE(event_timestamp) AS date,
                identifier,
                measure AS metric_fine
            FROM penalties
            WHERE 1=1
                AND category = 'FINE'
                AND approved = 'y'
                AND deleted = 'n'
                AND identifier NOT IN (SELECT identifier FROM excluded_identifiers)
                AND event_timestamp BETWEEN '{date} 00:00:00' AND '{date} 23:59:59';
        '''
        return pull_data_from_source_a(query)

    @task
    def pull_data_step_four():
        date = get_current_context().get('ds')
        query = f'''
            SELECT
                DATE(event_timestamp) AS date,
                identifier,
                measure AS metric_payout
            FROM payouts
            WHERE 1=1
                AND category IN ('CAT1', 'CAT2', 'CAT3', 'CAT4')
                AND approved = 'y'
                AND deleted = 'n'  
                AND identifier NOT IN (SELECT identifier FROM excluded_identifiers)
                AND event_timestamp BETWEEN '{date} 00:00:00' AND '{date} 23:59:59';
        '''
        return pull_data_from_source_a(query)

    @task
    def pull_data_step_five():
        date = get_current_context().get('ds')
        query = f'''
            SELECT
                DATE(event_timestamp) AS date,
                identifier,
                measure AS metric_hosting_payout
            FROM hosting_payouts
            WHERE 1=1
                AND category = 'HOSTING'
                AND approved = 'y'
                AND deleted = 'n'
                AND identifier NOT IN (SELECT identifier FROM excluded_identifiers)
                AND event_timestamp BETWEEN '{date} 00:00:00' AND '{date} 23:59:59';
        '''
        return pull_data_from_source_a(query)

    @task
    def pull_activation_data():
        date = get_current_context().get('ds')
        query = f'''
            SELECT
                DATE(log_entry_timestamp) date,
                interaction_id,
                'Yes' AS activation_within_180d
            FROM log_entries le
            JOIN entities e USING(entity_id)
            JOIN interactions i ON i.id = e.interaction_ref
            WHERE 1=1
                AND le.log_entry_timestamp BETWEEN DATE_SUB('{date}', INTERVAL 180 DAY) AND '{date} 23:59:59'
                AND le.action = 'specific_action'
                AND i.identifier NOT IN (SELECT identifier FROM excluded_identifiers)
            GROUP BY 1, 2
            UNION
            SELECT
                DATE(entity_state.first_payment_timestamp) date,
                i.identifier,
                'Yes' AS activation_within_180d
            FROM entity_states es
            JOIN entities e USING(entity_id)
            JOIN interactions i ON i.id = e.interaction_ref
            WHERE 1=1
                AND es.first_payment_timestamp BETWEEN DATE_SUB('{date}', INTERVAL 180 DAY) AND '{date} 23:59:59'
                AND e.type = 'specific_type'
                AND i.identifier NOT IN (SELECT identifier FROM excluded_identifiers)
            GROUP BY 1, 2
            ORDER BY 1;
        '''
        return pull_data_from_source_a(query)

    @task
    def merge_and_push_data(df1, df2, df3, df4, df5, df6, table_name):
        df_combined = pd.concat([df1, df2, df3, df4, df5]).groupby(by=['date', 'identifier'], as_index=False).sum()
        if not df_combined.empty:
            df_combined.date = pd.to_datetime(df_combined.date)
            df6.date = pd.to_datetime(df6.date)
            df_combined = pd.merge_asof(
                df_combined, df6,
                on='date',
                by='identifier',
                direction='backward',
                tolerance=pd.Timedelta('180d'),
                ).fillna('No')
            date = get_current_context().get('ds')
            push_data_to_source_b(df=df_combined, table_name=table_name, date=date)
        else:
            print('INFO: The input DataFrame is empty, so the push to the database is canceled.')

    merge_and_push_data(
        pull_data_step_one(),
        pull_data_step_two(),
        pull_data_step_three(),
        pull_data_step_four(),
        pull_data_step_five(),
        pull_activation_data(),
        TABLE_NAME,
    )

generic_data_processing()
