CREATE VIEW public.monthly_view AS
-- Добавляем порядковый номер платежа
WITH added_row_number AS (
        SELECT    userid,
                  Date_trunc('month', payment_date),
                  Row_number() OVER(partition BY userid ORDER BY payment_date ASC)
        FROM      payments_data ),
-- Генерируем календарь от первой даты в данных до текущей
calendar AS (
        SELECT    Generate_series(Min(date_trunc), CURRENT_DATE, '1 month')
        FROM      added_row_number ),
-- Объединяем месяцы с платежами и без платежей в одну таблицу
userid_gen_series AS (
        SELECT    userid,
                  generate_series
        FROM     (SELECT *
                  FROM   added_row_number
                  WHERE  row_number = 1) AS u,
        LATERAL  (SELECT *
                  FROM   calendar AS c
                  WHERE  c.generate_series >= u.date_trunc) AS x ),
-- Считаем признак "платил ли клиент"
userid_is_paid AS (
        SELECT    u.generate_series,
                  u.userid,
                 (CASE WHEN a.userid IS NULL THEN 0 ELSE 1 END) AS is_paid,
                  Count(a.rn) OVER (partition BY u.userid ORDER BY u.generate_series, a.rn) AS rn_group
        FROM      userid_gen_series AS u
        LEFT JOIN(SELECT userid,
                         date_trunc,
                         row_number AS rn
                  FROM   added_row_number) AS a
        ON        u.generate_series = a.date_trunc
        AND       u.userid = a.userid )
-- Финальный датасет с признаком "сколько месяцев прошло с последней оплаты"
                  -- DISTINCT дропает случаи нескольких платежей в одном месяце
        SELECT    DISTINCT to_char(generate_series, 'YYYY"-"MM') AS month,
                  userid,
                  is_paid,
                  Row_number() OVER(partition BY userid, rn_group) - 1 AS number_of_not_paid
        FROM      userid_is_paid
        ORDER BY  month;
ALTER TABLE public.monthly_view
    OWNER TO postgres;