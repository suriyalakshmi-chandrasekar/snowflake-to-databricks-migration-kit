-- A typical reporting query: translates fully automatically.
SELECT
    order_date,
    region,
    IFF(channel = 'WEB', 'online', 'store')              AS channel_group,
    COUNT(*)                                              AS orders,
    SUM(NVL(amount, 0))                                   AS revenue,
    DATEDIFF(day, MIN(created_at), MAX(shipped_at))       AS max_days_to_ship,
    MAX(TRY_TO_NUMBER(discount_code_pct))                 AS max_discount_pct
FROM sales.orders
WHERE order_date >= DATEADD(month, -3, CURRENT_DATE())
GROUP BY 1, 2, 3
QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC) <= 10
ORDER BY revenue DESC;
