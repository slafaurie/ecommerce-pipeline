CREATE TABLE datamart."mart_daily_performance" AS
    select
    purchase_date
    , customer_state
    , order_status
    , count(order_id) as count_orders
    , sum(payment_total_sum) as payment_sum
    , sum(total_freight) as freight_sum
    , sum(total_price) as total_costs
    from public."orders"
    group by 1,2,3;