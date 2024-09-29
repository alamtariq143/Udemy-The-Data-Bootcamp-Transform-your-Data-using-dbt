select
--from raw orders
{{ dbt_utils.generate_surrogate_key(['o.orderid','c.customerid','p.productid']) }} as sk_orders,
o.orderid,
o.orderdate,
o.shipdate,
o.shipmode,
o.customerid,
o.productid,
o.ordersellingprice,
o.ordercostprice,
--from raw customer
c.customerid,
c.customername,
c.segment,
c.country,
--from raw_product
p.productid,
p.category,
p.productname,
p.subcategory,
ordersellingprice - ordercostprice as orderprofit,
{{ markup() }} as markup
from {{ ref('raw_orders') }} as o
left join {{ ref('raw_customer') }} as c
on o.customerid = c.customerid
left join {{ ref('raw_product') }} as p
on o.productid = p.productid

{{limit_data_in_dev('orderdate')}}