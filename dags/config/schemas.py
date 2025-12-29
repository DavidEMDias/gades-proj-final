from config.categories.categories_schema import schema_fields as categories_schema
from config.customers.customers_schema import schema_fields as customers_schema
from config.order_details.order_details_schema import schema_fields as order_details_schema
from config.orders.orders_schema import schema_fields as orders_schema
from config.products.products_schema import schema_fields as products_schema
from config.shippers.shippers_schema import schema_fields as shippers_schema

SCHEMA_REGISTRY = {
    "categories": categories_schema,
    "customers": customers_schema,
    "order_details": order_details_schema,
    "orders": orders_schema,
    "products": products_schema,
    "shippers": shippers_schema,
}
