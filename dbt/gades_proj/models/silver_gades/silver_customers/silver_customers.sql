{% if execute and is_incremental() %}
    {% set query = 'select COALESCE(max(audit_updated_at), CAST(\'2000-01-01 00:00:00\' AS TIMESTAMP)) from {}'.format(this) %}
    {% set result = run_query(query).columns[0].values()[0] %}
{% endif %}

with source as (
        select *
        from {{ source('gades_source', 'bronze_customers') }} source_table
        {% if is_incremental() %}
        WHERE
          source_table.updated_at > '{{ result }}'
        {% endif %}
  ),

dedup as (
    select *,
           row_number() over (
               partition by CustomerID
               order by updated_at desc
           ) as _rn
    from source
)

      select
        CustomerID as code_customer,
        CustomerName as dsc_customer_name,
        ContactName as dsc_customer_contact_name,
        Address as dsc_customer_address,
        City as dsc_customer_city,
        PostalCode as dsc_customer_zip_code_prefix,
        Country as dsc_customer_country,
        CURRENT_TIMESTAMP as audit_created_at,
        CURRENT_TIMESTAMP as audit_updated_at
      from dedup
      where _rn = 1
      

  