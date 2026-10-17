{{ config(severity='warn') }}

-- The first 3 digits of the zip code must belong to a range of the state: the test fails if this query returns rows
select
    u.user_id,
    u.state,
    u.zip_code
from {{ ref('stg_users') }} u
where not exists (
    select 1
    from {{ ref('states') }} s
    where s.code = u.state
      and cast(left(u.zip_code, 3) as integer) between s.zip_prefix_min and s.zip_prefix_max
)
