with
    raw_data as (
        select
            cast(creditcardid as int) as pk_credit_card_id
            , cast(cardtype as varchar) as card_type
            , cast(cardnumber as int) as card_number 
            , cast(expmonth as int) as expiration_month
            , cast(expyear as int) as expiration_year
            --unused columns:
            --modifieddate
        from {{ source('erp', 'creditcard') }}
    )
select *
from raw_data