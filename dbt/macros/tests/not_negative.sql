{% test not_negative(model, columns) %}
with invalid as (
    select *
    from {{ model }}
    where {% for col in columns %}
        {{ col }} < 0
        {% if not loop.last %} or {% endif %}
    {% endfor %}
)
select count(*) as num_of_invalid_records
from invalid
{% endtest %}
