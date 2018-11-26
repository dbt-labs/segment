select 1

{% form %}

date_part:
    type: select
    default: day
    options: [hour, day, week, month]

    
start_date:
    type: date
    default: 2018-11-01
    
end_date:
    type: date
    default: 2018-12-01

{% endform %}
