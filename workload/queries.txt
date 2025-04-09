-- \subsection*{Q1}

SELECT is_opened, avg(subject_length) as NB
from h25_messages, h25_campaigns
WHERE campaign_id = h25_campaigns.id
AND campaign_type = message_type
GROUP BY is_opened;


-- \subsection*{Q2}

SELECT is_opened, count(1) as NB
from h25_messages, h25_campaigns
WHERE campaign_id = h25_campaigns.id
and campaign_type = message_type and email_provider = 'mail.ru'
GROUP BY is_opened;



-- \subsection*{Q3}

select is_opened, is_clicked, count(1)
from h25_messages
where campaign_id = 27
  and email_provider != 'mail.ru'
group by is_opened, is_clicked;


-- \subsection*{Q4}

SELECT message_type, count(message_id)
from h25_messages, h25_campaigns
WHERE campaign_id = h25_campaigns.id
and campaign_type = message_type
AND sent_at > '2021-06-02'::date
GROUP BY message_type ;


-- \subsection*{Q5}

SELECT message_type, is_clicked, count(message_id)
from h25_messages, h25_campaigns
WHERE campaign_id = h25_campaigns.id
and campaign_type = message_type
AND sent_at > '2021-06-02'::date and email_provider != 'mail.ru'
GROUP BY message_type, is_clicked ;



-- \subsection*{Q6}

SELECT count(distinct m1.client_id)
from h25_messages m1, h25_messages m2
WHERE m1.client_id = m2.client_id and m1.id != m2.id
and m1.email_provider = 'mail.ru'
and m1.sent_at < m2.sent_at
and m2.message_type = 'trigger' and m1.message_type = 'bulk';


-- \subsection*{Q7}

select h25_messages.channel, count(1)
from h25_messages,
     h25_campaigns,
     h25_purchase
WHERE campaign_id = h25_campaigns.id
  and campaign_type = message_type
  and h25_messages.client_id = h25_purchase.client_id
  and first_purchase_date >= h25_messages.date
  and is_purchased = 't'
GROUP BY h25_messages.channel;


-- \subsection*{Q8}

SELECT holiday, count(1) as NB
from h25_messages, h25_holidays
WHERE h25_holidays.date = sent_at::date
GROUP BY h25_holidays.date;


-- \subsection*{Q9}

select email_provider, count(1)
from h25_messages,
     h25_campaigns,
     h25_purchase
WHERE campaign_id = h25_campaigns.id
  and campaign_type = message_type
  and h25_messages.client_id = h25_purchase.client_id
  and first_purchase_date >= h25_messages.date
  and is_purchased = 't'
  and message_type = 'bulk'
GROUP BY email_provider;


-- \subsection*{Q10}

select email_provider
from h25_messages,
     h25_campaigns,
     h25_purchase
WHERE campaign_id = h25_campaigns.id
  and campaign_type = message_type
  and h25_messages.client_id = h25_purchase.client_id
  and first_purchase_date >= h25_messages.date
  and is_purchased = 't'
  and h25_messages.client_id in (select client_id from h25_purchase, h25_holidays
                                    where date = first_purchase_date)
GROUP BY email_provider
order by count(1) desc limit 1;


-- -- \subsection*{Q11}

select h25_messages.client_id
from h25_messages,
     h25_campaigns,
     h25_purchase,
     h25_holidays
WHERE campaign_id = h25_campaigns.id
  and campaign_type = message_type
  and h25_messages.client_id = h25_purchase.client_id
  and first_purchase_date >= h25_messages.date
  and holiday = 'Cyber Monday Sale'
  and is_purchased = 't'
  and message_type = 'transactional';


-- -- \subsection*{Q12}
select is_opened, is_clicked, count(1)
from h25_messages,
     h25_campaigns,
     h25_purchase,
     h25_holidays
WHERE campaign_id = h25_campaigns.id
  and campaign_type = message_type
  and h25_messages.client_id = h25_purchase.client_id
  and first_purchase_date >= h25_messages.date
  and holiday = 'Cyber Monday Sale'
  and campaign_id = 27
  and email_provider != 'mail.ru'
group by is_opened, is_clicked;