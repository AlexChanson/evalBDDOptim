create table public.h25_messages
(
    id         integer not null
               constraint h25_messages_pk primary key,
    message_id            text,
    campaign_id           integer,
    message_type          text,
    client_id             numeric,
    channel               text,
    category              text,
    platform              text,
    email_provider        text,
    stream                text,
    "date"                  date,
    sent_at               timestamp,
    is_opened             text,
    opened_first_time_at  timestamp,
    opened_last_time_at   timestamp,
    is_clicked            text,
    clicked_first_time_at timestamp,
    clicked_last_time_at  timestamp,
    is_unsubscribed       text,
    unsubscribed_at       timestamp,
    is_hard_bounced       text,
    hard_bounced_at       timestamp,
    is_soft_bounced       text,
    soft_bounced_at       timestamp,
    is_complained         text,
    complained_at         timestamp,
    is_blocked            text,
    blocked_at            timestamp,
    is_purchased          text,
    purchased_at          timestamp,
    created_at            timestamp,
    updated_at            timestamp
);
create table public.h25_campaigns
(
    id                           integer not null,
    campaign_type                text    not null,
    channel                      text,
    topic                        text,
    started_at                   timestamp,
    finished_at                  timestamp,
    total_count                  integer,
    ab_test                      text,
    warmup_mode                  boolean,
    hour_limit                   numeric,
    subject_length               numeric,
    subject_with_personalization boolean,
    subject_with_deadline        boolean,
    subject_with_emoji           text,
    subject_with_bonuses         text,
    subject_with_discount        text,
    subject_with_saleout         text,
    is_test                      boolean,
    position                     text,
    constraint h25_campaigns_pk primary key (campaign_type, id)
);
create table public.h25_holidays
(
    "date"    date not null
        constraint h25_holidays_pk
            primary key,
    holiday text
);
create table public.h25_purchase
(
    client_id           numeric not null
        constraint h25_purchase_pk
            primary key,
    first_purchase_date date
);