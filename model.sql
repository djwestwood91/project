-- Landing / staging schema (raw source data)
-- drop schema if exists pokemon_landing cascade;
create schema if not exists pokemon_landing authorization postgres;

-- Main normalized schema
-- drop schema if exists pokemon cascade;
create schema if not exists pokemon authorization postgres;

-- Lookup tables for normalization
drop table if exists pokemon.language;
create table if not exists pokemon.language (
  language_id serial primary key,
  name text not null unique
);

drop table if exists pokemon.card_set;
create table if not exists pokemon.card_set (
  card_set_id serial primary key,
  name text not null,
  year text not null,
  unique(name, year)
);

drop table if exists pokemon.grading_company;
create table if not exists pokemon.grading_company (
  grading_company_id serial primary key,
  name text not null unique,
  full_name text
);

drop table if exists pokemon.grading_company;
create table if not exists pokemon.grading_company (
  grading_company_id serial primary key,
  name text not null unique
);
  
drop table if exists pokemon.rarity;
create table if not exists pokemon.rarity (
  rarity_id serial primary key,
  name text not null unique
);

-- Core card table
drop table if exists pokemon.card;
create table if not exists pokemon.card (
     card_id bigint generated always as identity primary key,
     card text not null,
     card_set_id int references pokemon.card_set(card_set_id),
     card_year text,
     card_holo_flag boolean not null default false,
     card_first_edition_flag boolean not null default false,
     card_promo_flag boolean not null default false,
     card_language_id int references pokemon.language(language_id),
     card_rarity_id int references pokemon.rarity(rarity_id),
     extra_details jsonb,
     created_at timestamptz not null default now()
);

-- Grades
drop table if exists pokemon.card_grade;
create table if not exists pokemon.card_grade (
     grade_id bigint generated always as identity primary key,
     card_id bigint not null references pokemon.card(card_id) on delete cascade,
     grade numeric(5,2) not null,
     grade_description text,
     grading_company_id int references pokemon.grading_company(grading_company_id),
     grading_certification_number text unique,
     graded_card_url text,
     graded_at timestamptz
);

create index if not exists idx_card_grade_card_id on pokemon.card_grade(card_id);

-- Sellers and purchases
drop table if exists pokemon.seller;
create table if not exists pokemon.seller (
     seller_id bigint generated always as identity primary key,
     seller text not null,
     website text
);

drop table if exists pokemon.card_seller;
create table if not exists pokemon.card_seller (
    card_id bigint not null references pokemon.card(card_id) on delete cascade,
    seller_id bigint not null references pokemon.seller(seller_id),
    purchase_price numeric,
    purchase_price_currency text,
    purchase_source text,
    date_purchased date,
    quantity int,
    constraint pk_card_seller primary key (card_id, seller_id)
);
