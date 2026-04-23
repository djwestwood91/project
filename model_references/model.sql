-- Landing / staging schema (raw source data)
drop schema if exists pokemon_landing cascade;
create schema if not exists pokemon_landing authorization postgres;

-- Landing table for raw Pokemon card data
drop table if exists pokemon_landing.landing_pokemon_card;
create table if not exists pokemon_landing.landing_pokemon_card (
  row_id int primary key,
  card text,
  card_set text not null default 'Unknown',
  card_year text,
  card_graded boolean,
  grade numeric(5,2),
  grading_company text,
  grading_company_full_name text,
  grade_description text,
  grading_certification_number text unique,
  graded_card_url text,
  graded_date date,
  card_holo_flag boolean,
  card_first_edition_flag boolean,
  card_promo_flag boolean,
  card_language text,
  card_rarity text,
  card_additional_details_1 text,
  card_additional_details_2 text,
  card_additional_details_3 text,
  card_purchase_price numeric,
  postage_fees numeric,
  total_price numeric,
  card_currency text,
  card_date_purchased date,
  card_source text,
  card_seller text,
  website text,
  seller_country text,
  image_reference text,
  created_at timestamptz not null default now()
);

-- Main normalized schema
drop schema if exists pokemon_facts cascade;
create schema if not exists pokemon_facts authorization postgres;

-- Dimension schema (lookup tables)
drop schema if exists pokemon_dimensions cascade;
create schema if not exists pokemon_dimensions authorization postgres;

-- Facts schema
drop schema if exists pokemon_facts cascade;
create schema if not exists pokemon_facts authorization postgres;

-- Lookup tables for normalization (DIMENSIONS)
drop table if exists pokemon_dimensions.language;
create table if not exists pokemon_dimensions.language (
  language_id serial primary key,
  name text not null unique
);

drop table if exists pokemon_dimensions.card_set;
create table if not exists pokemon_dimensions.card_set (
  card_set_id serial primary key,
  name text not null,
  year text not null,
  unique(name, year)
);

drop table if exists pokemon_dimensions.grading_company;
create table if not exists pokemon_dimensions.grading_company (
  grading_company_id serial primary key,
  company text not null unique,
  company_full_name text
);

drop table if exists pokemon_dimensions.grade_description;
create table if not exists pokemon_dimensions.grade_description (
  grade_description_id serial primary key,
  grading_company_id int not null references pokemon_dimensions.grading_company(grading_company_id),
  grade numeric(5,2) not null,
  grade_description text not null,
  unique(grading_company_id, grade)
);
  
drop table if exists pokemon_dimensions.rarity;
create table if not exists pokemon_dimensions.rarity (
  rarity_id serial primary key,
  rarity text not null unique
);

drop table if exists pokemon_dimensions.currency;
create table if not exists pokemon_dimensions.currency (
  currency_id serial primary key,
  currency_code text not null unique
);

drop table if exists pokemon_dimensions.purchase_source;
create table if not exists pokemon_dimensions.purchase_source (
  purchase_source_id serial primary key,
  source text not null unique
);

drop table if exists pokemon_dimensions.country;
create table if not exists pokemon_dimensions.country (
  country_id serial primary key,
  country text not null unique
);

-- Core card table (FACT)
drop table if exists pokemon_facts.card;
create table if not exists pokemon_facts.card (
  card_id bigint generated always as identity primary key,
  row_id int,
  card text not null,
  card_set_id int references pokemon_dimensions.card_set(card_set_id),
  card_holo_flag boolean not null default false,
  card_first_edition_flag boolean not null default false,
  card_promo_flag boolean not null default false,
  card_language_id int references pokemon_dimensions.language(language_id),
  card_rarity_id int references pokemon_dimensions.rarity(rarity_id),
  extra_details jsonb,
  card_image_reference text,
  created_at timestamptz not null default now()
);

-- Card instance (FACT)
drop table if exists pokemon_facts.card_instance;
create table if not exists pokemon_facts.card_instance (
  card_instance_id bigint generated always as identity primary key,
  row_id int,
  card_id bigint not null references pokemon_facts.card(card_id) on delete cascade,
  created_at timestamptz not null default now()
);

create index if not exists idx_card_instance_row_id on pokemon_facts.card_instance(row_id);
create index if not exists idx_card_instance_card_id on pokemon_facts.card_instance(card_id);

-- Card grades (FACT)
drop table if exists pokemon_facts.card_grade;
create table if not exists pokemon_facts.card_grade (
  grade_id bigint generated always as identity primary key,
  row_id int,
  card_instance_id bigint not null references pokemon_facts.card_instance(card_instance_id) on delete cascade,
  grade_description_id int not null references pokemon_dimensions.grade_description(grade_description_id),
  grading_certification_number text unique,
  graded_card_url text,
  created_at timestamptz not null default now()
);

create index if not exists idx_card_grade_row_id on pokemon_facts.card_grade(row_id);
create index if not exists idx_card_grade_card_instance_id on pokemon_facts.card_grade(card_instance_id);

-- Sellers (FACT)
drop table if exists pokemon_facts.seller;
create table if not exists pokemon_facts.seller (
  seller_id bigint generated always as identity primary key,
  row_id int,
  seller text not null,
  website text,
  country_id int references pokemon_dimensions.country(country_id),
  created_at timestamptz not null default now(),
  unique(seller, website)
);

-- Purchases (FACT)
drop table if exists pokemon_facts.purchase;
create table if not exists pokemon_facts.purchase (
  purchase_id bigint generated always as identity primary key,
  row_id int,
  card_instance_id bigint not null references pokemon_facts.card_instance(card_instance_id),
  grade_id bigint references pokemon_facts.card_grade(grade_id),
  seller_id bigint not null references pokemon_facts.seller(seller_id),
  purchase_price numeric,
  postage_fees numeric,
  total_price numeric,
  currency_id int references pokemon_dimensions.currency(currency_id),
  purchase_source_id int references pokemon_dimensions.purchase_source(purchase_source_id),
  date_purchased date,
  created_at timestamptz default now()
);

create index if not exists idx_purchase_row_id on pokemon_facts.purchase(row_id);
create index if not exists idx_purchase_card_instance_id on pokemon_facts.purchase(card_instance_id);
create index if not exists idx_purchase_seller_id on pokemon_facts.purchase(seller_id);

-- API reference schema for TCGdex card data
drop schema if exists pokemon_api cascade;
create schema if not exists pokemon_api authorization postgres;

drop table if exists pokemon_api.tcgdex_card_reference;
create table if not exists pokemon_api.tcgdex_card_reference (
    tcgdex_card_ref_id bigint generated always as identity primary key,
    card_id bigint not null references pokemon_facts.card(card_id) on delete cascade,
    language_id int not null references pokemon_dimensions.language(language_id),
    tcgdex_id text not null unique,
    tcgdex_localid text not null,
    tcgdex_name text not null,
    tcgdex_image text,
    created_at timestamptz default now(),
    updated_at timestamptz default now()
);

create index if not exists idx_tcgdex_id on pokemon_api.tcgdex_card_reference(tcgdex_id);
create index if not exists idx_tcgdex_card_id on pokemon_api.tcgdex_card_reference(card_id);
create index if not exists idx_tcgdex_localid on pokemon_api.tcgdex_card_reference(tcgdex_localid);
