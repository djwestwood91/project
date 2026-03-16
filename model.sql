-- drop schema if exists pokemon_landing cascade;
create schema if not exists pokemon_landing authorization postgres;

select * from pokemon_landing.landing_pokemon_card lpc 

-- drop schema if exists pokemon cascade;
create schema if not exists pokemon authorization postgres;

create sequence if not exists pokemon.card_seq
start 1
increment 1
minvalue 1
maxvalue 2147483647
cache 1;

drop table if exists pokemon.card;
create table if not exists pokemon.card (
     card_id bigint not null default nextval('pokemon.card_seq'),
     card text not null,
     card_set text null,
     card_year text null,
     card_holo_flag bool null,
     card_first_edition_flag bool null,
     card_language text null,
     card_additional_details_1 text null,
     card_additional_details_2 text null,
     CONSTRAINT pk_card_id PRIMARY KEY (card_id)
);

create sequence if not exists pokemon.grade_seq
start 1
increment 1
minvalue 1
maxvalue 2147483647
cache 1;

drop table if exists pokemon.card_grade;
create table if not exists pokemon.card_grade (
     grade_id bigint not null default nextval('pokemon.grade_seq'),
     card_id bigint not null,
     grade numeric not null,
     grade_description text null,
     grading_company text null,
     grading_certification_number text null,
     graded_card_url text null,
     constraint pk_grade_id PRIMARY key (grade_id),
     foreign key (card_id) references pokemon.card(card_id)
);

create sequence if not exists pokemon.seller_seq
start 1
increment 1
minvalue 1
maxvalue 2147483647
cache 1;

drop table if exists pokemon.seller;
create table if not exists pokemon.seller (
     seller_id bigint not null default nextval('pokemon.seller_seq'),
     seller text not null,
     website text null,
     CONSTRAINT pk_seller_id PRIMARY KEY (seller_id)
);

drop table if exists pokemon.card_seller;
create table if not exists pokemon.card_seller ( 
	card_id bigint not null,
	seller_id bigint not null,
	purchase_price numeric null,
	purchase_price_currency text null,
	purchase_source text null,
	date_purchased date null,
	quantity int null,
	constraint pk_card_seller PRIMARY KEY (card_id, seller_id),
    foreign key (card_id) references pokemon.card(card_id),
    foreign key (seller_id) references pokemon.seller(seller_id)
);
