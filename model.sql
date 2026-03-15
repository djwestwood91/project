-- drop schema if exists pokemon_landing cascade;
create schema if not exists pokemon_landing authorization postgres;

-- main landing table create from excel using the python script
-- flexible as we are driving this by the excel file
-- select * from pokemon_landing.landing_pokemon_card

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
     card_first_eidition_flag bool null,
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

drop table if exists pokemon.grade;
create table if not exists pokemon.grade (
     grade_id bigint not null default nextval('pokemon.grade_seq'),
     grade numeric not null,
     grade_description text null,
     grading_company text null,
     grading_certification_number text null,
     grade_card_url text null,
     CONSTRAINT pk_grade_id PRIMARY KEY (grade_id)
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

drop table if exists pokemon.seller;
create table if not exists pokemon.seller (
     seller_id bigint not null default nextval('pokemon.seller_seq'),
     seller text not null,
     website text null,
     CONSTRAINT pk_seller_id PRIMARY KEY (seller_id)
);

-- sequence for the surrogate_id
create sequence if not exists pokemon.card_grade_seq
start 1
increment 1
minvalue 1
maxvalue 2147483647
cache 1;

drop table if exists pokemon.card_grade;
create table if not exists pokemon.card_grade (
	card_grade_id bigint not null default nextval('pokemon.card_grade_seq') primary_key, 
	card_id bigint not null,
	grade_id bigint not null,
	unique (card_id, grade_id),
    foreign key (card_id) references pokemon.card(card_id),
    foreign key (grade_id) references pokemon.grade(grade_id)
);

-- sequence for the surrogate_id
create sequence if not exists pokemon.card_seller_seq
start 1
increment 1
minvalue 1
maxvalue 2147483647
cache 1;

drop table if exists pokemon.card_seller;
create table if not exists pokemon.card_seller (
	card_sale_id bigint not null default nextval('pokemon.card_seller_seq') primary key, 
	card_id bigint not null,
	sale_id bigint not null,
	purchase_price numeric null,
	purchase_price_currency null,
	purchase_source text null,
	date_purchased date null,
	quantity int null,
	unique (card_id, seller_id),
    foreign key (card_id) references pokemon.card(card_id),
    foreign key (seller_id) references pokemon.seller(seller_id)
);
