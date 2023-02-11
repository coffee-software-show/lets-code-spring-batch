drop table if exists video_game_sales;
create table if not exists video_game_sales
(
    rank         int,
    name         text,
    platform     text,
    year         int,
    genre        text,
    publisher    text,
    na_sales     numeric(4, 2),
    eu_sales     numeric(4, 2),
    jp_sales     numeric(4, 2),
    other_sales  numeric(4, 2),
    global_sales numeric(4, 2),
    unique (name, platform, year, genre)

);