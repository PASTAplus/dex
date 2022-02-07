create table entity
(
    id         integer not null primary key,
    data_url   text not null,
    base_url   text not null,
    scope      text not null,
    identifier int  not null,
    version    int  not null,
    entity     text not null
);

create unique index entity_data_url_uindex
    on entity (data_url);

