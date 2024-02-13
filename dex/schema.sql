-- dist_url: distribution/online/url from EML
-- meta_url: location of the EML metadata
-- data_url: location of the CSV data
--
-- When DeX is called from PASTA, dist_url and data_url are the same, while meta_url
-- is derived from the dist_url.
-- When DeX is called from ezEML, dist_url and data_url may or may not be the same,
-- and meta_url is provided by the caller.
CREATE TABLE entity
(
    id       integer not null primary key,
    timestamp text default (datetime('now','localtime')),
    dist_url text not null,
    meta_url text,
    data_url text
);

-- create table entity
-- (
--     id         integer not null primary key,
--     data_url   text    not null,
--     base_url   text    not null,
--     scope      text    not null,
--     identifier int     not null,
--     version    int     not null,
--     entity     text    not null
-- );
--
-- create unique index entity_data_url_uindex
--     on entity (data_url);
--
-- CREATE TABLE "preview"
-- (
--     id          integer not null primary key,
--     entity_id   integer not null
--         constraint preview_entity_id_fk
--             references entity
--             on delete cascade,
--     remote_meta_url TEXT    not null,
--     remote_data_url TEXT    not null
-- );
