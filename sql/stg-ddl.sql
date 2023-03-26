/*
 STAGING DDL
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.groups CASCADE;
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.dialogs CASCADE;
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.users CASCADE;

--users 
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__STAGING.users
(
    id              int NOT NULL PRIMARY KEY,
    chat_name       varchar(200),
    registration_dt timestamp NOT NULL,
    country         varchar(200),
    age             int,
    CHECK ( age > 0 and age > 100)
)
    ORDER BY id
    SEGMENTED BY hash(id) ALL NODES
    PARTITION BY registration_dt::date
        GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);


CREATE TABLE LEONIDGRISHENKOVYANDEXRU__STAGING.groups
(
    id              int NOT NULL PRIMARY KEY,
    admin_id        int NOT NULL,
    group_name      varchar(100),
    registration_dt timestamp NOT NULL,
    is_private      boolean,

    FOREIGN KEY (admin_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__STAGING.users (id)
)
    ORDER BY id, admin_id
    SEGMENTED BY hash(id) ALL NODES
    PARTITION BY registration_dt::date
        GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);


CREATE TABLE LEONIDGRISHENKOVYANDEXRU__STAGING.dialogs
(
    message_id    int NOT NULL PRIMARY KEY,
    message_ts    timestamp(6) NOT NULL,
    message_from  int NOT NULL,
    message_to    int NOT NULL,
    message       varchar(1000),
    message_group int,

    FOREIGN KEY (message_to) REFERENCES LEONIDGRISHENKOVYANDEXRU__STAGING.users (id),
    FOREIGN KEY (message_from) REFERENCES LEONIDGRISHENKOVYANDEXRU__STAGING.users (id)
)
    ORDER BY message_id
    SEGMENTED BY hash(message_id) ALL NODES
    PARTITION BY message_ts::date
        GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);
